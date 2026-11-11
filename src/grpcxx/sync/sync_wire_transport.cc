#include "grpcxx/sync/sync_wire_transport.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cares/resolver/resolver.hpp>
#include <cerrno>
#include <limits>
#include <string>
#include <uarch/uarch.hpp>
#include <uring/uring.hpp>
#include <utility>

namespace grpcxx::sync_detail {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeSyncStatus(StatusCode code,
                                                              std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MapNexoraError(const nexora::Error& err,
                                                              std::string_view prefix) noexcept {
  uarch::cold_path();
  StatusCode code = StatusCode::kUnavailable;
  if (err == nexora::Error::Timeout()) {
    code = StatusCode::kDeadlineExceeded;
  } else if (err == nexora::Error::InvalidArgument()) {
    code = StatusCode::kInvalidArgument;
  } else if (err.domain == nexora::ErrorDomain::kProto &&
             err.code == static_cast<std::int32_t>(
                             std::to_underlying(nexora::ProtoErrorCode::kTlsConfigError))) {
    code = StatusCode::kInvalidArgument;
  } else if (err.domain == nexora::ErrorDomain::kProto &&
             err.code == static_cast<std::int32_t>(
                             std::to_underlying(nexora::ProtoErrorCode::kTlsVerifyFailed))) {
    code = StatusCode::kUnauthenticated;
  }
  std::string msg{prefix};
  msg.append(": ");
  msg.append(err.Message());
  return Status{code, std::move(msg)};
}

[[nodiscard]] std::uint32_t ClampTimeoutMs(std::chrono::milliseconds d) noexcept {
  if (d.count() <= 0) return 1U;
  if (d.count() > static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())) {
    return std::numeric_limits<std::uint32_t>::max();
  }
  return static_cast<std::uint32_t>(d.count());
}

[[nodiscard]] std::expected<void, Status> ConfigureTcpSocketFlags(int fd, bool nodelay,
                                                                  bool keepalive) noexcept {
  const int current = ::fcntl(fd, F_GETFL, 0);
  if (uarch::unlikely(current < 0 || ::fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0)) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInternal, "cannot set O_NONBLOCK on fd"));
  }
  const int nd = nodelay ? 1 : 0;
  static_cast<void>(::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd)));
  const int ka = keepalive ? 1 : 0;
  static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &ka, sizeof(ka)));
  return {};
}

[[nodiscard]] std::expected<void, Status> ConfigureUnixSocketFlags(int fd) noexcept {
  const int current = ::fcntl(fd, F_GETFL, 0);
  if (uarch::unlikely(current < 0 || ::fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0)) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInternal, "cannot set O_NONBLOCK on fd"));
  }
  return {};
}

}  // namespace

std::expected<SyncWireTransport, Status> SyncWireTransport::FromMemoryPipe(
    nexora::MemoryPipe pipe) noexcept {
  if (uarch::unlikely(!pipe.IsOpen())) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "memory pipe is closed"));
  }
  SyncWireTransport out{};
  out.raw_.emplace<nexora::MemoryPipe>(std::move(pipe));
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::FromMemoryPipeTlsClient(
    nexora::MemoryPipe pipe, const nexora::TlsClientConfig& tls,
    std::string_view server_name) noexcept {
  if (uarch::unlikely(!pipe.IsOpen())) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "memory pipe is closed"));
  }
  auto session = nexora::TlsSession::CreateClient(tls, server_name);
  if (!session) return std::unexpected(MapNexoraError(session.error(), "TLS client session"));
  SyncWireTransport out{};
  out.raw_.emplace<nexora::MemoryPipe>(std::move(pipe));
  out.tls_.emplace(std::move(*session));
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::FromMemoryPipeTlsServer(
    nexora::MemoryPipe pipe, const nexora::TlsServerConfig& tls) noexcept {
  if (uarch::unlikely(!pipe.IsOpen())) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "memory pipe is closed"));
  }
  auto session = nexora::TlsSession::CreateServer(tls);
  if (!session) return std::unexpected(MapNexoraError(session.error(), "TLS server session"));
  SyncWireTransport out{};
  out.raw_.emplace<nexora::MemoryPipe>(std::move(pipe));
  out.tls_.emplace(std::move(*session));
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::ConnectTcp(
    std::string_view host, std::uint16_t port, const SynchronousChannelOptions& options) noexcept {
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  if (host.empty() || port == 0U) {
    return std::unexpected(
        MakeSyncStatus(StatusCode::kInvalidArgument, "TCP connect requires non-empty host:port"));
  }

  auto proactor = uring::Proactor::Create(64U);
  if (!proactor) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInternal, "cannot initialize DNS proactor"));
  }
  auto resolver = cares::Resolver<>::FromSystem(
      *proactor, cares::ResolverConfig{.family = cares::IpFamily::DualStack});
  if (!resolver) {
    return std::unexpected(
        MakeSyncStatus(StatusCode::kUnavailable, "cannot initialize DNS resolver"));
  }
  auto resolved = resolver->ResolveEndpoints(host, port, ClampTimeoutMs(options.resolve_timeout));
  if (!resolved || resolved->empty()) {
    return std::unexpected(
        MakeSyncStatus(StatusCode::kUnavailable, "DNS endpoint resolution failed"));
  }

  const auto deadline = std::chrono::steady_clock::now() + options.connect_timeout;
  nexora::Error last_err = nexora::Error::FromRuntime(nexora::RuntimeErrorCode::kConnectionRefused);
  for (const auto& ep : *resolved) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return std::unexpected(
          MakeSyncStatus(StatusCode::kDeadlineExceeded, "TCP connect deadline exceeded"));
    }
    const auto rem_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    nexora::TcpTransport tcp{-1, std::nullopt, options.tcp_nodelay};
    auto connected = tcp.Connect(ep, ClampTimeoutMs(rem_ms));
    if (connected) {
      if (auto cfg =
              ConfigureTcpSocketFlags(tcp.NativeFd(), options.tcp_nodelay, options.tcp_keepalive);
          !cfg) {
        return std::unexpected(std::move(cfg.error()));
      }
      SyncWireTransport out{};
      out.raw_.emplace<nexora::TcpTransport>(std::move(tcp));
      out.read_timeout_ = options.read_timeout;
      out.write_timeout_ = options.write_timeout;
      return out;
    }
    last_err = connected.error();
  }
  return std::unexpected(MapNexoraError(last_err, "TCP connect"));
}

std::expected<SyncWireTransport, Status> SyncWireTransport::ConnectTls(
    std::string_view host, std::uint16_t port, const nexora::TlsClientConfig& tls,
    std::string_view server_name, const SynchronousChannelOptions& options) noexcept {
  auto wire = ConnectTcp(host, port, options);
  if (!wire) return wire;
  const std::string_view sni = server_name.empty() ? host : server_name;
  auto session = nexora::TlsSession::CreateClient(tls, sni);
  if (!session) return std::unexpected(MapNexoraError(session.error(), "TLS client session"));
  wire->tls_.emplace(std::move(*session));
  if (auto hs = wire->CompleteBlockingTlsHandshake(options.connect_timeout); !hs) {
    return std::unexpected(std::move(hs.error()));
  }
  return wire;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::ConnectUnix(
    std::string_view path, const SynchronousChannelOptions& options) noexcept {
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  auto sock = nexora::UnixSocket::Connect(path, ClampTimeoutMs(options.connect_timeout));
  if (!sock) return std::unexpected(MapNexoraError(sock.error(), "Unix connect"));
  if (auto cfg = ConfigureUnixSocketFlags(sock->NativeFd()); !cfg) {
    return std::unexpected(std::move(cfg.error()));
  }
  SyncWireTransport out{};
  out.raw_.emplace<nexora::UnixSocket>(std::move(*sock));
  out.read_timeout_ = options.read_timeout;
  out.write_timeout_ = options.write_timeout;
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::FromTcp(
    nexora::TcpTransport tcp, const SynchronousChannelOptions& options) noexcept {
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  if (uarch::unlikely(!tcp.IsOpen())) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "TCP socket is closed"));
  }
  if (auto cfg =
          ConfigureTcpSocketFlags(tcp.NativeFd(), options.tcp_nodelay, options.tcp_keepalive);
      !cfg) {
    return std::unexpected(std::move(cfg.error()));
  }
  SyncWireTransport out{};
  out.raw_.emplace<nexora::TcpTransport>(std::move(tcp));
  out.read_timeout_ = options.read_timeout;
  out.write_timeout_ = options.write_timeout;
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::FromTlsServer(
    nexora::TcpTransport tcp, const nexora::TlsServerConfig& tls,
    std::chrono::milliseconds handshake_timeout) noexcept {
  SynchronousChannelOptions opts{};
  opts.connect_timeout = handshake_timeout;
  auto wire = FromTcp(std::move(tcp), opts);
  if (!wire) return wire;
  auto session = nexora::TlsSession::CreateServer(tls);
  if (!session) return std::unexpected(MapNexoraError(session.error(), "TLS server session"));
  wire->tls_.emplace(std::move(*session));
  if (auto hs = wire->CompleteBlockingTlsHandshake(handshake_timeout); !hs) {
    return std::unexpected(std::move(hs.error()));
  }
  return wire;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::FromUnix(
    nexora::UnixSocket sock, const SynchronousChannelOptions& options) noexcept {
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  if (uarch::unlikely(!sock.IsOpen())) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "Unix socket is closed"));
  }
  if (auto cfg = ConfigureUnixSocketFlags(sock.NativeFd()); !cfg) {
    return std::unexpected(std::move(cfg.error()));
  }
  SyncWireTransport out{};
  out.raw_.emplace<nexora::UnixSocket>(std::move(sock));
  out.read_timeout_ = options.read_timeout;
  out.write_timeout_ = options.write_timeout;
  return out;
}

std::expected<SyncWireTransport, Status> SyncWireTransport::AdoptTcp(
    int fd, const SynchronousChannelOptions& options) noexcept {
  if (fd < 0)
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "invalid TCP fd"));
  return FromTcp(nexora::TcpTransport{fd, std::nullopt, options.tcp_nodelay}, options);
}

std::expected<SyncWireTransport, Status> SyncWireTransport::AdoptUnix(
    int fd, const SynchronousChannelOptions& options) noexcept {
  if (fd < 0) {
    return std::unexpected(MakeSyncStatus(StatusCode::kInvalidArgument, "invalid Unix fd"));
  }
  return FromUnix(nexora::UnixSocket{fd}, options);
}

bool SyncWireTransport::IsOpen() const noexcept {
  if (const auto* pipe = std::get_if<nexora::MemoryPipe>(&raw_)) return pipe->IsOpen();
  if (const auto* tcp = std::get_if<nexora::TcpTransport>(&raw_)) return tcp->IsOpen();
  if (const auto* ux = std::get_if<nexora::UnixSocket>(&raw_)) return ux->IsOpen();
  return false;
}

int SyncWireTransport::NativeHandle() const noexcept {
  if (const auto* tcp = std::get_if<nexora::TcpTransport>(&raw_)) return tcp->NativeFd();
  if (const auto* ux = std::get_if<nexora::UnixSocket>(&raw_)) return ux->NativeFd();
  return -1;
}

void SyncWireTransport::Close() noexcept {
  tls_.reset();
  cipher_tx_off_ = 0uz;
  cipher_tx_len_ = 0uz;
  if (auto* pipe = std::get_if<nexora::MemoryPipe>(&raw_)) pipe->Close();
  if (auto* tcp = std::get_if<nexora::TcpTransport>(&raw_)) tcp->Close();
  if (auto* ux = std::get_if<nexora::UnixSocket>(&raw_)) ux->Close();
  raw_.emplace<std::monostate>();
}

std::expected<SyncIoOutcome, Status> SyncWireTransport::RawReadSome(
    std::span<std::uint8_t> out) noexcept {
  if (out.empty()) return SyncIoOutcome{};
  if (auto* pipe = std::get_if<nexora::MemoryPipe>(&raw_)) {
    auto res = pipe->ReadSome(out, 0U);
    if (res) return *res == 0uz ? SyncIoOutcome{.eof = 1U} : SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::Incomplete()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "memory pipe read"));
  }
  if (auto* tcp = std::get_if<nexora::TcpTransport>(&raw_)) {
    auto res = tcp->ReadSome(out, 0U);
    if (res) return *res == 0uz ? SyncIoOutcome{.eof = 1U} : SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::Timeout()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "TCP read"));
  }
  if (auto* ux = std::get_if<nexora::UnixSocket>(&raw_)) {
    auto res = ux->ReadSome(out, 0U);
    if (res) return *res == 0uz ? SyncIoOutcome{.eof = 1U} : SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::Timeout()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "Unix socket read"));
  }
  return std::unexpected(MakeSyncStatus(StatusCode::kFailedPrecondition, "transport is closed"));
}

std::expected<SyncIoOutcome, Status> SyncWireTransport::RawWriteSome(
    std::span<const std::uint8_t> data) noexcept {
  if (data.empty()) return SyncIoOutcome{};
  if (auto* pipe = std::get_if<nexora::MemoryPipe>(&raw_)) {
    auto res = pipe->Write(data);
    if (res) return SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::BufferOverflow()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "memory pipe write"));
  }
  if (auto* tcp = std::get_if<nexora::TcpTransport>(&raw_)) {
    auto res = tcp->Write(data, 0U);
    if (res) return SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::Timeout()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "TCP write"));
  }
  if (auto* ux = std::get_if<nexora::UnixSocket>(&raw_)) {
    auto res = ux->Write(data, 0U);
    if (res) return SyncIoOutcome{.bytes = *res};
    if (res.error() == nexora::Error::Timeout()) return SyncIoOutcome{.would_block = 1U};
    return std::unexpected(MapNexoraError(res.error(), "Unix socket write"));
  }
  return std::unexpected(MakeSyncStatus(StatusCode::kFailedPrecondition, "transport is closed"));
}

std::expected<bool, Status> SyncWireTransport::FlushCiphertextOut() noexcept {
  if (!tls_.has_value()) return true;
  for (std::size_t iter = 0uz; iter < 64uz; ++iter) {
    if (cipher_tx_off_ < cipher_tx_len_) {
      auto wrote = RawWriteSome(std::span<const std::uint8_t>{cipher_tx_}.subspan(
          cipher_tx_off_, cipher_tx_len_ - cipher_tx_off_));
      if (!wrote) return std::unexpected(std::move(wrote.error()));
      cipher_tx_off_ += wrote->bytes;
      if (cipher_tx_off_ < cipher_tx_len_) return false;
      cipher_tx_off_ = 0uz;
      cipher_tx_len_ = 0uz;
    }
    if (!tls_->HasPendingCiphertextOut()) return true;
    auto taken = tls_->TakeCiphertext(cipher_tx_);
    if (!taken) return std::unexpected(MapNexoraError(taken.error(), "TLS ciphertext drain"));
    if (*taken == 0uz) return true;
    cipher_tx_off_ = 0uz;
    cipher_tx_len_ = *taken;
  }
  return cipher_tx_off_ == cipher_tx_len_ && !tls_->HasPendingCiphertextOut();
}

std::expected<void, Status> SyncWireTransport::StepTls() noexcept {
  if (!tls_.has_value()) return {};
  if (!tls_->IsHandshakeComplete()) {
    if (auto drove = tls_->DriveHandshake(); !drove) {
      return std::unexpected(MapNexoraError(drove.error(), "TLS handshake"));
    }
  }
  auto flushed = FlushCiphertextOut();
  if (!flushed) return std::unexpected(std::move(flushed.error()));

  if (!tls_->IsHandshakeComplete()) {
    std::array<std::uint8_t, 16384> rx_buf;
    for (std::size_t iter = 0uz; iter < 16uz && !tls_->IsHandshakeComplete(); ++iter) {
      auto read_res = RawReadSome(rx_buf);
      if (!read_res) return std::unexpected(std::move(read_res.error()));
      if (read_res->eof != 0U) {
        return std::unexpected(MakeSyncStatus(StatusCode::kUnavailable,
                                              "peer closed connection during TLS handshake"));
      }
      if (read_res->bytes == 0uz) break;
      std::size_t fed_total = 0uz;
      while (fed_total < read_res->bytes) {
        auto fed = tls_->FeedCiphertext(
            std::span<const std::uint8_t>{rx_buf.data() + fed_total, read_res->bytes - fed_total});
        if (!fed) return std::unexpected(MapNexoraError(fed.error(), "TLS handshake feed"));
        if (*fed == 0uz) break;
        fed_total += *fed;
      }
      auto out_flush = FlushCiphertextOut();
      if (!out_flush) return std::unexpected(std::move(out_flush.error()));
    }
  }

  if (tls_->IsHandshakeComplete() && !alpn_verified_) {
    if (tls_->NegotiatedAlpn() != nexora::AlpnProtocol::kHttp2) {
      return std::unexpected(
          MakeSyncStatus(StatusCode::kUnavailable, "TLS ALPN did not negotiate h2"));
    }
    alpn_verified_ = true;
  }
  return {};
}

std::expected<void, Status> SyncWireTransport::CompleteBlockingTlsHandshake(
    std::chrono::milliseconds timeout) noexcept {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (tls_.has_value() && (!tls_->IsHandshakeComplete() || HasPendingTlsWrite())) {
    if (auto stepped = StepTls(); !stepped) return stepped;
    if (tls_->IsHandshakeComplete() && !HasPendingTlsWrite()) break;
    if (auto waited = WaitSocket(HasPendingTlsWrite(), deadline); !waited) return waited;
  }
  return StepTls();
}

std::expected<SyncIoOutcome, Status> SyncWireTransport::ReadSome(
    std::span<std::byte> out) noexcept {
  if (out.empty()) return SyncIoOutcome{};
  if (!tls_.has_value()) return RawReadSome(AsUint8Span(out));
  if (auto stepped = StepTls(); !stepped) return std::unexpected(std::move(stepped.error()));
  if (!tls_->IsHandshakeComplete()) return SyncIoOutcome{.would_block = 1U};

  const auto plain_out = AsUint8Span(out);
  for (std::size_t iter = 0uz; iter < 16uz; ++iter) {
    auto plain = tls_->ReadPlaintext(plain_out);
    if (!plain) return std::unexpected(MapNexoraError(plain.error(), "TLS read"));
    if (*plain > 0uz) {
      auto flushed = FlushCiphertextOut();
      if (!flushed) return std::unexpected(std::move(flushed.error()));
      return SyncIoOutcome{.bytes = *plain};
    }
    if (tls_->IsPeerClosed()) return SyncIoOutcome{.eof = 1U};

    std::array<std::uint8_t, 16384> cipher_in;
    auto raw_in = RawReadSome(cipher_in);
    if (!raw_in) return std::unexpected(std::move(raw_in.error()));
    if (raw_in->eof != 0U) return SyncIoOutcome{.eof = 1U};
    if (raw_in->would_block != 0U || raw_in->bytes == 0uz) {
      return SyncIoOutcome{.would_block = 1U};
    }
    std::size_t fed = 0uz;
    while (fed < raw_in->bytes) {
      auto n = tls_->FeedCiphertext(
          std::span<const std::uint8_t>{cipher_in.data() + fed, raw_in->bytes - fed});
      if (!n) return std::unexpected(MapNexoraError(n.error(), "TLS feed"));
      if (*n == 0uz) break;
      fed += *n;
    }
  }
  return SyncIoOutcome{.would_block = 1U};
}

std::expected<SyncIoOutcome, Status> SyncWireTransport::WriteSome(
    std::span<const std::byte> data) noexcept {
  if (data.empty()) return SyncIoOutcome{};
  if (!tls_.has_value()) return RawWriteSome(AsUint8Span(data));
  if (auto stepped = StepTls(); !stepped) return std::unexpected(std::move(stepped.error()));
  if (!tls_->IsHandshakeComplete()) return SyncIoOutcome{.would_block = 1U};
  auto flushed = FlushCiphertextOut();
  if (!flushed) return std::unexpected(std::move(flushed.error()));
  if (!*flushed) return SyncIoOutcome{.would_block = 1U};

  auto accepted = tls_->WritePlaintext(AsUint8Span(data));
  if (!accepted) return std::unexpected(MapNexoraError(accepted.error(), "TLS write"));
  auto post_flush = FlushCiphertextOut();
  if (!post_flush) return std::unexpected(std::move(post_flush.error()));
  if (*accepted == 0uz) return SyncIoOutcome{.would_block = 1U};
  return SyncIoOutcome{.bytes = *accepted};
}

std::expected<void, Status> SyncWireTransport::WaitSocket(
    bool want_write, std::chrono::steady_clock::time_point rpc_deadline,
    bool honor_read_timeout) noexcept {
  const int fd = NativeHandle();
  if (fd < 0) return {};
  const auto now = std::chrono::steady_clock::now();
  if (now >= rpc_deadline) {
    return std::unexpected(MakeSyncStatus(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"));
  }
  auto stall_limit = rpc_deadline;
  if (want_write) {
    if (write_timeout_.count() > 0) {
      const auto cap = now + write_timeout_;
      if (cap < stall_limit) stall_limit = cap;
    }
  } else if (honor_read_timeout && read_timeout_.count() > 0) {
    const auto cap = now + read_timeout_;
    if (cap < stall_limit) stall_limit = cap;
  }
  const auto effective_deadline = stall_limit;
  const auto wait_ms = std::max<std::int64_t>(
      1, std::chrono::duration_cast<std::chrono::milliseconds>(effective_deadline - now).count());
  const int clamped_ms = wait_ms > static_cast<std::int64_t>(std::numeric_limits<int>::max())
                             ? std::numeric_limits<int>::max()
                             : static_cast<int>(wait_ms);

  ::pollfd pfd{};
  pfd.fd = fd;
  pfd.events = static_cast<short>(POLLIN | (want_write ? POLLOUT : 0));
  for (;;) {
    const int rc = ::poll(&pfd, 1, clamped_ms);
    if (rc > 0) return {};
    if (rc == 0) {
      const bool rpc_expired = std::chrono::steady_clock::now() >= rpc_deadline;
      return std::unexpected(MakeSyncStatus(
          StatusCode::kDeadlineExceeded,
          rpc_expired ? "RPC deadline exceeded" : "socket I/O stall timeout exceeded"));
    }
    if (errno == EINTR) continue;
    return std::unexpected(MakeSyncStatus(StatusCode::kUnavailable, "socket poll failed"));
  }
}

}  // namespace grpcxx::sync_detail
