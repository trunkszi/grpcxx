#pragma once

#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <nexora/socket/memory_pipe.hpp>
#include <nexora/socket/tcp_transport.hpp>
#include <nexora/socket/unix_socket.hpp>
#include <nexora/tls/tls_config.hpp>
#include <nexora/tls/tls_session.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <uarch/uarch.hpp>
#include <variant>

#include "grpcxx/core/status.h"
#include "grpcxx/sync/call_options.h"

namespace grpcxx::sync_detail {

template <class T>
  requires(std::same_as<std::remove_const_t<T>, std::byte>)
[[nodiscard]] inline std::span<
    std::conditional_t<std::is_const_v<T>, const std::uint8_t, std::uint8_t>>
AsUint8Span(std::span<T> bytes) noexcept {
  using Out = std::conditional_t<std::is_const_v<T>, const std::uint8_t, std::uint8_t>;
  return {reinterpret_cast<Out*>(bytes.data()), bytes.size()};
}

struct SyncIoOutcome final {
  std::size_t bytes{0uz};
  std::uint32_t would_block{0U};
  std::uint32_t eof{0U};
};
static_assert(uarch::NoPadding<SyncIoOutcome> && uarch::RegisterPassable<SyncIoOutcome>);

class SyncWireTransport final {
 public:
  SyncWireTransport() noexcept = default;
  SyncWireTransport(const SyncWireTransport&) =
      delete("SyncWireTransport owns single-owner OS/TLS state; move instead");
  SyncWireTransport& operator=(const SyncWireTransport&) =
      delete("SyncWireTransport owns single-owner OS/TLS state; move instead");
  SyncWireTransport(SyncWireTransport&&) noexcept = default;
  SyncWireTransport& operator=(SyncWireTransport&&) noexcept = default;
  ~SyncWireTransport() noexcept = default;

  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromMemoryPipe(
      nexora::MemoryPipe pipe) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromMemoryPipeTlsClient(
      nexora::MemoryPipe pipe, const nexora::TlsClientConfig& tls,
      std::string_view server_name) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromMemoryPipeTlsServer(
      nexora::MemoryPipe pipe, const nexora::TlsServerConfig& tls) noexcept;

  [[nodiscard]] static std::expected<SyncWireTransport, Status> ConnectTcp(
      std::string_view host, std::uint16_t port, const SynchronousChannelOptions& options) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> ConnectTls(
      std::string_view host, std::uint16_t port, const nexora::TlsClientConfig& tls,
      std::string_view server_name, const SynchronousChannelOptions& options) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> ConnectUnix(
      std::string_view path, const SynchronousChannelOptions& options) noexcept;

  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromTcp(
      nexora::TcpTransport tcp, const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromTlsServer(
      nexora::TcpTransport tcp, const nexora::TlsServerConfig& tls,
      std::chrono::milliseconds handshake_timeout) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> FromUnix(
      nexora::UnixSocket sock, const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> AdoptTcp(
      int fd, const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<SyncWireTransport, Status> AdoptUnix(
      int fd, const SynchronousChannelOptions& options = {}) noexcept;

  [[nodiscard]] bool IsOpen() const noexcept;
  [[nodiscard]] bool IsSecure() const noexcept { return tls_.has_value(); }
  [[nodiscard]] bool IsHandshakeComplete() const noexcept {
    return !tls_.has_value() || tls_->IsHandshakeComplete();
  }
  [[nodiscard]] bool HasPendingTlsWrite() const noexcept {
    return tls_.has_value() && (tls_->HasPendingCiphertextOut() || cipher_tx_off_ < cipher_tx_len_);
  }
  [[nodiscard]] int NativeHandle() const noexcept;
  void Close() noexcept;

  [[nodiscard]] std::expected<void, Status> StepTls() noexcept;
  [[nodiscard]] std::expected<SyncIoOutcome, Status> ReadSome(std::span<std::byte> out) noexcept;
  [[nodiscard]] std::expected<SyncIoOutcome, Status> WriteSome(
      std::span<const std::byte> data) noexcept;
  [[nodiscard]] std::expected<void, Status> WaitSocket(
      bool want_write, std::chrono::steady_clock::time_point rpc_deadline,
      bool honor_read_timeout = false) noexcept;

 private:
  using RawVariant =
      std::variant<std::monostate, nexora::MemoryPipe, nexora::TcpTransport, nexora::UnixSocket>;

  [[nodiscard]] std::expected<SyncIoOutcome, Status> RawReadSome(
      std::span<std::uint8_t> out) noexcept;
  [[nodiscard]] std::expected<SyncIoOutcome, Status> RawWriteSome(
      std::span<const std::uint8_t> data) noexcept;
  [[nodiscard]] std::expected<bool, Status> FlushCiphertextOut() noexcept;
  [[nodiscard]] std::expected<void, Status> CompleteBlockingTlsHandshake(
      std::chrono::milliseconds timeout) noexcept;

  RawVariant raw_{};
  std::optional<nexora::TlsSession> tls_{};
  std::array<std::uint8_t, 18437> cipher_tx_{};
  std::size_t cipher_tx_off_{0uz};
  std::size_t cipher_tx_len_{0uz};
  std::chrono::milliseconds read_timeout_{0};
  std::chrono::milliseconds write_timeout_{5000};
  bool alpn_verified_{false};
};

}  // namespace grpcxx::sync_detail
