#include "grpcxx/tls/tls_session.h"

#include <fmt/format.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <array>
#include <memory>
#include <utility>

namespace grpcxx {
namespace {

constexpr auto CiphertextLimit() -> std::size_t {
  if consteval {
    return 256 * 1024;
  } else {
    return 256 * 1024;
  }
}

constexpr std::size_t kRecordPayloadLimit = 16 * 1024;

auto Failure(std::string_view operation) -> Status {
  std::array<char, 256> text{};
  ERR_error_string_n(ERR_get_error(), text.data(), text.size());
  ERR_clear_error();
  return Status{StatusCode::kUnavailable, fmt::format("TLS {}: {}", operation, text.data())};
}

}  // namespace

TlsSession::TlsSession(void* handle) noexcept : handle_(handle) {}
TlsSession::~TlsSession() { SSL_free(static_cast<SSL*>(handle_)); }

TlsSession::TlsSession(TlsSession&& other) noexcept
    : handle_(std::exchange(other.handle_, nullptr)),
      state_(other.state_),
      rejected_transitions_(std::exchange(other.rejected_transitions_, 0)) {
  other.state_ = TlsHandshakeState::kClosed;
}

auto TlsSession::operator=(TlsSession&& other) noexcept -> TlsSession& {
  if (this != &other) {
    SSL_free(static_cast<SSL*>(handle_));
    handle_ = std::exchange(other.handle_, nullptr);
    state_ = other.state_;
    rejected_transitions_ = std::exchange(other.rejected_transitions_, 0);
    other.state_ = TlsHandshakeState::kClosed;
  }
  return *this;
}

auto TlsSession::Create(const TlsContext& context,
                        std::string_view server_name) -> std::expected<TlsSession, Status> {
  ERR_clear_error();
  if (!context.handle_ || server_name.find('\0') != std::string_view::npos ||
      (context.mode_ == TlsMode::kClient && context.verify_peer_ && server_name.empty())) {
    return std::unexpected(Status{StatusCode::kInvalidArgument,
                                  "TLS clients verifying peers require a valid server name"});
  }
  std::unique_ptr<SSL, decltype(&SSL_free)> ssl(SSL_new(static_cast<SSL_CTX*>(context.handle_)),
                                                SSL_free);
  if (!ssl) return std::unexpected(Failure("session allocation"));
  std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new(BIO_s_mem()), BIO_free);
  std::unique_ptr<BIO, decltype(&BIO_free)> output(BIO_new(BIO_s_mem()), BIO_free);
  if (!input || !output) return std::unexpected(Failure("memory BIO allocation"));
  BIO_set_mem_eof_return(input.get(), -1);
  BIO_set_mem_eof_return(output.get(), -1);
  SSL_set_bio(ssl.get(), input.release(), output.release());
  SSL_set_mode(ssl.get(), SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  if (context.mode_ == TlsMode::kServer) {
    SSL_set_accept_state(ssl.get());
  } else {
    SSL_set_connect_state(ssl.get());
    constexpr auto kAlpn = std::array<unsigned char, 3>{2, 'h', '2'};
    if (SSL_set_alpn_protos(ssl.get(), kAlpn.data(), kAlpn.size()) != 0) {
      return std::unexpected(Failure("ALPN configuration"));
    }
    if (!server_name.empty()) {
      const auto name = std::string(server_name);
      std::unique_ptr<ASN1_OCTET_STRING, decltype(&ASN1_OCTET_STRING_free)> ip(
          a2i_IPADDRESS(name.c_str()), ASN1_OCTET_STRING_free);
      ERR_clear_error();
      if (ip) {
        if (context.verify_peer_ &&
            X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl.get()), name.c_str()) != 1) {
          return std::unexpected(Failure("IP address verification configuration"));
        }
      } else {
        if (SSL_set_tlsext_host_name(ssl.get(), name.c_str()) != 1 ||
            (context.verify_peer_ && SSL_set1_host(ssl.get(), name.c_str()) != 1)) {
          return std::unexpected(Failure("hostname verification configuration"));
        }
        SSL_set_hostflags(ssl.get(), X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
      }
    }
  }
  return TlsSession(ssl.release());
}

auto TlsSession::RejectTransition() const -> Status {
  ++rejected_transitions_;
  return Status{StatusCode::kFailedPrecondition, "TLS operation is invalid in the current state"};
}

auto TlsSession::HandleResult(int result,
                              std::string_view operation) -> std::expected<bool, Status> {
  if (result == 1) return true;
  const int error = SSL_get_error(static_cast<SSL*>(handle_), result);
  if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) return false;
  if (error == SSL_ERROR_ZERO_RETURN) {
    state_ = TlsHandshakeState::kClosing;
    return false;
  }
  state_ = TlsHandshakeState::kFailed;
  return std::unexpected(Failure(operation));
}

auto TlsSession::FeedCiphertext(std::span<const std::byte> bytes)
    -> std::expected<std::size_t, Status> {
  if (!handle_ || state_ == TlsHandshakeState::kClosed || state_ == TlsHandshakeState::kFailed) {
    return std::unexpected(RejectTransition());
  }
  auto* input = SSL_get_rbio(static_cast<SSL*>(handle_));
  const auto pending = BIO_ctrl_pending(input);
  const auto capacity = pending >= CiphertextLimit() ? 0 : CiphertextLimit() - pending;
  const auto accepted = std::min(bytes.size(), capacity);
  if (accepted == 0) return 0;
  ERR_clear_error();
  std::size_t written = 0;
  if (BIO_write_ex(input, bytes.data(), accepted, &written) != 1) {
    state_ = TlsHandshakeState::kFailed;
    return std::unexpected(Failure("ciphertext input"));
  }
  return written;
}

auto TlsSession::DrainCiphertext(std::span<std::byte> output)
    -> std::expected<std::size_t, Status> {
  if (!handle_) return std::unexpected(RejectTransition());
  if (output.empty() || PendingCiphertext() == 0) return 0;
  std::size_t read = 0;
  ERR_clear_error();
  if (BIO_read_ex(SSL_get_wbio(static_cast<SSL*>(handle_)), output.data(), output.size(), &read) !=
      1) {
    return std::unexpected(Failure("ciphertext output"));
  }
  return read;
}

auto TlsSession::WritePlaintext(std::span<const std::byte> bytes)
    -> std::expected<std::size_t, Status> {
  if (!handle_ || state_ != TlsHandshakeState::kEstablished) {
    return std::unexpected(RejectTransition());
  }
  if (bytes.empty() || PendingCiphertext() >= CiphertextLimit()) return 0;
  ERR_clear_error();
  std::size_t written = 0;
  const int result = SSL_write_ex(static_cast<SSL*>(handle_), bytes.data(),
                                  std::min(bytes.size(), kRecordPayloadLimit), &written);
  if (auto outcome = HandleResult(result, "plaintext write"); !outcome) {
    return std::unexpected(outcome.error());
  }
  return written;
}

auto TlsSession::ReadPlaintext(std::span<std::byte> output) -> std::expected<std::size_t, Status> {
  if (!handle_ ||
      (state_ != TlsHandshakeState::kEstablished && state_ != TlsHandshakeState::kClosing)) {
    return std::unexpected(RejectTransition());
  }
  if (output.empty() || PendingCiphertext() >= CiphertextLimit()) return 0;
  ERR_clear_error();
  std::size_t read = 0;
  const int result = SSL_read_ex(static_cast<SSL*>(handle_), output.data(),
                                 std::min(output.size(), kRecordPayloadLimit), &read);
  if (auto outcome = HandleResult(result, "plaintext read"); !outcome) {
    return std::unexpected(outcome.error());
  }
  return read;
}

auto TlsSession::PollOnce() -> std::expected<TlsHandshakeState, Status> {
  if (!handle_) return std::unexpected(RejectTransition());
  switch (state_) {
    case TlsHandshakeState::kHandshaking: {
      if (PendingCiphertext() >= CiphertextLimit()) return state_;
      ERR_clear_error();
      const int result = SSL_do_handshake(static_cast<SSL*>(handle_));
      auto outcome = HandleResult(result, "handshake");
      if (!outcome) return std::unexpected(outcome.error());
      if (*outcome) {
        if (auto alpn = VerifyAlpnH2(); !alpn) {
          state_ = TlsHandshakeState::kFailed;
          return std::unexpected(alpn.error());
        }
        state_ = TlsHandshakeState::kEstablished;
      }
      return state_;
    }
    case TlsHandshakeState::kEstablished:
    case TlsHandshakeState::kClosed:
      return state_;
    case TlsHandshakeState::kClosing: {
      if (PendingCiphertext() >= CiphertextLimit()) return state_;
      auto* ssl = static_cast<SSL*>(handle_);
      const auto shutdown = SSL_get_shutdown(ssl);
      if ((shutdown & SSL_SENT_SHUTDOWN) != 0 && (shutdown & SSL_RECEIVED_SHUTDOWN) == 0) {
        std::byte next{};
        std::size_t available = 0;
        ERR_clear_error();
        const int result = SSL_peek_ex(ssl, &next, 1, &available);
        if (auto outcome = HandleResult(result, "shutdown input"); !outcome) {
          return std::unexpected(outcome.error());
        }
        return state_;
      }
      ERR_clear_error();
      const int result = SSL_shutdown(ssl);
      if (result == 1) {
        state_ = TlsHandshakeState::kClosed;
      } else if (result < 0) {
        if (auto outcome = HandleResult(result, "shutdown"); !outcome) {
          return std::unexpected(outcome.error());
        }
      }
      return state_;
    }
    case TlsHandshakeState::kFailed:
      return std::unexpected(RejectTransition());
  }
  std::unreachable();
}

auto TlsSession::BeginShutdown() -> std::expected<void, Status> {
  if (!handle_) return std::unexpected(RejectTransition());
  switch (state_) {
    case TlsHandshakeState::kEstablished:
      state_ = TlsHandshakeState::kClosing;
      return {};
    case TlsHandshakeState::kClosing:
    case TlsHandshakeState::kClosed:
      return {};
    case TlsHandshakeState::kHandshaking:
    case TlsHandshakeState::kFailed:
      return std::unexpected(RejectTransition());
  }
  std::unreachable();
}

auto TlsSession::VerifyAlpnH2() const -> std::expected<void, Status> {
  if (!handle_ || !SSL_is_init_finished(static_cast<SSL*>(handle_))) {
    return std::unexpected(RejectTransition());
  }
  const unsigned char* selected = nullptr;
  unsigned int selected_size = 0;
  SSL_get0_alpn_selected(static_cast<SSL*>(handle_), &selected, &selected_size);
  if (selected_size != 2 || selected[0] != 'h' || selected[1] != '2') {
    return std::unexpected(Status{StatusCode::kUnavailable, "TLS peer did not negotiate ALPN h2"});
  }
  return {};
}

void TlsSession::MarkTransportEof() noexcept {
  if (handle_) BIO_set_mem_eof_return(SSL_get_rbio(static_cast<SSL*>(handle_)), 0);
}

auto TlsSession::RejectedTransitions() const noexcept -> std::uint64_t {
  return rejected_transitions_;
}

auto TlsSession::PendingCiphertext() const noexcept -> std::size_t {
  return handle_ ? BIO_ctrl_pending(SSL_get_wbio(static_cast<SSL*>(handle_))) : 0;
}

auto TlsSession::ProtocolVersion() const noexcept -> std::string_view {
  return handle_ && SSL_is_init_finished(static_cast<SSL*>(handle_))
             ? std::string_view(SSL_get_version(static_cast<SSL*>(handle_)))
             : std::string_view{};
}

}  // namespace grpcxx
