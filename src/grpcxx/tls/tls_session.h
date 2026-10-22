#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "grpcxx/tls/tls_context.h"

namespace grpcxx {

enum class TlsHandshakeState : std::uint8_t {
  kHandshaking,
  kEstablished,
  kClosing,
  kClosed,
  kFailed,
};

/** Thread-confined TLS connection driven exclusively through memory BIOs.
 * A zero-byte read/write indicates backpressure; drain ciphertext, feed peer
 * ciphertext, and retry on a later caller tick. After a zero-byte write, retry
 * the identical plaintext until it is accepted. No method touches a socket.
 */
class TlsSession final {
 public:
  static auto Create(const TlsContext& context,
                     std::string_view server_name = {}) -> std::expected<TlsSession, Status>;
  ~TlsSession();
  TlsSession(TlsSession&& other) noexcept;
  auto operator=(TlsSession&& other) noexcept -> TlsSession&;
  TlsSession(const TlsSession&) = delete;
  auto operator=(const TlsSession&) -> TlsSession& = delete;

  /** Accepts at most the free space in the bounded ciphertext input buffer. */
  auto FeedCiphertext(std::span<const std::byte> bytes) -> std::expected<std::size_t, Status>;
  auto DrainCiphertext(std::span<std::byte> output) -> std::expected<std::size_t, Status>;
  auto WritePlaintext(std::span<const std::byte> bytes) -> std::expected<std::size_t, Status>;
  /** Remains readable during closing until all peer application data is drained. */
  auto ReadPlaintext(std::span<std::byte> output) -> std::expected<std::size_t, Status>;
  /** Advances one TLS operation, preserving final application data for reads.
   * During shutdown, callers must drain ReadPlaintext as well as ciphertext.
   */
  auto PollOnce() -> std::expected<TlsHandshakeState, Status>;
  auto BeginShutdown() -> std::expected<void, Status>;
  auto VerifyAlpnH2() const -> std::expected<void, Status>;
  void MarkTransportEof() noexcept;
  auto State() const noexcept -> TlsHandshakeState { return state_; }
  /** Counts rejected operations without changing the TLS lifecycle state. */
  auto RejectedTransitions() const noexcept -> std::uint64_t;
  auto PendingCiphertext() const noexcept -> std::size_t;
  auto ProtocolVersion() const noexcept -> std::string_view;

 private:
  explicit TlsSession(void* handle) noexcept;
  auto RejectTransition() const -> Status;
  auto HandleResult(int result, std::string_view operation) -> std::expected<bool, Status>;
  void* handle_{nullptr};
  TlsHandshakeState state_{TlsHandshakeState::kHandshaking};
  mutable std::uint64_t rejected_transitions_{0};
};

}  // namespace grpcxx
