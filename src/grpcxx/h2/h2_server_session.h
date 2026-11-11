#pragma once
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "grpcxx/core/status.h"
namespace grpcxx {
/** Caller-driven, bounded in-memory HTTP/2 unary and streaming server session over nexora. */
class H2ServerSession final {
 public:
  static constexpr std::size_t kMaxStreams = 16;
  static constexpr std::size_t kMaxMessageBytes = 8uz * 1024uz * 1024uz;
  enum class State : std::uint8_t { kOpen, kDraining, kClosed, kFailed };
  /** Concrete subtypes are checked before dispatch; invalid types leave Ready() false. */
  explicit H2ServerSession(std::string_view content_type = "application/grpc") noexcept;
  ~H2ServerSession();
  H2ServerSession(const H2ServerSession&) =
      delete("H2ServerSession owns a stateful H2 session; copying is disabled");
  H2ServerSession& operator=(const H2ServerSession&) =
      delete("H2ServerSession owns a stateful H2 session; copying is disabled");
  H2ServerSession(H2ServerSession&&) =
      delete("H2ServerSession callbacks bind the session address; moving is disabled");
  H2ServerSession& operator=(H2ServerSession&&) =
      delete("H2ServerSession callbacks bind the session address; moving is disabled");
  [[nodiscard]] bool HasPendingOutput() const noexcept;
  [[nodiscard]] bool Ready() const noexcept;
  [[nodiscard]] State GetState() const noexcept;
  [[nodiscard]] std::size_t ActiveStreams() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;
  [[nodiscard]] std::expected<std::size_t, Status> Receive(
      std::span<const std::byte> bytes) noexcept;
  /** Returned bytes remain valid until the next Send or Receive on this session. */
  [[nodiscard]] std::expected<std::span<const std::byte>, Status> Send() noexcept;
  [[nodiscard]] std::expected<void, Status> Ping() noexcept;
  [[nodiscard]] std::expected<void, Status> GoAway() noexcept;
  /** Request path and payload views are borrowed for the duration of the callback. */
  using RequestHandler = void (*)(void*, std::int32_t, std::uint64_t, std::string_view,
                                  std::span<const std::byte>) noexcept;
  void SetRequestHandler(void* context, RequestHandler handler) noexcept;
  [[nodiscard]] bool IsCurrent(std::int32_t stream_id, std::uint64_t generation) const noexcept;
  [[nodiscard]] std::string_view StreamScheme(std::int32_t stream_id) const noexcept;
  [[nodiscard]] std::string_view StreamAuthority(std::int32_t stream_id) const noexcept;
  [[nodiscard]] std::uint64_t StreamTimeoutNs(std::int32_t stream_id) const noexcept;
  /** An empty response type uses the session's configured type; header values are copied. */
  [[nodiscard]] std::expected<void, Status> Complete(std::int32_t stream_id,
                                                     std::uint64_t generation,
                                                     std::span<const std::byte> payload,
                                                     int grpc_status = 0,
                                                     std::string_view content_type = {},
                                                     std::string_view grpc_message = {}) noexcept;

  /** EOF retires incomplete ingress. Streaming cancellation delivers an empty end callback
   * with IsCurrent(id, generation) false; completed requests may still finish their response. */
  [[nodiscard]] std::expected<void, Status> OnPeerEof() noexcept;
  using StreamHandler = void (*)(void*, std::int32_t, std::uint64_t, std::string_view,
                                 std::span<const std::byte>, bool) noexcept;
  void SetStreamingHandler(void*, StreamHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SendMessage(std::int32_t, std::uint64_t,
                                                        std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> Finish(std::int32_t, std::uint64_t, int status = 0,
                                                   std::string_view grpc_message = {}) noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace grpcxx
