#pragma once
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "grpcxx/core/status.h"
namespace grpcxx {
/** Caller-driven, bounded in-memory HTTP/2 unary and streaming client session over nexora. */
class H2ClientSession final {
 public:
  static constexpr std::size_t kMaxStreams = 16;
  static constexpr std::size_t kMaxMessageBytes = 8uz * 1024uz * 1024uz;
  enum class State : std::uint8_t { kOpen, kDraining, kClosed, kFailed };
  H2ClientSession() noexcept;
  ~H2ClientSession();
  H2ClientSession(const H2ClientSession&) =
      delete("H2ClientSession owns a stateful H2 session; copying is disabled");
  H2ClientSession& operator=(const H2ClientSession&) =
      delete("H2ClientSession owns a stateful H2 session; copying is disabled");
  H2ClientSession(H2ClientSession&&) =
      delete("H2ClientSession callbacks bind the session address; moving is disabled");
  H2ClientSession& operator=(H2ClientSession&&) =
      delete("H2ClientSession callbacks bind the session address; moving is disabled");
  [[nodiscard]] bool Ready() const noexcept;
  [[nodiscard]] bool HasPendingOutput() const noexcept;
  [[nodiscard]] State GetState() const noexcept;
  [[nodiscard]] std::size_t ActiveStreams() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;
  [[nodiscard]] std::expected<void, Status> SetTarget(std::string_view scheme,
                                                      std::string_view authority) noexcept;
  void SetDefaultTimeoutNs(std::uint64_t timeout_ns) noexcept;
  [[nodiscard]] std::string_view LastStatusMessage() const noexcept;
  [[nodiscard]] std::expected<std::size_t, Status> Receive(
      std::span<const std::byte> bytes) noexcept;
  /** Returned bytes remain valid until the next Send or Receive on this session. */
  [[nodiscard]] std::expected<std::span<const std::byte>, Status> Send() noexcept;
  [[nodiscard]] std::expected<void, Status> Ping() noexcept;
  [[nodiscard]] std::expected<void, Status> GoAway() noexcept;
  /** Detaches completion before queuing cancellation, so callback storage may be released. */
  [[nodiscard]] std::expected<void, Status> Cancel(std::int32_t stream_id) noexcept;
  /** Completion payload is borrowed for the duration of the callback. */
  using Completion = void (*)(void*, std::int32_t, int, std::span<const std::byte>) noexcept;
  /** Copies the selected media type and requires that subtype on successful responses. */
  [[nodiscard]] std::expected<std::int32_t, Status> Submit(
      std::string_view path, std::span<const std::byte> payload, void* context,
      Completion completion, std::string_view content_type = "application/grpc",
      std::uint64_t timeout_ns = 0) noexcept;

  using MessageCallback = void (*)(void*, std::int32_t, std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<std::int32_t, Status> Open(
      std::string_view path, void* context, MessageCallback message, Completion completion,
      std::string_view content_type = "application/grpc", std::uint64_t timeout_ns = 0) noexcept;
  [[nodiscard]] std::expected<void, Status> SendMessage(std::int32_t,
                                                        std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> HalfClose(std::int32_t) noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace grpcxx
