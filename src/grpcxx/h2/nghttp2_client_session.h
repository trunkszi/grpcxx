#pragma once
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "grpcxx/core/status.h"
namespace grpcxx {
/** Caller-driven, bounded in-memory HTTP/2 unary session. Native library internals stay private. */
class Nghttp2ClientSession final {
 public:
  static constexpr std::size_t kMaxStreams = 16;
  static constexpr std::size_t kMaxMessageBytes = 65536;
  enum class State : std::uint8_t { kOpen, kDraining, kClosed, kFailed };
  Nghttp2ClientSession() noexcept;
  ~Nghttp2ClientSession();
  Nghttp2ClientSession(const Nghttp2ClientSession&) = delete;
  Nghttp2ClientSession& operator=(const Nghttp2ClientSession&) = delete;
  Nghttp2ClientSession(Nghttp2ClientSession&&) = delete;
  Nghttp2ClientSession& operator=(Nghttp2ClientSession&&) = delete;
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
  /** Detaches completion before queuing cancellation, so callback storage may be released. */
  [[nodiscard]] std::expected<void, Status> Cancel(std::int32_t stream_id) noexcept;
  /** Completion payload is borrowed for the duration of the callback. */
  using Completion = void (*)(void*, std::int32_t, int, std::span<const std::byte>) noexcept;
  /** Copies the selected media type and requires that subtype on successful responses. */
  [[nodiscard]] std::expected<std::int32_t, Status> Submit(
      std::string_view path, std::span<const std::byte> payload, void* context,
      Completion completion, std::string_view content_type = "application/grpc") noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace grpcxx
