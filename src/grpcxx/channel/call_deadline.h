#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <string_view>

#include "grpcxx/interceptor/call_types.h"

namespace grpcxx {

/**
 * Absolute deadline derived from a `grpc-timeout` header value.
 *
 * The deadline is expressed on the caller's monotonic clock. An empty
 * deadline is unarmed and never expires.
 */
class CallDeadline final {
 public:
  constexpr CallDeadline() noexcept = default;

  /**
   * Parses `value` and adds it to `now`.
   *
   * Returns `kInvalidArgument` for a malformed header, and saturates at the
   * clock maximum rather than wrapping.
   */
  [[nodiscard]] static std::expected<CallDeadline, Status> FromGrpcTimeout(
      std::string_view value, MonotonicTimestamp now) noexcept;

  /** Builds an armed deadline from an absolute timestamp. */
  [[nodiscard]] static constexpr CallDeadline FromTimestamp(MonotonicTimestamp deadline) noexcept {
    return CallDeadline{deadline};
  }

  /** Reports whether a deadline was supplied. */
  [[nodiscard]] constexpr bool Armed() const noexcept { return deadline_.has_value(); }

  /** Returns the absolute deadline when armed. */
  [[nodiscard]] constexpr std::optional<MonotonicTimestamp> Deadline() const noexcept {
    return deadline_;
  }

  /** Reports whether an armed deadline has been reached at `now`. */
  [[nodiscard]] constexpr bool Expired(MonotonicTimestamp now) const noexcept {
    return deadline_.has_value() && now >= *deadline_;
  }

  /** Returns the remaining duration, or nullopt when unarmed or expired. */
  [[nodiscard]] constexpr std::optional<MonotonicTimestamp> Remaining(
      MonotonicTimestamp now) const noexcept {
    if (!deadline_.has_value() || now >= *deadline_) {
      return std::nullopt;
    }
    return *deadline_ - now;
  }

 private:
  explicit constexpr CallDeadline(MonotonicTimestamp deadline) noexcept : deadline_{deadline} {}

  std::optional<MonotonicTimestamp> deadline_{};
};

}  // namespace grpcxx
