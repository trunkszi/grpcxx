#include "grpcxx/channel/call_deadline.h"

#include <limits>

namespace grpcxx {

std::expected<CallDeadline, Status> CallDeadline::FromGrpcTimeout(std::string_view value,
                                                                  MonotonicTimestamp now) noexcept {
  auto parsed = GrpcTimeout::Parse(value);
  if (!parsed) {
    return std::unexpected(parsed.error());
  }
  constexpr auto kMax = std::numeric_limits<MonotonicTimestamp>::max();
  const auto timeout = *parsed;
  if (now > kMax - timeout) {
    return CallDeadline{kMax};
  }
  return CallDeadline{now + timeout};
}

}  // namespace grpcxx
