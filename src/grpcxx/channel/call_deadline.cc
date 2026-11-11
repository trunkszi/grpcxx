#include "grpcxx/channel/call_deadline.h"

#include <uarch/uarch.hpp>

namespace grpcxx {

std::expected<CallDeadline, Status> CallDeadline::FromGrpcTimeout(std::string_view value,
                                                                  MonotonicTimestamp now) noexcept {
  auto parsed = GrpcTimeout::Parse(value);
  if (!parsed) {
    return std::unexpected(parsed.error());
  }
  return CallDeadline{uarch::arithmetic::saturating_add(now, *parsed)};
}

}  // namespace grpcxx
