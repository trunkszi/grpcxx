#include "grpcxx/channel/load_balancer.h"

#include <ranges>
#include <tuple>
#include <utility>

namespace grpcxx {
namespace {

/** Both flags are required; a ready-but-unhealthy address stays out of the set. */
[[nodiscard]] constexpr bool Selectable(const LoadBalancerEndpoint& endpoint) noexcept {
  return endpoint.healthy && endpoint.ready;
}

constexpr auto kSelectableEnumerated = [](const auto& enumerated) noexcept {
  return Selectable(std::get<1>(enumerated));
};

}  // namespace

std::optional<std::size_t> PickFirstLoadBalancer::Pick(
    std::span<const LoadBalancerEndpoint> endpoints) const noexcept {
  // enumerate runs before filter so the index is the position in `endpoints`.
  // Filtering first would renumber the surviving addresses.
  for (auto enumerated :
       endpoints | std::views::enumerate | std::views::filter(kSelectableEnumerated)) {
    return static_cast<std::size_t>(std::get<0>(enumerated));
  }
  return std::nullopt;
}

std::optional<std::size_t> RoundRobinLoadBalancer::Pick(
    std::span<const LoadBalancerEndpoint> endpoints) noexcept {
  std::optional<std::size_t> first;
  std::optional<std::size_t> at_or_after_cursor;
  for (auto enumerated :
       endpoints | std::views::enumerate | std::views::filter(kSelectableEnumerated)) {
    const auto index = static_cast<std::size_t>(std::get<0>(enumerated));
    if (!first.has_value()) {
      first = index;
    }
    if (!at_or_after_cursor.has_value() && index >= next_index_) {
      at_or_after_cursor = index;
    }
  }
  if (auto chosen = at_or_after_cursor ? at_or_after_cursor : first; chosen.has_value()) {
    next_index_ = *chosen + 1;
    return chosen;
  }
  return std::nullopt;
}

void RoundRobinLoadBalancer::Reset() noexcept { next_index_ = 0; }

std::size_t RoundRobinLoadBalancer::Cursor() const noexcept { return next_index_; }

}  // namespace grpcxx
