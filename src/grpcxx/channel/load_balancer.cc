#include "grpcxx/channel/load_balancer.h"

#include <tuple>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

/** Both flags are required; a ready-but-unhealthy address stays out of the set. */
[[nodiscard]] constexpr bool Selectable(const LoadBalancerEndpoint& endpoint) noexcept {
  return endpoint.healthy && endpoint.ready;
}

/** Bits 0..size-1. `size` is at most 64; a set bit is a selectable endpoint. */
[[nodiscard]] uarch::Bitmap64 SelectableMask(
    std::span<const LoadBalancerEndpoint> endpoints) noexcept {
  uarch::Bitmap64 mask{};
  const auto count = static_cast<std::uint32_t>(endpoints.size());
  for (std::uint32_t index = 0; index < count; ++index) {
    if (Selectable(endpoints[index])) {
      mask = mask.set(index);
    }
  }
  return mask;
}

}  // namespace

std::optional<std::size_t> PickFirstLoadBalancer::Pick(
    std::span<const LoadBalancerEndpoint> endpoints) const noexcept {
  if (endpoints.empty()) {
    return std::nullopt;
  }
  if (endpoints.size() <= 64U) {
    const auto chosen = SelectableMask(endpoints).lowest_one();
    if (!chosen) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(*chosen);
  }
  // enumerate runs before the selectable test so the index is the position in
  // `endpoints`. Filtering first would renumber the surviving addresses.
  for (auto enumerated : endpoints | uarch::views::enumerate) {
    if (Selectable(std::get<1>(enumerated))) {
      return static_cast<std::size_t>(std::get<0>(enumerated));
    }
  }
  return std::nullopt;
}

std::optional<std::size_t> RoundRobinLoadBalancer::Pick(
    std::span<const LoadBalancerEndpoint> endpoints) noexcept {
  if (endpoints.empty()) {
    return std::nullopt;
  }
  const auto count = endpoints.size();
  // Wrap at the start of Pick so growing the endpoint span after picking the
  // former tail continues at `chosen + 1`, while unchanged or shrunk spans wrap to 0.
  const std::size_t origin =
      uarch::select_register(uarch::predicate(next_index_ < count), next_index_, std::size_t{0});
  if (count <= 64U) {
    const auto mask = SelectableMask(endpoints);
    auto chosen = mask.next_one_from(static_cast<std::uint32_t>(origin));
    if (!chosen) {
      chosen = mask.lowest_one();
    }
    if (!chosen) {
      return std::nullopt;
    }
    const auto index = static_cast<std::size_t>(*chosen);
    next_index_ = index + 1U;
    return index;
  }

  std::optional<std::size_t> first;
  std::optional<std::size_t> at_or_after_cursor;
  for (auto enumerated : endpoints | uarch::views::enumerate) {
    if (!Selectable(std::get<1>(enumerated))) {
      continue;
    }
    const auto index = static_cast<std::size_t>(std::get<0>(enumerated));
    if (!first.has_value()) {
      first = index;
    }
    if (!at_or_after_cursor.has_value() && index >= origin) {
      at_or_after_cursor = index;
    }
  }
  if (auto picked = at_or_after_cursor ? at_or_after_cursor : first; picked.has_value()) {
    next_index_ = *picked + 1U;
    return picked;
  }
  return std::nullopt;
}

void RoundRobinLoadBalancer::Reset() noexcept { next_index_ = 0; }

std::size_t RoundRobinLoadBalancer::Cursor() const noexcept { return next_index_; }

}  // namespace grpcxx
