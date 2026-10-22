#pragma once

#include <cstddef>
#include <optional>
#include <span>

namespace grpcxx {

/**
 * One resolved address as seen by a channel load balancer.
 *
 * Both `healthy` and `ready` must be true before the address can be picked.
 */
struct LoadBalancerEndpoint {
  bool healthy{false};
  bool ready{false};
};

/**
 * Pick-first policy.
 *
 * Always returns the lowest selectable index. The choice does not rotate
 * while that endpoint stays healthy and ready.
 */
class PickFirstLoadBalancer final {
 public:
  /**
   * Returns the lowest healthy and ready index, or nullopt when none qualify.
   */
  [[nodiscard]] std::optional<std::size_t> Pick(
      std::span<const LoadBalancerEndpoint> endpoints) const noexcept;
};

/**
 * Round-robin policy over the same endpoint list.
 *
 * Each call advances to the next selectable index at or after the cursor,
 * wrapping to the lowest selectable index after the last one.
 */
class RoundRobinLoadBalancer final {
 public:
  /**
   * Returns the next healthy and ready index, or nullopt when none qualify.
   *
   * A miss does not move the cursor.
   */
  [[nodiscard]] std::optional<std::size_t> Pick(
      std::span<const LoadBalancerEndpoint> endpoints) noexcept;

  /** Puts the cursor back at index 0. */
  void Reset() noexcept;

  /** Next index the policy will consider, including non-selectable holes. */
  [[nodiscard]] std::size_t Cursor() const noexcept;

 private:
  std::size_t next_index_{0};
};

}  // namespace grpcxx
