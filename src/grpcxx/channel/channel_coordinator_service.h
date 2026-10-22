#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "grpcxx/channel/channel_connectivity_fsm.h"
#include "grpcxx/channel/load_balancer.h"

namespace grpcxx {

/** Endpoint selection policy for `ChannelCoordinatorService`. */
enum class LoadBalancingPolicy : std::uint8_t {
  kPickFirst = 0,
  kRoundRobin = 1,
};

/**
 * Caller-driven client channel coordinator.
 *
 * Owns the connectivity FSM and the endpoint load balancer. Transport code
 * feeds resolution, TCP, TLS, HTTP/2, and PING outcomes through `OnEvent`;
 * `PollOnce` fires the armed backoff/connect/PING deadlines. The service does
 * not own a socket or a thread: DNS and transport remain caller-composed.
 */
class ChannelCoordinatorService final {
 public:
  static constexpr std::size_t kMaxEndpoints = 8;

  explicit ChannelCoordinatorService(
      ChannelTiming timing = {},
      LoadBalancingPolicy policy = LoadBalancingPolicy::kPickFirst) noexcept;

  ChannelCoordinatorService(const ChannelCoordinatorService&) = delete;
  ChannelCoordinatorService& operator=(const ChannelCoordinatorService&) = delete;
  ChannelCoordinatorService(ChannelCoordinatorService&&) = delete;
  ChannelCoordinatorService& operator=(ChannelCoordinatorService&&) = delete;

  /** Enters `kRunning` and starts the FSM in `kResolving`. */
  [[nodiscard]] std::expected<void, Status> Start() noexcept;

  /** Returns the lifecycle phase. */
  [[nodiscard]] ServiceState State() const noexcept;

  /** Fires an armed backoff, connect, or PING deadline. */
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept;

  /** Stops the channel and moves the FSM to `kShuttingDown`. */
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp now) noexcept;

  /** Reports whether the FSM has reached `kShuttingDown`. */
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget budget,
                                                         MonotonicTimestamp now) noexcept;

  /** Reports whether shutdown has drained. */
  [[nodiscard]] bool Stopped() const noexcept;

  /** Releases the service state. A running service must drain first. */
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;

  /** Appends an endpoint; returns its index or `kResourceExhausted`. */
  [[nodiscard]] std::expected<std::size_t, Status> AddEndpoint(
      LoadBalancerEndpoint endpoint) noexcept;

  /** Updates one endpoint; returns false for an out-of-range index. */
  [[nodiscard]] bool UpdateEndpoint(std::size_t index, LoadBalancerEndpoint endpoint) noexcept;

  /** Number of registered endpoints. */
  [[nodiscard]] std::size_t EndpointCount() const noexcept;

  /** Selects an endpoint using the configured policy. */
  [[nodiscard]] std::optional<std::size_t> PickEndpoint() noexcept;

  /** Changes the endpoint selection policy. */
  void SetPolicy(LoadBalancingPolicy policy) noexcept;

  /** Feeds one transport event into the FSM. */
  [[nodiscard]] bool OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept;

  /** Current FSM phase. */
  [[nodiscard]] ChannelState Channel() const noexcept;

  /** Events rejected by the FSM. */
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

  /** Consecutive transient failures since the last ready transition. */
  [[nodiscard]] std::uint32_t FailureCount() const noexcept;

 private:
  [[nodiscard]] std::optional<MonotonicTimestamp> NextDeadline() const noexcept;

  ChannelTiming timing_{};
  ChannelConnectivityFsm fsm_{};
  PickFirstLoadBalancer pick_first_{};
  RoundRobinLoadBalancer round_robin_{};
  std::array<LoadBalancerEndpoint, kMaxEndpoints> endpoints_{};
  std::size_t endpoint_count_{0};
  LoadBalancingPolicy policy_{LoadBalancingPolicy::kPickFirst};
  ServiceState state_{ServiceState::kStopped};
  bool quiescent_{false};
};

static_assert(ServiceConcept<ChannelCoordinatorService>);

}  // namespace grpcxx
