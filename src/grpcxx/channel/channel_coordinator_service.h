#pragma once

#include <cares/types/endpoint.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <nexora/pool/endpoint_failover.hpp>
#include <optional>
#include <span>

#include "grpcxx/channel/channel_connectivity_fsm.h"
#include "grpcxx/core/service.h"
#include "grpcxx/core/status.h"

namespace grpcxx {

using EndpointSelectionPolicy = nexora::EndpointSelectionPolicy;
using EndpointFailoverConfig = nexora::EndpointFailoverConfig;

/**
 * Caller-driven gRPC channel connectivity and endpoint failover service.
 *
 * Composes `ChannelConnectivityFsm` with `nexora::EndpointFailover` over
 * `cares::Endpoint` targets without threads, sockets, or heap allocation.
 */
class ChannelCoordinatorService final {
 public:
  static constexpr std::size_t kMaxEndpoints{16U};
  using Failover = nexora::EndpointFailover<kMaxEndpoints>;

  explicit ChannelCoordinatorService(ChannelTiming timing = {},
                                     EndpointFailoverConfig failover = {
                                         .policy = EndpointSelectionPolicy::kPickFirst}) noexcept;

  ChannelCoordinatorService(const ChannelCoordinatorService&) =
      delete("ChannelCoordinatorService owns channel connectivity state; copying is disabled");
  ChannelCoordinatorService& operator=(const ChannelCoordinatorService&) =
      delete("ChannelCoordinatorService owns channel connectivity state; copying is disabled");
  ChannelCoordinatorService(ChannelCoordinatorService&&) =
      delete("ChannelCoordinatorService owns channel connectivity state; moving is disabled");
  ChannelCoordinatorService& operator=(ChannelCoordinatorService&&) =
      delete("ChannelCoordinatorService owns channel connectivity state; moving is disabled");

  /** Enters `ServiceState::kRunning` and starts resolution on the connectivity FSM. */
  [[nodiscard]] std::expected<void, Status> Start() noexcept;

  /** Current service lifecycle state. */
  [[nodiscard]] ServiceState State() const noexcept;

  /** Advances due timers within `budget` and reports the earliest armed deadline. */
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept;

  /** Transitions the connectivity FSM to `kShuttingDown` and begins draining. */
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp now) noexcept;

  /** Reports whether shutdown draining has reached quiescence. */
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget budget,
                                                         MonotonicTimestamp now) noexcept;

  /** True when stopped or when `kStopping` has drained to `kShuttingDown`. */
  [[nodiscard]] bool Stopped() const noexcept;

  /** Resets channel state and clears endpoints once drained. */
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;

  /** Applies one transport or resolver event to the connectivity FSM. */
  [[nodiscard]] bool OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept;

  /** Current gRPC connectivity state. */
  [[nodiscard]] ChannelState Channel() const noexcept;

  /** Consecutive connectivity failure count on the channel FSM. */
  [[nodiscard]] std::uint32_t FailureCount() const noexcept;

  /** Total unexpected events rejected by the channel FSM. */
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

  /** Earliest armed connect, backoff, or keepalive ping deadline. */
  [[nodiscard]] std::optional<MonotonicTimestamp> NextDeadline() const noexcept;

  /** Updates the selection policy and keeps quarantine, failure counts, EWMA, and readiness. */
  void SetPolicy(EndpointSelectionPolicy policy) noexcept;

  /** Active endpoint selection policy. */
  [[nodiscard]] EndpointSelectionPolicy Policy() const noexcept;

  /** Replaces failover limits and keeps quarantine, failure counts, EWMA, and readiness. */
  void SetFailoverConfig(EndpointFailoverConfig config) noexcept;

  /** Active failover configuration. */
  [[nodiscard]] const EndpointFailoverConfig& FailoverConfig() const noexcept;

  /** Replaces the resolved endpoint table and marks all loaded endpoints ready. */
  [[nodiscard]] std::expected<void, Status> SetEndpoints(
      std::span<const cares::Endpoint> endpoints) noexcept;

  /**
   * Reconciles a refreshed endpoint list in place without resetting existing health state.
   *
   * Preserves quarantine deadlines, failure counts, EWMA latency, readiness, and selection cursor
   * for surviving endpoints while deduplicating duplicates and marking new slots ready.
   */
  [[nodiscard]] std::expected<void, Status> ReconcileEndpoints(
      std::span<const cares::Endpoint> endpoints) noexcept;

  /**
   * Appends one validated endpoint up to `kMaxEndpoints` and returns its slot index.
   *
   * Quarantine, failure counts, and EWMA already recorded on earlier slots stay in place.
   */
  [[nodiscard]] std::expected<std::size_t, Status> AddEndpoint(const cares::Endpoint& endpoint,
                                                               bool ready = true) noexcept;

  /** Clears all configured endpoints. */
  void ClearEndpoints() noexcept;

  /** Number of configured endpoints. */
  [[nodiscard]] std::size_t EndpointCount() const noexcept;

  /**
   * Counts endpoints outside quarantine at `now`.
   *
   * `now` is nanoseconds on the caller monotonic clock. Readiness is a separate bit (`IsReady`).
   */
  [[nodiscard]] std::size_t HealthyEndpointCount(MonotonicTimestamp now) const noexcept;

  /** Read-only view of configured endpoints. */
  [[nodiscard]] std::span<const cares::Endpoint> Endpoints() const noexcept;

  /** Returns the endpoint at `index`, or `std::nullopt` when out of bounds. */
  [[nodiscard]] std::optional<cares::Endpoint> EndpointAt(std::size_t index) const noexcept;

  /** Marks endpoint `index` transport-ready or unready for selection. */
  [[nodiscard]] std::expected<void, Status> SetReady(std::size_t index, bool ready) noexcept;

  /** Reports whether endpoint `index` is marked ready. */
  [[nodiscard]] bool IsReady(std::size_t index) const noexcept;

  /**
   * Reports whether endpoint `index` is outside quarantine at `now`.
   *
   * `now` is nanoseconds on the caller monotonic clock. A quarantined endpoint may still be
   * ready, and a healthy endpoint may be unready. An index past the table is not healthy.
   */
  [[nodiscard]] bool IsHealthy(std::size_t index, MonotonicTimestamp now) const noexcept;

  /** Selects the next healthy and ready endpoint index at monotonic nanoseconds `now`. */
  [[nodiscard]] std::expected<std::size_t, Status> SelectIndex(MonotonicTimestamp now) noexcept;

  /** Selects the next healthy and ready endpoint address at monotonic nanoseconds `now`. */
  [[nodiscard]] std::expected<cares::Endpoint, Status> SelectEndpoint(
      MonotonicTimestamp now) noexcept;

  /** Clears failure counters on endpoint `index` without updating EWMA latency. */
  [[nodiscard]] std::expected<void, Status> MarkSuccess(std::size_t index) noexcept;

  /** Clears failure counters on `endpoint` without updating EWMA latency. */
  [[nodiscard]] std::expected<void, Status> MarkSuccess(const cares::Endpoint& endpoint) noexcept;

  /** Clears failure counters and updates EWMA latency (`rtt_ns`) on endpoint `index`. */
  [[nodiscard]] std::expected<void, Status> RecordSuccess(std::size_t index,
                                                          std::uint64_t rtt_ns) noexcept;

  /** Clears failure counters and updates EWMA latency (`rtt_ns`) on `endpoint`. */
  [[nodiscard]] std::expected<void, Status> RecordSuccess(const cares::Endpoint& endpoint,
                                                          std::uint64_t rtt_ns) noexcept;

  /** Records a failure on endpoint `index`, entering exponential cooldown past threshold. */
  [[nodiscard]] std::expected<void, Status> MarkFailure(std::size_t index,
                                                        MonotonicTimestamp now) noexcept;

  /** Records a failure on `endpoint`, entering exponential cooldown past threshold. */
  [[nodiscard]] std::expected<void, Status> MarkFailure(const cares::Endpoint& endpoint,
                                                        MonotonicTimestamp now) noexcept;

  /** Consecutive failure count recorded on endpoint `index`. */
  [[nodiscard]] std::uint32_t EndpointFailures(std::size_t index) const noexcept;

  /** Smoothed EWMA RTT in nanoseconds for endpoint `index`. */
  [[nodiscard]] std::uint64_t EwmaLatencyNs(std::size_t index) const noexcept;

  /** Monotonic timestamp until which endpoint `index` remains quarantined (`0` if unquarantined).
   */
  [[nodiscard]] MonotonicTimestamp QuarantinedUntil(std::size_t index) const noexcept;

 private:
  ChannelTiming timing_{};
  ChannelConnectivityFsm fsm_;
  Failover failover_;
  ServiceState state_{ServiceState::kStopped};
};

static_assert(ServiceConcept<ChannelCoordinatorService>);

}  // namespace grpcxx
