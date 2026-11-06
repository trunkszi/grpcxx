#include "grpcxx/channel/channel_coordinator_service.h"

#include <algorithm>

namespace grpcxx {

ChannelCoordinatorService::ChannelCoordinatorService(ChannelTiming timing,
                                                     LoadBalancingPolicy policy) noexcept
    : timing_{timing}, fsm_{timing}, policy_{policy} {}

std::expected<void, Status> ChannelCoordinatorService::Start() noexcept {
  if (Lifecycle() != ServiceState::kStopped) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not stopped"});
  }
  fsm_ = ChannelConnectivityFsm{timing_};
  if (!fsm_.OnEvent(ChannelEvent::kStart, 0)) {
    SetLifecycle(ServiceState::kFailed);
    return std::unexpected(Status{StatusCode::kInternal, "client channel FSM rejected start"});
  }
  SetQuiescent(false);
  SetLifecycle(ServiceState::kRunning);
  return {};
}

ServiceState ChannelCoordinatorService::State() const noexcept { return Lifecycle(); }

std::expected<PollResult, Status> ChannelCoordinatorService::PollOnce(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  if (Lifecycle() != ServiceState::kRunning) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not running"});
  }
  PollResult result{};
  if (budget.timers > 0 && fsm_.OnTime(now)) {
    result.timers = 1;
  }
  result.pending = fsm_.BackoffArmed() || fsm_.PingArmed() || fsm_.ConnectArmed();
  result.deadline = NextDeadline();
  return result;
}

std::expected<void, Status> ChannelCoordinatorService::BeginShutdown(
    MonotonicTimestamp now) noexcept {
  if (Lifecycle() == ServiceState::kStopping) {
    return {};
  }
  if (Lifecycle() != ServiceState::kRunning) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not running"});
  }
  static_cast<void>(fsm_.OnEvent(ChannelEvent::kShutdown, now));
  SetLifecycle(ServiceState::kStopping);
  SetQuiescent(fsm_.State() == ChannelState::kShuttingDown);
  return {};
}

std::expected<bool, Status> ChannelCoordinatorService::PollShutdown(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  static_cast<void>(budget);
  static_cast<void>(now);
  if (Lifecycle() == ServiceState::kStopped) {
    return true;
  }
  if (Lifecycle() != ServiceState::kStopping) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not shutting down"});
  }
  SetQuiescent(fsm_.State() == ChannelState::kShuttingDown);
  return Quiescent();
}

bool ChannelCoordinatorService::Stopped() const noexcept {
  return Lifecycle() == ServiceState::kStopped ||
         (Lifecycle() == ServiceState::kStopping && Quiescent());
}

std::expected<void, Status> ChannelCoordinatorService::Shutdown() noexcept {
  if (Lifecycle() == ServiceState::kStopped || Lifecycle() == ServiceState::kFailed) {
    SetLifecycle(ServiceState::kStopped);
    SetQuiescent(true);
    return {};
  }
  if (Lifecycle() != ServiceState::kStopping || !Quiescent()) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not drained"});
  }
  SetLifecycle(ServiceState::kStopped);
  SetQuiescent(true);
  return {};
}

std::expected<std::size_t, Status> ChannelCoordinatorService::AddEndpoint(
    LoadBalancerEndpoint endpoint) noexcept {
  const auto slot = occupied_.lowest_zero();
  if (!slot.has_value() || *slot >= kMaxEndpoints) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "client channel endpoint capacity exhausted"});
  }
  endpoints_[*slot] = endpoint;
  occupied_ = occupied_.set(*slot);
  return *slot;
}

bool ChannelCoordinatorService::UpdateEndpoint(std::size_t index,
                                               LoadBalancerEndpoint endpoint) noexcept {
  if (index >= kMaxEndpoints || !occupied_.test(static_cast<std::uint32_t>(index))) {
    return false;
  }
  endpoints_[index] = endpoint;
  return true;
}

std::size_t ChannelCoordinatorService::EndpointCount() const noexcept { return occupied_.count(); }

std::optional<std::size_t> ChannelCoordinatorService::PickEndpoint() noexcept {
  const auto live = std::span<const LoadBalancerEndpoint>{endpoints_.data(), occupied_.count()};
  if (policy_ == LoadBalancingPolicy::kRoundRobin) {
    return round_robin_.Pick(live);
  }
  return pick_first_.Pick(live);
}

void ChannelCoordinatorService::SetPolicy(LoadBalancingPolicy policy) noexcept { policy_ = policy; }

bool ChannelCoordinatorService::OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept {
  if (Lifecycle() != ServiceState::kRunning) {
    return false;
  }
  return fsm_.OnEvent(event, now);
}

ChannelState ChannelCoordinatorService::Channel() const noexcept { return fsm_.State(); }

std::uint64_t ChannelCoordinatorService::RejectedTransitions() const noexcept {
  return fsm_.RejectedTransitions();
}

std::uint32_t ChannelCoordinatorService::FailureCount() const noexcept {
  return fsm_.FailureCount();
}

std::optional<MonotonicTimestamp> ChannelCoordinatorService::NextDeadline() const noexcept {
  std::optional<MonotonicTimestamp> next;
  const auto consider = [&next](std::optional<MonotonicTimestamp> candidate) {
    if (candidate.has_value() && (!next.has_value() || *candidate < *next)) {
      next = candidate;
    }
  };
  consider(fsm_.BackoffDeadline());
  consider(fsm_.PingDeadline());
  consider(fsm_.ConnectDeadline());
  return next;
}

ServiceState ChannelCoordinatorService::Lifecycle() const noexcept {
  return life_.get<^^ChannelCoordinatorMetaSpec::state>();
}

void ChannelCoordinatorService::SetLifecycle(ServiceState state) noexcept {
  life_ = life_.with<^^ChannelCoordinatorMetaSpec::state>(state);
}

bool ChannelCoordinatorService::Quiescent() const noexcept {
  return life_.test<^^ChannelCoordinatorMetaSpec::quiescent>();
}

void ChannelCoordinatorService::SetQuiescent(bool quiescent) noexcept {
  life_ = life_.with<^^ChannelCoordinatorMetaSpec::quiescent>(quiescent);
}

}  // namespace grpcxx
