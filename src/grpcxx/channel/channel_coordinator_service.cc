#include "grpcxx/channel/channel_coordinator_service.h"

#include <algorithm>

namespace grpcxx {

ChannelCoordinatorService::ChannelCoordinatorService(ChannelTiming timing,
                                                     LoadBalancingPolicy policy) noexcept
    : timing_{timing}, fsm_{timing}, policy_{policy} {}

std::expected<void, Status> ChannelCoordinatorService::Start() noexcept {
  if (state_ != ServiceState::kStopped) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not stopped"});
  }
  fsm_ = ChannelConnectivityFsm{timing_};
  if (!fsm_.OnEvent(ChannelEvent::kStart, 0)) {
    state_ = ServiceState::kFailed;
    return std::unexpected(Status{StatusCode::kInternal, "client channel FSM rejected start"});
  }
  quiescent_ = false;
  state_ = ServiceState::kRunning;
  return {};
}

ServiceState ChannelCoordinatorService::State() const noexcept { return state_; }

std::expected<PollResult, Status> ChannelCoordinatorService::PollOnce(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  if (state_ != ServiceState::kRunning) {
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
  if (state_ == ServiceState::kStopping) {
    return {};
  }
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not running"});
  }
  static_cast<void>(fsm_.OnEvent(ChannelEvent::kShutdown, now));
  state_ = ServiceState::kStopping;
  quiescent_ = fsm_.State() == ChannelState::kShuttingDown;
  return {};
}

std::expected<bool, Status> ChannelCoordinatorService::PollShutdown(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  static_cast<void>(budget);
  static_cast<void>(now);
  if (state_ == ServiceState::kStopped) {
    return true;
  }
  if (state_ != ServiceState::kStopping) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not shutting down"});
  }
  quiescent_ = fsm_.State() == ChannelState::kShuttingDown;
  return quiescent_;
}

bool ChannelCoordinatorService::Stopped() const noexcept {
  return state_ == ServiceState::kStopped || (state_ == ServiceState::kStopping && quiescent_);
}

std::expected<void, Status> ChannelCoordinatorService::Shutdown() noexcept {
  if (state_ == ServiceState::kStopped || state_ == ServiceState::kFailed) {
    state_ = ServiceState::kStopped;
    quiescent_ = true;
    return {};
  }
  if (state_ != ServiceState::kStopping || !quiescent_) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "client channel is not drained"});
  }
  state_ = ServiceState::kStopped;
  quiescent_ = true;
  return {};
}

std::expected<std::size_t, Status> ChannelCoordinatorService::AddEndpoint(
    LoadBalancerEndpoint endpoint) noexcept {
  if (endpoint_count_ >= endpoints_.size()) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "client channel endpoint capacity exhausted"});
  }
  const auto index = endpoint_count_;
  endpoints_[index] = endpoint;
  ++endpoint_count_;
  return index;
}

bool ChannelCoordinatorService::UpdateEndpoint(std::size_t index,
                                               LoadBalancerEndpoint endpoint) noexcept {
  if (index >= endpoint_count_) {
    return false;
  }
  endpoints_[index] = endpoint;
  return true;
}

std::size_t ChannelCoordinatorService::EndpointCount() const noexcept { return endpoint_count_; }

std::optional<std::size_t> ChannelCoordinatorService::PickEndpoint() noexcept {
  const auto live = std::span<const LoadBalancerEndpoint>{endpoints_.data(), endpoint_count_};
  if (policy_ == LoadBalancingPolicy::kRoundRobin) {
    return round_robin_.Pick(live);
  }
  return pick_first_.Pick(live);
}

void ChannelCoordinatorService::SetPolicy(LoadBalancingPolicy policy) noexcept { policy_ = policy; }

bool ChannelCoordinatorService::OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept {
  if (state_ != ServiceState::kRunning) {
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

}  // namespace grpcxx
