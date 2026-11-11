#include "grpcxx/channel/channel_coordinator_service.h"

#include <algorithm>
#include <array>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

[[nodiscard]] constexpr bool ValidEndpoint(const cares::Endpoint& endpoint) noexcept {
  return (endpoint.ip.IsV4() || endpoint.ip.IsV6()) && endpoint.port != 0U;
}

[[nodiscard]] std::optional<MonotonicTimestamp> EarlierOptional(
    std::optional<MonotonicTimestamp> first, std::optional<MonotonicTimestamp> second) noexcept {
  if (!first.has_value()) {
    return second;
  }
  if (!second.has_value()) {
    return first;
  }
  return std::min(*first, *second);
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MapFailoverError(nexora::Error error) noexcept {
  uarch::cold_path();
  if (error == nexora::Error::InvalidArgument()) {
    return Status::FromLiteral(StatusCode::kInvalidArgument, "invalid channel endpoint");
  }
  if (error == nexora::Error::BufferOverflow()) {
    return Status::FromLiteral(StatusCode::kResourceExhausted, "channel endpoint table is full");
  }
  if (error == nexora::Error::FromRuntime(nexora::RuntimeErrorCode::kAllEndpointsUnhealthy)) {
    return Status::FromLiteral(StatusCode::kUnavailable, "no healthy ready endpoint available");
  }
  return Status::FromLiteral(StatusCode::kInternal, error.Message());
}

[[nodiscard, gnu::cold, gnu::noinline]] Status InvalidEndpointStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInvalidArgument, "invalid channel endpoint"};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status EndpointTableFullStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kResourceExhausted, "channel endpoint table is full"};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status EndpointOutOfRangeStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kOutOfRange, "channel endpoint index out of range"};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status EndpointNotFoundStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kNotFound, "channel endpoint not found"};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status InvalidBudgetStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInvalidArgument, "poll budget must be positive"};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status PreconditionStatus(const char* detail) noexcept {
  uarch::cold_path();
  return Status{StatusCode::kFailedPrecondition, detail};
}

}  // namespace

ChannelCoordinatorService::ChannelCoordinatorService(ChannelTiming timing,
                                                     EndpointFailoverConfig failover) noexcept
    : timing_(timing), fsm_(timing), failover_(failover) {}

std::expected<void, Status> ChannelCoordinatorService::Start() noexcept {
  if (uarch::unlikely(state_ != ServiceState::kStopped)) {
    return std::unexpected(PreconditionStatus("channel coordinator is already active"));
  }
  fsm_ = ChannelConnectivityFsm{timing_};
  if (uarch::unlikely(!fsm_.OnEvent(ChannelEvent::kStart, MonotonicTimestamp{0}))) {
    state_ = ServiceState::kFailed;
    return std::unexpected(Status{StatusCode::kInternal, "channel FSM rejected start"});
  }
  state_ = ServiceState::kRunning;
  return {};
}

ServiceState ChannelCoordinatorService::State() const noexcept { return state_; }

std::expected<PollResult, Status> ChannelCoordinatorService::PollOnce(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  if (uarch::unlikely(!budget.Valid())) {
    return std::unexpected(InvalidBudgetStatus());
  }
  if (uarch::unlikely(state_ != ServiceState::kRunning)) {
    return std::unexpected(PreconditionStatus("channel coordinator is not running"));
  }

  PollResult result{};
  if (budget.timers > 0U && fsm_.OnTime(now)) {
    result.timers = 1U;
  }
  result.pending = uarch::eager_any(fsm_.BackoffArmed(), fsm_.PingArmed(), fsm_.ConnectArmed());
  result.deadline = NextDeadline();
  return result;
}

std::expected<void, Status> ChannelCoordinatorService::BeginShutdown(
    MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopping || state_ == ServiceState::kStopped) {
    return {};
  }
  if (uarch::unlikely(state_ != ServiceState::kRunning)) {
    return std::unexpected(PreconditionStatus("channel coordinator cannot drain from this state"));
  }
  static_cast<void>(fsm_.OnEvent(ChannelEvent::kShutdown, now));
  state_ = ServiceState::kStopping;
  return {};
}

std::expected<bool, Status> ChannelCoordinatorService::PollShutdown(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  if (uarch::unlikely(!budget.Valid())) {
    return std::unexpected(InvalidBudgetStatus());
  }
  if (state_ == ServiceState::kStopped) {
    return true;
  }
  if (uarch::unlikely(state_ != ServiceState::kStopping)) {
    return std::unexpected(PreconditionStatus("channel coordinator shutdown has not started"));
  }
  static_cast<void>(now);
  return fsm_.State() == ChannelState::kShuttingDown;
}

bool ChannelCoordinatorService::Stopped() const noexcept {
  return state_ == ServiceState::kStopped ||
         (state_ == ServiceState::kStopping && fsm_.State() == ChannelState::kShuttingDown);
}

std::expected<void, Status> ChannelCoordinatorService::Shutdown() noexcept {
  if (state_ == ServiceState::kStopped) {
    return {};
  }
  if (uarch::unlikely(state_ == ServiceState::kRunning)) {
    return std::unexpected(
        PreconditionStatus("call BeginShutdown before Shutdown on a running channel"));
  }
  fsm_ = ChannelConnectivityFsm{timing_};
  failover_.Reset({});
  state_ = ServiceState::kStopped;
  return {};
}

bool ChannelCoordinatorService::OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept {
  if (uarch::unlikely(state_ != ServiceState::kRunning)) {
    return false;
  }
  return fsm_.OnEvent(event, now);
}

ChannelState ChannelCoordinatorService::Channel() const noexcept { return fsm_.State(); }

std::uint32_t ChannelCoordinatorService::FailureCount() const noexcept {
  return fsm_.FailureCount();
}

std::uint64_t ChannelCoordinatorService::RejectedTransitions() const noexcept {
  return fsm_.RejectedTransitions();
}

std::optional<MonotonicTimestamp> ChannelCoordinatorService::NextDeadline() const noexcept {
  const auto connect_or_backoff = EarlierOptional(fsm_.ConnectDeadline(), fsm_.BackoffDeadline());
  return EarlierOptional(connect_or_backoff, fsm_.PingDeadline());
}

void ChannelCoordinatorService::SetPolicy(EndpointSelectionPolicy policy) noexcept {
  failover_.SetPolicy(policy);
}

EndpointSelectionPolicy ChannelCoordinatorService::Policy() const noexcept {
  return failover_.Config().policy;
}

void ChannelCoordinatorService::SetFailoverConfig(EndpointFailoverConfig config) noexcept {
  failover_.SetConfig(std::move(config));
}

const EndpointFailoverConfig& ChannelCoordinatorService::FailoverConfig() const noexcept {
  return failover_.Config();
}

std::expected<void, Status> ChannelCoordinatorService::SetEndpoints(
    std::span<const cares::Endpoint> endpoints) noexcept {
  if (uarch::unlikely(endpoints.empty())) {
    return std::unexpected(InvalidEndpointStatus());
  }
  if (uarch::unlikely(endpoints.size() > kMaxEndpoints)) {
    return std::unexpected(EndpointTableFullStatus());
  }
  for (const auto& endpoint : endpoints) {
    if (uarch::unlikely(!ValidEndpoint(endpoint))) {
      return std::unexpected(InvalidEndpointStatus());
    }
  }
  const auto res = failover_.SetEndpoints(endpoints);
  if (uarch::unlikely(!res.has_value())) {
    return std::unexpected(MapFailoverError(res.error()));
  }
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::ReconcileEndpoints(
    std::span<const cares::Endpoint> endpoints) noexcept {
  if (uarch::unlikely(endpoints.empty())) {
    return std::unexpected(InvalidEndpointStatus());
  }
  if (uarch::unlikely(endpoints.size() > kMaxEndpoints)) {
    return std::unexpected(EndpointTableFullStatus());
  }
  for (const auto& endpoint : endpoints) {
    if (uarch::unlikely(!ValidEndpoint(endpoint))) {
      return std::unexpected(InvalidEndpointStatus());
    }
  }
  const auto res = failover_.ReconcileEndpoints(endpoints);
  if (uarch::unlikely(!res.has_value())) {
    return std::unexpected(MapFailoverError(res.error()));
  }
  return {};
}

std::expected<std::size_t, Status> ChannelCoordinatorService::AddEndpoint(
    const cares::Endpoint& endpoint, bool ready) noexcept {
  if (uarch::unlikely(!ValidEndpoint(endpoint))) {
    return std::unexpected(InvalidEndpointStatus());
  }
  const auto added = failover_.AddEndpoint(endpoint);
  if (uarch::unlikely(!added.has_value())) {
    return std::unexpected(MapFailoverError(added.error()));
  }
  if (!*added) {
    const auto existing = failover_.FindEndpoint(endpoint);
    if (uarch::unlikely(!existing.has_value())) {
      return std::unexpected(InvalidEndpointStatus());
    }
    return *existing;
  }
  const std::size_t index = failover_.Size() - 1U;
  if (!ready) {
    failover_.SetReady(index, false);
  }
  return index;
}

void ChannelCoordinatorService::ClearEndpoints() noexcept { failover_.Reset({}); }

std::size_t ChannelCoordinatorService::EndpointCount() const noexcept { return failover_.Size(); }

std::size_t ChannelCoordinatorService::HealthyEndpointCount(MonotonicTimestamp now) const noexcept {
  return failover_.HealthyCount(now);
}

std::span<const cares::Endpoint> ChannelCoordinatorService::Endpoints() const noexcept {
  return failover_.Endpoints();
}

std::optional<cares::Endpoint> ChannelCoordinatorService::EndpointAt(
    std::size_t index) const noexcept {
  if (uarch::unlikely(index >= failover_.Size())) {
    return std::nullopt;
  }
  return failover_.EndpointAt(index);
}

std::expected<void, Status> ChannelCoordinatorService::SetReady(std::size_t index,
                                                                bool ready) noexcept {
  if (uarch::unlikely(index >= failover_.Size())) {
    return std::unexpected(EndpointOutOfRangeStatus());
  }
  failover_.SetReady(index, ready);
  return {};
}

bool ChannelCoordinatorService::IsReady(std::size_t index) const noexcept {
  return failover_.IsReady(index);
}

bool ChannelCoordinatorService::IsHealthy(std::size_t index,
                                          MonotonicTimestamp now) const noexcept {
  return failover_.IsHealthy(index, now);
}

std::expected<std::size_t, Status> ChannelCoordinatorService::SelectIndex(
    MonotonicTimestamp now) noexcept {
  const auto res = failover_.SelectNext(now);
  if (uarch::unlikely(!res.has_value())) {
    return std::unexpected(MapFailoverError(res.error()));
  }
  return *res;
}

std::expected<cares::Endpoint, Status> ChannelCoordinatorService::SelectEndpoint(
    MonotonicTimestamp now) noexcept {
  const auto res = failover_.SelectEndpoint(now);
  if (uarch::unlikely(!res.has_value())) {
    return std::unexpected(MapFailoverError(res.error()));
  }
  return *res;
}

std::expected<void, Status> ChannelCoordinatorService::MarkSuccess(std::size_t index) noexcept {
  if (uarch::unlikely(index >= failover_.Size())) {
    return std::unexpected(EndpointOutOfRangeStatus());
  }
  failover_.MarkSuccess(index);
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::MarkSuccess(
    const cares::Endpoint& endpoint) noexcept {
  const auto index = failover_.FindEndpoint(endpoint);
  if (uarch::unlikely(!index.has_value())) {
    return std::unexpected(EndpointNotFoundStatus());
  }
  failover_.MarkSuccess(*index);
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::RecordSuccess(
    std::size_t index, std::uint64_t rtt_ns) noexcept {
  if (uarch::unlikely(index >= failover_.Size())) {
    return std::unexpected(EndpointOutOfRangeStatus());
  }
  failover_.RecordSuccess(index, rtt_ns);
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::RecordSuccess(
    const cares::Endpoint& endpoint, std::uint64_t rtt_ns) noexcept {
  const auto index = failover_.FindEndpoint(endpoint);
  if (uarch::unlikely(!index.has_value())) {
    return std::unexpected(EndpointNotFoundStatus());
  }
  failover_.RecordSuccess(*index, rtt_ns);
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::MarkFailure(
    std::size_t index, MonotonicTimestamp now) noexcept {
  if (uarch::unlikely(index >= failover_.Size())) {
    return std::unexpected(EndpointOutOfRangeStatus());
  }
  failover_.MarkFailure(index, now);
  return {};
}

std::expected<void, Status> ChannelCoordinatorService::MarkFailure(
    const cares::Endpoint& endpoint, MonotonicTimestamp now) noexcept {
  const auto index = failover_.FindEndpoint(endpoint);
  if (uarch::unlikely(!index.has_value())) {
    return std::unexpected(EndpointNotFoundStatus());
  }
  failover_.MarkFailure(*index, now);
  return {};
}

std::uint32_t ChannelCoordinatorService::EndpointFailures(std::size_t index) const noexcept {
  return failover_.FailureCount(index);
}

std::uint64_t ChannelCoordinatorService::EwmaLatencyNs(std::size_t index) const noexcept {
  return failover_.EwmaLatencyNs(index);
}

MonotonicTimestamp ChannelCoordinatorService::QuarantinedUntil(std::size_t index) const noexcept {
  return failover_.QuarantinedUntilNs(index);
}

}  // namespace grpcxx
