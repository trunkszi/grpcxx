#include "grpcxx/channel/channel_connectivity_fsm.h"

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#endif
#ifndef FMT_USE_EXCEPTIONS
#define FMT_USE_EXCEPTIONS 0
#endif
#include <fmt/format.h>

#include <algorithm>
#include <utility>

namespace grpcxx {
namespace {

[[nodiscard]] constexpr bool Due(bool armed, MonotonicTimestamp deadline,
                                 MonotonicTimestamp now) noexcept {
  return armed && now >= deadline;
}

/** Deterministic +/- jitter in per mille, derived from the attempt number. */
[[nodiscard]] constexpr std::int64_t JitterPermille(std::uint32_t attempt,
                                                    std::uint32_t limit) noexcept {
  if (limit == 0U) {
    return 0;
  }
  const auto bounded = std::min(limit, 1000U);
  std::uint32_t mixed = attempt * 2654435761U + 0x9e3779b9U;
  mixed ^= mixed >> 16U;
  mixed *= 0x7feb352dU;
  mixed ^= mixed >> 15U;
  const auto span = static_cast<std::uint32_t>(2U * bounded + 1U);
  return static_cast<std::int64_t>(mixed % span) - static_cast<std::int64_t>(bounded);
}

}  // namespace

ChannelConnectivityFsm::ChannelConnectivityFsm(ChannelTiming timing) noexcept : timing_{timing} {}

ChannelState ChannelConnectivityFsm::State() const noexcept { return state_; }

std::uint64_t ChannelConnectivityFsm::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

std::uint32_t ChannelConnectivityFsm::FailureCount() const noexcept { return failure_count_; }

std::uint32_t ChannelConnectivityFsm::KeepaliveTimeouts() const noexcept {
  return keepalive_timeouts_;
}

bool ChannelConnectivityFsm::BackoffArmed() const noexcept { return backoff_armed_; }

bool ChannelConnectivityFsm::PingArmed() const noexcept { return ping_armed_; }

bool ChannelConnectivityFsm::ConnectArmed() const noexcept { return connect_armed_; }

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::BackoffDeadline() const noexcept {
  if (!backoff_armed_) {
    return std::nullopt;
  }
  return backoff_deadline_;
}

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::PingDeadline() const noexcept {
  if (!ping_armed_) {
    return std::nullopt;
  }
  return ping_deadline_;
}

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::ConnectDeadline() const noexcept {
  if (!connect_armed_) {
    return std::nullopt;
  }
  return connect_deadline_;
}

std::size_t ChannelConnectivityFsm::Describe(std::span<char> buffer) const noexcept {
  if (buffer.size() < 2) {
    return 0;
  }
  const auto written = fmt::format_to_n(
      buffer.data(), buffer.size() - 1, "state={} failures={} rejected={} keepalive_timeouts={}",
      StateName(state_), failure_count_, rejected_transitions_, keepalive_timeouts_);
  const auto count = std::min(written.size, buffer.size() - 1);
  buffer[count] = '\0';
  return count;
}

bool ChannelConnectivityFsm::OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (state_) {
    case ChannelState::kIdle:
      return OnIdle(event);
    case ChannelState::kResolving:
      return OnResolving(event, now);
    case ChannelState::kConnecting:
      return OnConnecting(event, now);
    case ChannelState::kTlsHandshaking:
      return OnTlsHandshaking(event, now);
    case ChannelState::kReady:
      return OnReady(event, now);
    case ChannelState::kTransientFailure:
      return OnTransientFailure(event, now);
    case ChannelState::kShuttingDown:
      return OnShuttingDown(event);
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnTime(MonotonicTimestamp now) noexcept {
  switch (state_) {
    case ChannelState::kConnecting:
      if (Due(connect_armed_, connect_deadline_, now)) {
        return OnEvent(ChannelEvent::kConnectFailed, now);
      }
      return false;
    case ChannelState::kReady:
      if (Due(ping_armed_, ping_deadline_, now)) {
        return OnEvent(ChannelEvent::kPingTimeout, now);
      }
      return false;
    case ChannelState::kTransientFailure:
      if (Due(backoff_armed_, backoff_deadline_, now)) {
        return OnEvent(ChannelEvent::kBackoffElapsed, now);
      }
      return false;
    case ChannelState::kIdle:
    case ChannelState::kResolving:
    case ChannelState::kTlsHandshaking:
    case ChannelState::kShuttingDown:
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnIdle(ChannelEvent event) noexcept {
  switch (event) {
    case ChannelEvent::kStart:
      state_ = ChannelState::kResolving;
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kBackoffElapsed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnResolving(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (event) {
    case ChannelEvent::kResolutionSucceeded:
      state_ = ChannelState::kConnecting;
      connect_armed_ = timing_.connect_timeout_nanos > 0;
      if (connect_armed_) {
        connect_deadline_ = SaturatingAdd(now, timing_.connect_timeout_nanos);
      }
      return true;
    case ChannelEvent::kResolutionFailed:
      EnterTransientFailure(now, false);
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kBackoffElapsed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnConnecting(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (event) {
    case ChannelEvent::kTcpConnected:
      connect_armed_ = false;
      EnterReady();
      return true;
    case ChannelEvent::kTlsRequired:
      connect_armed_ = false;
      state_ = ChannelState::kTlsHandshaking;
      return true;
    case ChannelEvent::kConnectFailed:
      connect_armed_ = false;
      EnterTransientFailure(now, false);
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kBackoffElapsed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnTlsHandshaking(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (event) {
    case ChannelEvent::kTlsSucceeded:
      EnterReady();
      return true;
    case ChannelEvent::kTlsFailed:
      EnterTransientFailure(now, false);
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kBackoffElapsed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnReady(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (event) {
    case ChannelEvent::kTransportFailed:
      EnterTransientFailure(now, false);
      return true;
    case ChannelEvent::kPingSent:
      if (ping_armed_) {
        Reject();
        return false;
      }
      ping_deadline_ = SaturatingAdd(now, timing_.keepalive_timeout_nanos);
      ping_armed_ = true;
      return true;
    case ChannelEvent::kPingAcked:
      if (!ping_armed_) {
        Reject();
        return false;
      }
      ping_armed_ = false;
      return true;
    case ChannelEvent::kPingTimeout:
      if (auto deadline = ping_deadline_; !Due(ping_armed_, deadline, now)) {
        Reject();
        return false;
      }
      EnterTransientFailure(now, true);
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kBackoffElapsed:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnTransientFailure(ChannelEvent event,
                                                MonotonicTimestamp now) noexcept {
  switch (event) {
    case ChannelEvent::kBackoffElapsed:
      if (auto deadline = backoff_deadline_; !Due(backoff_armed_, deadline, now)) {
        Reject();
        return false;
      }
      backoff_armed_ = false;
      state_ = ChannelState::kResolving;
      return true;
    case ChannelEvent::kShutdown:
      EnterShuttingDown();
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

bool ChannelConnectivityFsm::OnShuttingDown(ChannelEvent event) noexcept {
  switch (event) {
    case ChannelEvent::kShutdown:
      // Idempotent: a second shutdown is not a protocol error.
      return true;
    case ChannelEvent::kStart:
    case ChannelEvent::kResolutionSucceeded:
    case ChannelEvent::kResolutionFailed:
    case ChannelEvent::kTcpConnected:
    case ChannelEvent::kConnectFailed:
    case ChannelEvent::kTlsRequired:
    case ChannelEvent::kTlsSucceeded:
    case ChannelEvent::kTlsFailed:
    case ChannelEvent::kTransportFailed:
    case ChannelEvent::kBackoffElapsed:
    case ChannelEvent::kPingSent:
    case ChannelEvent::kPingAcked:
    case ChannelEvent::kPingTimeout:
      Reject();
      return false;
  }
  std::unreachable();
}

void ChannelConnectivityFsm::EnterReady() noexcept {
  failure_count_ = 0;
  ping_armed_ = false;
  backoff_armed_ = false;
  connect_armed_ = false;
  state_ = ChannelState::kReady;
}

void ChannelConnectivityFsm::EnterTransientFailure(MonotonicTimestamp now,
                                                   bool keepalive_timeout) noexcept {
  if (keepalive_timeout && keepalive_timeouts_ < std::numeric_limits<std::uint32_t>::max()) {
    ++keepalive_timeouts_;
  }
  const auto attempt = failure_count_;
  if (failure_count_ < std::numeric_limits<std::uint32_t>::max()) {
    ++failure_count_;
  }
  ping_armed_ = false;
  connect_armed_ = false;
  backoff_deadline_ = SaturatingAdd(now, BackoffDelay(timing_, attempt));
  backoff_armed_ = true;
  state_ = ChannelState::kTransientFailure;
}

void ChannelConnectivityFsm::EnterShuttingDown() noexcept {
  ping_armed_ = false;
  backoff_armed_ = false;
  connect_armed_ = false;
  state_ = ChannelState::kShuttingDown;
}

void ChannelConnectivityFsm::Reject() noexcept { ++rejected_transitions_; }

std::int64_t ChannelConnectivityFsm::BackoffDelay(const ChannelTiming& timing,
                                                  std::uint32_t attempt) noexcept {
  if (timing.initial_backoff_nanos <= 0 || timing.max_backoff_nanos <= 0 ||
      timing.backoff_numerator == 0U || timing.backoff_denominator == 0U) {
    return 0;
  }
  std::int64_t delay = timing.initial_backoff_nanos;
  const std::uint32_t steps = attempt > 63U ? 63U : attempt;
  for (std::uint32_t step = 0; step < steps; ++step) {
    if (delay >= timing.max_backoff_nanos) {
      break;
    }
    delay = SaturatingMulDiv(delay, timing.backoff_numerator, timing.backoff_denominator);
    if (delay >= timing.max_backoff_nanos) {
      delay = timing.max_backoff_nanos;
      break;
    }
  }
  if (const auto jitter = JitterPermille(attempt, timing.jitter_permille); jitter != 0) {
    delay += delay * jitter / 1000;
  }
  delay = std::max<std::int64_t>(delay, 0);
  return std::min(delay, timing.max_backoff_nanos);
}

}  // namespace grpcxx
