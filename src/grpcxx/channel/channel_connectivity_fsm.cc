#include "grpcxx/channel/channel_connectivity_fsm.h"

#include <algorithm>
#include <array>
#include <utility>

namespace grpcxx {
namespace {

constexpr auto kPerMille = uarch::arithmetic::divisor_u64<1000>();

constexpr auto kPingInFlight =
    uarch::BitPattern<ChannelFsmMetaSpec>{}.flag_set<^^ChannelFsmMetaSpec::ping_armed>();
constexpr auto kPingIdle =
    uarch::BitPattern<ChannelFsmMetaSpec>{}.flag_clear<^^ChannelFsmMetaSpec::ping_armed>();

[[nodiscard]] constexpr bool Due(bool armed, MonotonicTimestamp deadline,
                                 MonotonicTimestamp now) noexcept {
  return armed && now >= deadline;
}

[[nodiscard]] uarch::BitWord<ChannelFsmMetaSpec> WithState(uarch::BitWord<ChannelFsmMetaSpec> meta,
                                                           ChannelState state) noexcept {
  return meta.with<^^ChannelFsmMetaSpec::state>(state);
}

/** Signed per-mille offset. `span` is `2 * bounded + 1` for this limit. */
[[nodiscard]] std::int64_t JitterPermille(std::uint32_t attempt, std::uint32_t limit,
                                          const uarch::FastDivisorU32& span) noexcept {
  if (limit == 0U) {
    return 0;
  }
  const auto bounded = std::min(limit, 1000U);
  std::uint32_t mixed = attempt * 2654435761U + 0x9e3779b9U;
  mixed ^= mixed >> 16U;
  mixed *= 0x7feb352dU;
  mixed ^= mixed >> 15U;
  return static_cast<std::int64_t>(span.rem(mixed)) - static_cast<std::int64_t>(bounded);
}

[[nodiscard]] std::int64_t SaturatingMulDiv(std::int64_t value, std::uint32_t numerator,
                                            std::uint32_t denominator,
                                            const uarch::FastDivisorU64& backoff) noexcept {
  constexpr auto kIntMax = std::numeric_limits<std::int64_t>::max();
  if (value <= 0 || numerator == 0U || denominator == 0U) {
    return 0;
  }
  const auto wide = static_cast<__uint128_t>(static_cast<std::uint64_t>(value)) * numerator;
  if (wide > static_cast<__uint128_t>(kIntMax)) {
    return kIntMax;
  }
  return static_cast<std::int64_t>(backoff.div(static_cast<std::uint64_t>(wide)));
}

}  // namespace

ChannelState ChannelConnectivityFsm::State() const noexcept {
  return meta_.get<^^ChannelFsmMetaSpec::state>();
}

std::uint64_t ChannelConnectivityFsm::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

std::uint32_t ChannelConnectivityFsm::FailureCount() const noexcept {
  return meta_.get<^^ChannelFsmMetaSpec::failure_count>();
}

std::uint32_t ChannelConnectivityFsm::KeepaliveTimeouts() const noexcept {
  return keepalive_timeouts_;
}

bool ChannelConnectivityFsm::BackoffArmed() const noexcept {
  return meta_.test<^^ChannelFsmMetaSpec::backoff_armed>();
}

bool ChannelConnectivityFsm::PingArmed() const noexcept {
  return meta_.test<^^ChannelFsmMetaSpec::ping_armed>();
}

bool ChannelConnectivityFsm::ConnectArmed() const noexcept {
  return meta_.test<^^ChannelFsmMetaSpec::connect_armed>();
}

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::BackoffDeadline() const noexcept {
  if (!BackoffArmed()) {
    return std::nullopt;
  }
  return backoff_deadline_;
}

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::PingDeadline() const noexcept {
  if (!PingArmed()) {
    return std::nullopt;
  }
  return ping_deadline_;
}

std::optional<MonotonicTimestamp> ChannelConnectivityFsm::ConnectDeadline() const noexcept {
  if (!ConnectArmed()) {
    return std::nullopt;
  }
  return connect_deadline_;
}

std::size_t ChannelConnectivityFsm::Describe(std::span<char> buffer) const noexcept {
  if (buffer.size() < 2) {
    return 0;
  }
  // Base-10 digits use the frozen divisor. A formatting library's integer
  // path would emit a hardware `div` in this translation unit.
  constexpr auto kTen = uarch::arithmetic::divisor_u64<10>();
  std::size_t used = 0;
  const auto append = [&](std::string_view text) noexcept {
    for (const char ch : text) {
      if (used + 1 >= buffer.size()) {
        return;
      }
      buffer[used] = ch;
      ++used;
    }
  };
  const auto append_u64 = [&](std::uint64_t value) noexcept {
    std::array<char, 20> digits{};
    std::size_t count = 0;
    if (value == 0) {
      digits[count++] = '0';
    }
    while (value != 0 && count < digits.size()) {
      const auto quotient = kTen.div(value);
      const auto digit = static_cast<char>(value - quotient * 10U);
      digits[count++] = static_cast<char>('0' + digit);
      value = quotient;
    }
    while (count > 0) {
      --count;
      append(std::string_view{digits.data() + count, 1});
    }
  };
  append("state=");
  append(StateName(State()));
  append(" failures=");
  append_u64(FailureCount());
  append(" rejected=");
  append_u64(rejected_transitions_);
  append(" keepalive_timeouts=");
  append_u64(keepalive_timeouts_);
  buffer[used] = '\0';
  return used;
}

bool ChannelConnectivityFsm::OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept {
  switch (State()) {
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
  switch (State()) {
    case ChannelState::kConnecting:
      if (Due(ConnectArmed(), connect_deadline_, now)) {
        return OnEvent(ChannelEvent::kConnectFailed, now);
      }
      return false;
    case ChannelState::kReady:
      if (Due(PingArmed(), ping_deadline_, now)) {
        return OnEvent(ChannelEvent::kPingTimeout, now);
      }
      return false;
    case ChannelState::kTransientFailure:
      if (Due(BackoffArmed(), backoff_deadline_, now)) {
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
      meta_ = WithState(meta_, ChannelState::kResolving);
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
    case ChannelEvent::kResolutionSucceeded: {
      const bool arm = timing_.connect_timeout_nanos > 0;
      meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
                  .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kConnecting)
                  .set_flag<^^ChannelFsmMetaSpec::connect_armed>(arm)
                  .apply(meta_);
      if (arm) {
        connect_deadline_ = SaturatingAdd(now, timing_.connect_timeout_nanos);
      }
      return true;
    }
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
      EnterReady();
      return true;
    case ChannelEvent::kTlsRequired:
      meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
                  .set_flag<^^ChannelFsmMetaSpec::connect_armed>(false)
                  .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kTlsHandshaking)
                  .apply(meta_);
      return true;
    case ChannelEvent::kConnectFailed:
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
      if (kPingInFlight.matches(meta_)) {
        Reject();
        return false;
      }
      ping_deadline_ = SaturatingAdd(now, timing_.keepalive_timeout_nanos);
      meta_ = meta_.with<^^ChannelFsmMetaSpec::ping_armed>(true);
      return true;
    case ChannelEvent::kPingAcked:
      if (kPingIdle.matches(meta_)) {
        Reject();
        return false;
      }
      meta_ = meta_.with<^^ChannelFsmMetaSpec::ping_armed>(false);
      return true;
    case ChannelEvent::kPingTimeout:
      if (auto deadline = ping_deadline_; !Due(PingArmed(), deadline, now)) {
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
      if (auto deadline = backoff_deadline_; !Due(BackoffArmed(), deadline, now)) {
        Reject();
        return false;
      }
      meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
                  .set_flag<^^ChannelFsmMetaSpec::backoff_armed>(false)
                  .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kResolving)
                  .apply(meta_);
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
  meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
              .set_flag<^^ChannelFsmMetaSpec::backoff_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::ping_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::connect_armed>(false)
              .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kReady)
              .set_field<^^ChannelFsmMetaSpec::failure_count>(0U)
              .apply(meta_);
}

void ChannelConnectivityFsm::EnterTransientFailure(MonotonicTimestamp now,
                                                   bool keepalive_timeout) noexcept {
  if (keepalive_timeout && keepalive_timeouts_ < std::numeric_limits<std::uint32_t>::max()) {
    ++keepalive_timeouts_;
  }
  const auto attempt = FailureCount();
  meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
              .set_flag<^^ChannelFsmMetaSpec::ping_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::connect_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::backoff_armed>(true)
              .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kTransientFailure)
              .apply(meta_);
  if (attempt < std::numeric_limits<std::uint32_t>::max()) {
    meta_ = meta_.wrapping_add_top_field<^^ChannelFsmMetaSpec::failure_count>(1ULL);
  }
  backoff_deadline_ = SaturatingAdd(now, ApplyBackoff(timing_, attempt, backoff_div_, jitter_div_));
}

void ChannelConnectivityFsm::EnterShuttingDown() noexcept {
  meta_ = uarch::BitPatch<ChannelFsmMetaSpec>{}
              .set_flag<^^ChannelFsmMetaSpec::ping_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::backoff_armed>(false)
              .set_flag<^^ChannelFsmMetaSpec::connect_armed>(false)
              .set_field<^^ChannelFsmMetaSpec::state>(ChannelState::kShuttingDown)
              .apply(meta_);
}

void ChannelConnectivityFsm::Reject() noexcept { ++rejected_transitions_; }

std::int64_t ChannelConnectivityFsm::ApplyBackoff(
    const ChannelTiming& timing, std::uint32_t attempt, const uarch::FastDivisorU64& backoff,
    const uarch::FastDivisorU32& jitter_span) noexcept {
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
    delay = SaturatingMulDiv(delay, timing.backoff_numerator, timing.backoff_denominator, backoff);
    if (delay >= timing.max_backoff_nanos) {
      delay = timing.max_backoff_nanos;
      break;
    }
  }
  if (const auto jitter = JitterPermille(attempt, timing.jitter_permille, jitter_span);
      jitter != 0) {
    const bool negative = jitter < 0;
    const auto magnitude = static_cast<std::uint64_t>(negative ? -jitter : jitter);
    const auto product = static_cast<std::uint64_t>(delay) * magnitude;
    const auto delta = static_cast<std::int64_t>(kPerMille.div(product));
    delay += negative ? -delta : delta;
  }
  delay = std::max<std::int64_t>(delay, 0);
  return std::min(delay, timing.max_backoff_nanos);
}

}  // namespace grpcxx
