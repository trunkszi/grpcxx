#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <ranges>
#include <string_view>

#include "grpcxx/channel/channel_connectivity_fsm.h"
#include "grpcxx/channel/load_balancer.h"

// vcpkg gtest was built against glibc >= 2.38. This gcc-glibc sysroot is 2.34
// and does not export the ISO C23 strtol redirects gtest calls.
extern "C" long __isoc23_strtol(const char* nptr, char** endptr, int base) {
  return std::strtol(nptr, endptr, base);
}

extern "C" unsigned long long __isoc23_strtoull(const char* nptr, char** endptr, int base) {
  return std::strtoull(nptr, endptr, base);
}

namespace grpcxx {
namespace {

constexpr ChannelTiming kTiming{
    .initial_backoff_nanos = 100,
    .max_backoff_nanos = 1000,
    .keepalive_timeout_nanos = 50,
    .connect_timeout_nanos = 500,
    .backoff_numerator = 8,
    .backoff_denominator = 5,
    .jitter_permille = 0,
};

constexpr MonotonicTimestamp kT0{1'000};

[[nodiscard]] std::optional<std::size_t> Index(std::size_t index) noexcept {
  return std::optional<std::size_t>{index};
}

void ExpectTransient(const ChannelConnectivityFsm& fsm, MonotonicTimestamp now,
                     std::uint32_t failures, std::int64_t delay) {
  EXPECT_EQ(fsm.State(), ChannelState::kTransientFailure);
  EXPECT_EQ(fsm.FailureCount(), failures);
  EXPECT_TRUE(fsm.BackoffArmed());
  EXPECT_FALSE(fsm.PingArmed());
  EXPECT_FALSE(fsm.ConnectArmed());
  const auto deadline = fsm.BackoffDeadline();
  ASSERT_TRUE(deadline.has_value());
  EXPECT_EQ(*deadline, now + static_cast<MonotonicTimestamp>(delay));
  EXPECT_EQ(delay, ChannelConnectivityFsm::BackoffDelay(kTiming, failures - 1));
}

}  // namespace

TEST(ChannelBackoffDelay, ScheduleMatchesSaturatingSixteenths) {
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 0), 100);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 1), 160);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 2), 256);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 3), 409);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 4), 654);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 5), 1000);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 6), 1000);
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(kTiming, 80), 1000);

  ChannelTiming zero_denominator = kTiming;
  zero_denominator.backoff_denominator = 0;
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(zero_denominator, 2), 0);

  ChannelTiming negative_initial = kTiming;
  negative_initial.initial_backoff_nanos = -1;
  EXPECT_EQ(ChannelConnectivityFsm::BackoffDelay(negative_initial, 0), 0);

  EXPECT_EQ(ChannelConnectivityFsm::StateName(ChannelState::kReady), "ready");
}

TEST(ChannelBackoffDelay, ProductionDefaultsAreBoundedAndJittered) {
  constexpr ChannelTiming defaults{};
  EXPECT_EQ(defaults.initial_backoff_nanos, 100'000'000);
  EXPECT_EQ(defaults.max_backoff_nanos, 30'000'000'000);
  EXPECT_EQ(defaults.keepalive_timeout_nanos, 20'000'000'000);
  EXPECT_EQ(defaults.connect_timeout_nanos, 20'000'000'000);
  EXPECT_EQ(defaults.backoff_numerator, 8U);
  EXPECT_EQ(defaults.backoff_denominator, 5U);
  EXPECT_EQ(defaults.jitter_permille, 200U);

  const auto first = ChannelConnectivityFsm::BackoffDelay(defaults, 0);
  EXPECT_GE(first, 80'000'000);
  EXPECT_LE(first, 120'000'000);
  const auto capped = ChannelConnectivityFsm::BackoffDelay(defaults, 80);
  EXPECT_GE(capped, 24'000'000'000);
  EXPECT_LE(capped, 30'000'000'000);
}

TEST(ChannelConnectivityFsm, PlaintextHappyPathReachesReady) {
  ChannelConnectivityFsm fsm{kTiming};
  EXPECT_EQ(fsm.State(), ChannelState::kIdle);
  EXPECT_EQ(fsm.RejectedTransitions(), 0U);

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kResolving);
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kConnecting);
  EXPECT_TRUE(fsm.ConnectArmed());
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTcpConnected, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kReady);
  EXPECT_FALSE(fsm.ConnectArmed());
  EXPECT_EQ(fsm.FailureCount(), 0U);
  EXPECT_FALSE(fsm.BackoffArmed());
  EXPECT_FALSE(fsm.PingArmed());
  EXPECT_EQ(fsm.RejectedTransitions(), 0U);
}

TEST(ChannelConnectivityFsm, TlsHappyPathHandshakesBeforeReady) {
  ChannelConnectivityFsm fsm{kTiming};
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, kT0));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kConnecting);
  EXPECT_TRUE(fsm.ConnectArmed());

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTlsRequired, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kTlsHandshaking);
  EXPECT_FALSE(fsm.ConnectArmed());
  EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kTcpConnected, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kTlsHandshaking);
  EXPECT_EQ(fsm.RejectedTransitions(), 1U);

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTlsSucceeded, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kReady);
  EXPECT_EQ(fsm.RejectedTransitions(), 1U);
}

TEST(ChannelConnectivityFsm, ConnectTimeoutEntersTransientFailure) {
  ChannelConnectivityFsm fsm{kTiming};
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, kT0));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  const auto deadline = fsm.ConnectDeadline();
  ASSERT_TRUE(deadline.has_value());
  EXPECT_EQ(*deadline, kT0 + 500U);

  EXPECT_FALSE(fsm.OnTime(kT0 + 499U));
  EXPECT_EQ(fsm.State(), ChannelState::kConnecting);
  EXPECT_TRUE(fsm.OnTime(kT0 + 500U));
  ExpectTransient(fsm, kT0 + 500U, 1U, 100);
}

TEST(ChannelConnectivityFsm, FailuresFromActiveStatesEnterTransientFailure) {
  const std::array<ChannelEvent, 4> kFailures{
      ChannelEvent::kResolutionFailed,
      ChannelEvent::kConnectFailed,
      ChannelEvent::kTlsFailed,
      ChannelEvent::kTransportFailed,
  };
  for (const auto failure : kFailures) {
    ChannelConnectivityFsm fsm{kTiming};
    ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, kT0));
    if (failure != ChannelEvent::kResolutionFailed) {
      ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
    }
    if (failure == ChannelEvent::kTlsFailed) {
      ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTlsRequired, kT0));
    }
    if (failure == ChannelEvent::kTransportFailed) {
      ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTcpConnected, kT0));
      EXPECT_EQ(fsm.State(), ChannelState::kReady);
    }
    ASSERT_TRUE(fsm.OnEvent(failure, kT0));
    ExpectTransient(fsm, kT0, 1U, 100);
    EXPECT_EQ(fsm.KeepaliveTimeouts(), 0U);
  }
}

TEST(ChannelConnectivityFsm, BackoffScheduleUsesAttemptThenReturnsToResolving) {
  ChannelConnectivityFsm fsm{kTiming};
  auto now = kT0;
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, now));

  constexpr std::array<std::int64_t, 6> kDelays{100, 160, 256, 409, 654, 1000};
  for (const auto [attempt, delay] : kDelays | std::views::enumerate) {
    ASSERT_EQ(fsm.State(), ChannelState::kResolving);
    ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionFailed, now));
    ExpectTransient(fsm, now, static_cast<std::uint32_t>(attempt) + 1U, delay);

    const auto early = now + static_cast<MonotonicTimestamp>(delay) - 1U;
    EXPECT_FALSE(fsm.OnTime(early));
    EXPECT_EQ(fsm.State(), ChannelState::kTransientFailure);
    EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kBackoffElapsed, early));
    EXPECT_EQ(fsm.RejectedTransitions(), static_cast<std::uint64_t>(attempt) + 1U);

    now += static_cast<MonotonicTimestamp>(delay);
    ASSERT_TRUE(fsm.OnTime(now));
    EXPECT_EQ(fsm.State(), ChannelState::kResolving);
    EXPECT_FALSE(fsm.BackoffArmed());
  }

  std::array<char, 160> text{};
  const auto length = fsm.Describe(text);
  const auto description = std::string_view{text.data(), length};
  EXPECT_NE(description.find("state=resolving"), std::string_view::npos);
  EXPECT_NE(description.find("failures=6"), std::string_view::npos);
  EXPECT_NE(description.find("rejected=6"), std::string_view::npos);
}

TEST(ChannelConnectivityFsm, FailureCountResetsAfterReady) {
  ChannelConnectivityFsm fsm{kTiming};
  auto now = kT0;
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, now));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionFailed, now));
  ExpectTransient(fsm, now, 1U, 100);

  now += 100U;
  ASSERT_TRUE(fsm.OnTime(now));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, now));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTcpConnected, now));
  ASSERT_EQ(fsm.State(), ChannelState::kReady);
  EXPECT_EQ(fsm.FailureCount(), 0U);

  now += 10U;
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTransportFailed, now));
  ExpectTransient(fsm, now, 1U, 100);
}

TEST(ChannelConnectivityFsm, PingTimeoutAndAck) {
  ChannelConnectivityFsm fsm{kTiming};
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kStart, kT0));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kTcpConnected, kT0));

  EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kPingAcked, kT0));
  EXPECT_EQ(fsm.RejectedTransitions(), 1U);
  EXPECT_EQ(fsm.State(), ChannelState::kReady);

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kPingSent, kT0));
  EXPECT_TRUE(fsm.PingArmed());
  const auto ping_deadline = fsm.PingDeadline();
  ASSERT_TRUE(ping_deadline.has_value());
  EXPECT_EQ(*ping_deadline, kT0 + kTiming.keepalive_timeout_nanos);
  EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kPingSent, kT0));
  EXPECT_EQ(fsm.RejectedTransitions(), 2U);
  EXPECT_EQ(*fsm.PingDeadline(), kT0 + kTiming.keepalive_timeout_nanos);

  EXPECT_FALSE(fsm.OnTime(kT0 + 49U));
  EXPECT_EQ(fsm.State(), ChannelState::kReady);
  EXPECT_EQ(fsm.KeepaliveTimeouts(), 0U);

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kPingAcked, kT0 + 49U));
  EXPECT_FALSE(fsm.PingArmed());
  EXPECT_FALSE(fsm.PingDeadline().has_value());
  EXPECT_FALSE(fsm.OnTime(kT0 + 50U));
  EXPECT_EQ(fsm.State(), ChannelState::kReady);
  EXPECT_EQ(fsm.KeepaliveTimeouts(), 0U);

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kPingSent, MonotonicTimestamp{2'000}));
  EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kPingTimeout, MonotonicTimestamp{2'049}));
  EXPECT_EQ(fsm.State(), ChannelState::kReady);
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kPingTimeout, MonotonicTimestamp{2'050}));
  ExpectTransient(fsm, MonotonicTimestamp{2'050}, 1U, 100);
  EXPECT_EQ(fsm.KeepaliveTimeouts(), 1U);

  ChannelConnectivityFsm timed{kTiming};
  ASSERT_TRUE(timed.OnEvent(ChannelEvent::kStart, kT0));
  ASSERT_TRUE(timed.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  ASSERT_TRUE(timed.OnEvent(ChannelEvent::kTcpConnected, kT0));
  ASSERT_TRUE(timed.OnEvent(ChannelEvent::kPingSent, kT0));
  ASSERT_TRUE(timed.OnTime(kT0 + 50U));
  ExpectTransient(timed, kT0 + 50U, 1U, 100);
  EXPECT_EQ(timed.KeepaliveTimeouts(), 1U);
  EXPECT_EQ(timed.RejectedTransitions(), 0U);
}

TEST(ChannelConnectivityFsm, ShutdownIsIdempotentAndUnexpectedEventsStick) {
  ChannelConnectivityFsm fsm{kTiming};
  constexpr std::array<ChannelEvent, 12> kUnexpected{
      ChannelEvent::kResolutionSucceeded, ChannelEvent::kResolutionFailed,
      ChannelEvent::kTcpConnected,        ChannelEvent::kConnectFailed,
      ChannelEvent::kTlsRequired,         ChannelEvent::kTlsSucceeded,
      ChannelEvent::kTlsFailed,           ChannelEvent::kTransportFailed,
      ChannelEvent::kBackoffElapsed,      ChannelEvent::kPingSent,
      ChannelEvent::kPingAcked,           ChannelEvent::kPingTimeout,
  };
  for (const auto event : kUnexpected) {
    EXPECT_FALSE(fsm.OnEvent(event, kT0));
    EXPECT_EQ(fsm.State(), ChannelState::kIdle);
  }
  EXPECT_EQ(fsm.RejectedTransitions(), kUnexpected.size());

  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kShutdown, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kShuttingDown);
  ASSERT_TRUE(fsm.OnEvent(ChannelEvent::kShutdown, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kShuttingDown);
  EXPECT_EQ(fsm.RejectedTransitions(), kUnexpected.size());
  EXPECT_FALSE(fsm.OnEvent(ChannelEvent::kStart, kT0));
  EXPECT_EQ(fsm.State(), ChannelState::kShuttingDown);
  EXPECT_EQ(fsm.RejectedTransitions(), kUnexpected.size() + 1U);
  EXPECT_FALSE(fsm.OnTime(kT0 + 10'000U));
  EXPECT_EQ(fsm.RejectedTransitions(), kUnexpected.size() + 1U);
}

TEST(LoadBalancer, PickFirstSticksAndRoundRobinRotates) {
  std::array<LoadBalancerEndpoint, 4> endpoints{{
      {.healthy = false, .ready = true},
      {.healthy = true, .ready = false},
      {.healthy = true, .ready = true},
      {.healthy = true, .ready = true},
  }};

  PickFirstLoadBalancer pick_first;
  EXPECT_EQ(pick_first.Pick(endpoints), Index(2));
  EXPECT_EQ(pick_first.Pick(endpoints), Index(2));
  endpoints[0].healthy = true;
  EXPECT_EQ(pick_first.Pick(endpoints), Index(0));
  endpoints[0].ready = false;
  EXPECT_EQ(pick_first.Pick(endpoints), Index(2));

  RoundRobinLoadBalancer round_robin;
  EXPECT_EQ(round_robin.Pick(endpoints), Index(2));
  EXPECT_EQ(round_robin.Cursor(), 3U);
  EXPECT_EQ(round_robin.Pick(endpoints), Index(3));
  EXPECT_EQ(round_robin.Cursor(), 4U);
  EXPECT_EQ(round_robin.Pick(endpoints), Index(2));
  EXPECT_EQ(round_robin.Pick(endpoints), Index(3));

  endpoints[2].healthy = false;
  endpoints[3].ready = false;
  EXPECT_EQ(round_robin.Pick(endpoints), std::nullopt);
  EXPECT_EQ(round_robin.Cursor(), 4U);

  endpoints[3].ready = true;
  EXPECT_EQ(round_robin.Pick(endpoints), Index(3));
  endpoints[3].ready = false;
  endpoints[2].healthy = true;
  EXPECT_EQ(round_robin.Pick(endpoints), Index(2));

  round_robin.Reset();
  endpoints[0] = {.healthy = true, .ready = true};
  endpoints[1] = {.healthy = true, .ready = true};
  endpoints[2] = {.healthy = true, .ready = true};
  endpoints[3] = {.healthy = false, .ready = false};
  EXPECT_EQ(round_robin.Pick(endpoints), Index(0));
  EXPECT_EQ(round_robin.Pick(endpoints), Index(1));
  EXPECT_EQ(round_robin.Pick(endpoints), Index(2));
  EXPECT_EQ(round_robin.Pick(endpoints), Index(0));

  const std::array<LoadBalancerEndpoint, 1> none{{{.healthy = true, .ready = false}}};
  EXPECT_EQ(pick_first.Pick(none), std::nullopt);
  EXPECT_EQ(pick_first.Pick({}), std::nullopt);
  EXPECT_EQ(round_robin.Pick({}), std::nullopt);
}

}  // namespace grpcxx
