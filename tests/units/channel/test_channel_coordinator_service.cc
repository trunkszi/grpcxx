#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <optional>

#include "grpcxx/channel/call_deadline.h"
#include "grpcxx/channel/channel_coordinator_service.h"

// Host sysroot compatibility shims for GTest built against newer glibc headers.
extern "C" long __isoc23_strtol(const char* text, char** end, int base) {
  return std::strtol(text, end, base);
}

extern "C" unsigned long long __isoc23_strtoull(const char* text, char** end, int base) {
  return std::strtoull(text, end, base);
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

[[nodiscard]] constexpr cares::Endpoint MakeEndpoint(std::uint8_t host_octet,
                                                     std::uint16_t port) noexcept {
  return cares::Endpoint{cares::IpAddress::FromV4({127U, 0U, 0U, host_octet}), port};
}

TEST(CallDeadline, ParsesGrpcTimeoutAndExpires) {
  const auto parsed = CallDeadline::FromGrpcTimeout("1S", kT0);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_TRUE(parsed->Armed());
  EXPECT_EQ(parsed->Deadline(), std::optional<MonotonicTimestamp>{kT0 + 1'000'000'000U});
  EXPECT_FALSE(parsed->Expired(kT0));
  EXPECT_EQ(parsed->Remaining(kT0), std::optional<MonotonicTimestamp>{1'000'000'000U});
  EXPECT_TRUE(parsed->Expired(kT0 + 1'000'000'000U));
  EXPECT_FALSE(parsed->Remaining(kT0 + 1'000'000'000U).has_value());

  const auto invalid = CallDeadline::FromGrpcTimeout("1X", kT0);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().Code(), StatusCode::kInvalidArgument);
}

TEST(CallDeadline, EarlierPrefersSoonerDeadline) {
  const auto sooner = CallDeadline::FromGrpcTimeout("1S", kT0);
  const auto later = CallDeadline::FromGrpcTimeout("2S", kT0);
  ASSERT_TRUE(sooner.has_value());
  ASSERT_TRUE(later.has_value());
  EXPECT_EQ(CallDeadline::Earlier(*sooner, *later).Deadline(), sooner->Deadline());
  EXPECT_EQ(CallDeadline::Earlier({}, *later).Deadline(), later->Deadline());
  EXPECT_FALSE(CallDeadline::Earlier({}, {}).Armed());
}

TEST(ChannelCoordinatorService, ReconnectsAndDrainsCleanly) {
  static_assert(ServiceConcept<ChannelCoordinatorService>);

  ChannelCoordinatorService channel{kTiming};
  EXPECT_EQ(channel.State(), ServiceState::kStopped);
  EXPECT_TRUE(channel.Stopped());
  EXPECT_EQ(channel.Policy(), EndpointSelectionPolicy::kPickFirst);

  ASSERT_TRUE(channel.Start().has_value());
  EXPECT_EQ(channel.State(), ServiceState::kRunning);
  EXPECT_EQ(channel.Channel(), ChannelState::kResolving);

  const auto first_ep = MakeEndpoint(1U, 50051U);
  const auto second_ep = MakeEndpoint(2U, 50052U);
  const auto first = channel.AddEndpoint(first_ep);
  const auto second = channel.AddEndpoint(second_ep);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*first, 0U);
  EXPECT_EQ(*second, 1U);
  EXPECT_EQ(channel.EndpointCount(), 2U);
  EXPECT_EQ(channel.HealthyEndpointCount(kT0), 2U);

  const auto picked0 = channel.SelectEndpoint(kT0);
  ASSERT_TRUE(picked0.has_value());
  EXPECT_EQ(*picked0, first_ep);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  EXPECT_EQ(channel.Channel(), ChannelState::kConnecting);
  const auto polled = channel.PollOnce(PollBudget{}, kT0 + 500U);
  ASSERT_TRUE(polled.has_value());
  EXPECT_EQ(polled->timers, 1U);
  EXPECT_EQ(channel.Channel(), ChannelState::kTransientFailure);
  EXPECT_EQ(channel.FailureCount(), 1U);

  const auto backoff = channel.PollOnce(PollBudget{}, kT0 + 600U);
  ASSERT_TRUE(backoff.has_value());
  EXPECT_EQ(backoff->timers, 1U);
  EXPECT_EQ(channel.Channel(), ChannelState::kResolving);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kResolutionSucceeded, kT0 + 600U));
  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kTcpConnected, kT0 + 600U));
  EXPECT_EQ(channel.Channel(), ChannelState::kReady);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kTransportFailed, kT0 + 700U));
  EXPECT_EQ(channel.Channel(), ChannelState::kTransientFailure);
  EXPECT_EQ(channel.FailureCount(), 1U);

  ASSERT_TRUE(channel.BeginShutdown(kT0 + 800U).has_value());
  EXPECT_EQ(channel.State(), ServiceState::kStopping);
  EXPECT_TRUE(channel.Stopped());
  ASSERT_TRUE(channel.Shutdown().has_value());
  EXPECT_EQ(channel.State(), ServiceState::kStopped);
  EXPECT_EQ(channel.EndpointCount(), 0U);
}

TEST(ChannelCoordinatorService, RunningShutdownMustDrainFirst) {
  ChannelCoordinatorService channel{kTiming};
  ASSERT_TRUE(channel.Start().has_value());
  const auto direct = channel.Shutdown();
  ASSERT_FALSE(direct.has_value());
  EXPECT_EQ(direct.error().Code(), StatusCode::kFailedPrecondition);
  ASSERT_TRUE(channel.BeginShutdown(kT0).has_value());
  ASSERT_TRUE(channel.PollShutdown(PollBudget{}, kT0).has_value());
  ASSERT_TRUE(channel.Shutdown().has_value());
}

TEST(ChannelCoordinatorService, PickFirstAndRoundRobinPolicies) {
  ChannelCoordinatorService channel{kTiming};
  const std::array<cares::Endpoint, 3> endpoints{
      MakeEndpoint(10U, 8080U),
      MakeEndpoint(11U, 8081U),
      MakeEndpoint(12U, 8082U),
  };
  ASSERT_TRUE(channel.SetEndpoints(endpoints).has_value());
  ASSERT_TRUE(channel.SetReady(0U, false).has_value());

  // kPickFirst always picks the lowest-index healthy & ready endpoint.
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 1U);
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 1U);
  ASSERT_TRUE(channel.SetReady(0U, true).has_value());
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 0U);

  // Switching to kRoundRobin preserves readiness and rotates across ready slots.
  ASSERT_TRUE(channel.SetReady(1U, false).has_value());
  channel.SetPolicy(EndpointSelectionPolicy::kRoundRobin);
  EXPECT_EQ(channel.Policy(), EndpointSelectionPolicy::kRoundRobin);
  EXPECT_FALSE(channel.IsReady(1U));
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 0U);
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 2U);
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 0U);
}

TEST(ChannelCoordinatorService, StickyPolicyHoldsHealthyEndpoint) {
  ChannelCoordinatorService channel{kTiming, EndpointFailoverConfig{
                                                 .failure_threshold = 1U,
                                                 .base_cooldown_ns = 500U,
                                                 .max_cooldown_ns = 2'000U,
                                                 .policy = EndpointSelectionPolicy::kSticky,
                                             }};
  const std::array<cares::Endpoint, 3> endpoints{
      MakeEndpoint(20U, 9000U),
      MakeEndpoint(21U, 9001U),
      MakeEndpoint(22U, 9002U),
  };
  ASSERT_TRUE(channel.SetEndpoints(endpoints).has_value());
  ASSERT_TRUE(channel.SetReady(0U, false).has_value());

  // Sticky latches onto slot 1 and remains pinned even after slot 0 becomes ready.
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 1U);
  ASSERT_TRUE(channel.SetReady(0U, true).has_value());
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 1U);

  // Quarantining slot 1 fails over to slot 2 and sticks there.
  ASSERT_TRUE(channel.MarkFailure(1U, kT0).has_value());
  EXPECT_FALSE(channel.IsHealthy(1U, kT0));
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 2U);
  EXPECT_EQ(channel.SelectIndex(kT0).value(), 2U);
}

TEST(ChannelCoordinatorService, LatencyAwarePrefersLowerEwmaRtt) {
  ChannelCoordinatorService channel{
      kTiming, EndpointFailoverConfig{.policy = EndpointSelectionPolicy::kLatencyAware}};
  const std::array<cares::Endpoint, 3> endpoints{
      MakeEndpoint(30U, 9100U),
      MakeEndpoint(31U, 9101U),
      MakeEndpoint(32U, 9102U),
  };
  ASSERT_TRUE(channel.SetEndpoints(endpoints).has_value());

  ASSERT_TRUE(channel.RecordSuccess(endpoints[0], 900'000U).has_value());
  ASSERT_TRUE(channel.RecordSuccess(endpoints[1], 120'000U).has_value());
  ASSERT_TRUE(channel.RecordSuccess(endpoints[2], 450'000U).has_value());
  EXPECT_LT(channel.EwmaLatencyNs(1U), channel.EwmaLatencyNs(2U));
  EXPECT_LT(channel.EwmaLatencyNs(2U), channel.EwmaLatencyNs(0U));
  EXPECT_EQ(channel.SelectEndpoint(kT0).value(), endpoints[1]);
}

TEST(ChannelCoordinatorService, RandomPolicySelectsHealthyEndpoints) {
  ChannelCoordinatorService channel{kTiming, EndpointFailoverConfig{
                                                 .policy = EndpointSelectionPolicy::kRandom,
                                                 .random_seed = 0x1234'5678'9abc'def0ULL,
                                             }};
  const std::array<cares::Endpoint, 3> endpoints{
      MakeEndpoint(40U, 9200U),
      MakeEndpoint(41U, 9201U),
      MakeEndpoint(42U, 9202U),
  };
  ASSERT_TRUE(channel.SetEndpoints(endpoints).has_value());
  ASSERT_TRUE(channel.SetReady(1U, false).has_value());

  bool saw_zero = false;
  bool saw_two = false;
  for (std::size_t step = 0U; step < 32U; ++step) {
    const auto idx = channel.SelectIndex(kT0);
    ASSERT_TRUE(idx.has_value());
    EXPECT_NE(*idx, 1U);
    saw_zero = saw_zero || (*idx == 0U);
    saw_two = saw_two || (*idx == 2U);
  }
  EXPECT_TRUE(saw_zero);
  EXPECT_TRUE(saw_two);
}

TEST(ChannelCoordinatorService, QuarantineBackoffAndRecovery) {
  ChannelCoordinatorService channel{kTiming, EndpointFailoverConfig{
                                                 .failure_threshold = 2U,
                                                 .base_cooldown_ns = 100U,
                                                 .max_cooldown_ns = 400U,
                                                 .policy = EndpointSelectionPolicy::kRoundRobin,
                                             }};
  const auto ep = MakeEndpoint(50U, 9300U);
  ASSERT_TRUE(channel.AddEndpoint(ep).has_value());

  // First failure stays below threshold; second triggers 100ns quarantine.
  ASSERT_TRUE(channel.MarkFailure(0U, kT0).has_value());
  EXPECT_TRUE(channel.IsHealthy(0U, kT0));
  ASSERT_TRUE(channel.MarkFailure(0U, kT0).has_value());
  EXPECT_EQ(channel.EndpointFailures(0U), 2U);
  EXPECT_EQ(channel.QuarantinedUntil(0U), kT0 + 100U);
  EXPECT_FALSE(channel.IsHealthy(0U, kT0 + 99U));

  const auto unavailable = channel.SelectEndpoint(kT0 + 99U);
  ASSERT_FALSE(unavailable.has_value());
  EXPECT_EQ(unavailable.error().Code(), StatusCode::kUnavailable);

  // Cooldown expires at kT0 + 100; another failure doubles cooldown to 200ns.
  EXPECT_TRUE(channel.IsHealthy(0U, kT0 + 100U));
  EXPECT_EQ(channel.SelectEndpoint(kT0 + 100U).value(), ep);
  ASSERT_TRUE(channel.MarkFailure(ep, kT0 + 100U).has_value());
  EXPECT_EQ(channel.QuarantinedUntil(0U), kT0 + 300U);

  // MarkSuccess clears quarantine and failure streak immediately.
  ASSERT_TRUE(channel.MarkSuccess(ep).has_value());
  EXPECT_EQ(channel.EndpointFailures(0U), 0U);
  EXPECT_EQ(channel.QuarantinedUntil(0U), 0U);
  EXPECT_TRUE(channel.IsHealthy(0U, kT0 + 150U));
}

TEST(ChannelCoordinatorService, PollOnceReportsArmedTimersPending) {
  ChannelCoordinatorService channel{kTiming};
  ASSERT_TRUE(channel.Start().has_value());

  const auto resolving = channel.PollOnce(PollBudget{}, kT0);
  ASSERT_TRUE(resolving.has_value());
  EXPECT_FALSE(resolving->pending);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kResolutionSucceeded, kT0));
  const auto connecting = channel.PollOnce(PollBudget{}, kT0);
  ASSERT_TRUE(connecting.has_value());
  EXPECT_EQ(connecting->timers, 0U);
  EXPECT_TRUE(connecting->pending);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kConnectFailed, kT0));
  const auto backing_off = channel.PollOnce(PollBudget{}, kT0);
  ASSERT_TRUE(backing_off.has_value());
  EXPECT_TRUE(backing_off->pending);

  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kBackoffElapsed, kT0 + 100U));
  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kResolutionSucceeded, kT0 + 100U));
  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kTcpConnected, kT0 + 100U));
  ASSERT_TRUE(channel.OnEvent(ChannelEvent::kPingSent, kT0 + 100U));
  const auto pinging = channel.PollOnce(PollBudget{}, kT0 + 100U);
  ASSERT_TRUE(pinging.has_value());
  EXPECT_EQ(channel.Channel(), ChannelState::kReady);
  EXPECT_TRUE(pinging->pending);

  ASSERT_TRUE(channel.BeginShutdown(kT0 + 100U).has_value());
  const auto stopping = channel.PollOnce(PollBudget{}, kT0 + 100U);
  ASSERT_FALSE(stopping.has_value());
  EXPECT_EQ(stopping.error().Code(), StatusCode::kFailedPrecondition);
}

TEST(ChannelCoordinatorService, KeepsQuarantineAndEwmaAcrossUpdates) {
  constexpr std::uint64_t kCooldown = 1'000U;
  constexpr std::uint64_t kFastRtt = 20'000U;
  ChannelCoordinatorService channel{kTiming, EndpointFailoverConfig{
                                                 .failure_threshold = 1U,
                                                 .base_cooldown_ns = kCooldown,
                                                 .max_cooldown_ns = kCooldown,
                                                 .policy = EndpointSelectionPolicy::kLatencyAware,
                                             }};
  const auto ep0 = MakeEndpoint(1U, 4400U);
  const auto ep1 = MakeEndpoint(2U, 4401U);
  const auto ep2 = MakeEndpoint(3U, 4402U);
  ASSERT_TRUE(channel.AddEndpoint(ep0).has_value());
  ASSERT_TRUE(channel.AddEndpoint(ep1).has_value());
  ASSERT_TRUE(channel.MarkFailure(0U, kT0).has_value());
  const auto quarantine = channel.QuarantinedUntil(0U);
  const auto failures = channel.EndpointFailures(0U);
  ASSERT_GT(quarantine, kT0);
  ASSERT_TRUE(channel.RecordSuccess(1U, kFastRtt).has_value());
  const auto ewma = channel.EwmaLatencyNs(1U);
  ASSERT_EQ(ewma, kFastRtt);

  ASSERT_EQ(channel.AddEndpoint(ep2).value(), 2U);
  EXPECT_EQ(channel.QuarantinedUntil(0U), quarantine);
  EXPECT_EQ(channel.EwmaLatencyNs(1U), ewma);
  EXPECT_FALSE(channel.IsHealthy(0U, quarantine - 1U));

  channel.SetPolicy(EndpointSelectionPolicy::kRoundRobin);
  EXPECT_EQ(channel.Policy(), EndpointSelectionPolicy::kRoundRobin);
  auto config = channel.FailoverConfig();
  config.policy = EndpointSelectionPolicy::kLatencyAware;
  channel.SetFailoverConfig(config);
  EXPECT_EQ(channel.Policy(), EndpointSelectionPolicy::kLatencyAware);
  EXPECT_EQ(channel.QuarantinedUntil(0U), quarantine);
  EXPECT_EQ(channel.EndpointFailures(0U), failures);
  EXPECT_FALSE(channel.IsHealthy(0U, quarantine - 1U));
  EXPECT_TRUE(channel.IsHealthy(0U, quarantine));
  EXPECT_EQ(channel.EwmaLatencyNs(1U), ewma);

  ASSERT_TRUE(channel.RecordSuccess(2U, kFastRtt * 10U).has_value());
  EXPECT_EQ(channel.SelectEndpoint(quarantine - 1U).value(), ep1);
  EXPECT_EQ(channel.EwmaLatencyNs(1U), ewma);
}

TEST(ChannelCoordinatorService, RejectsInvalidAndExcessEndpoints) {
  ChannelCoordinatorService channel{kTiming};
  const cares::Endpoint invalid{};
  const auto bad = channel.AddEndpoint(invalid);
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error().Code(), StatusCode::kInvalidArgument);

  for (std::size_t i = 0U; i < ChannelCoordinatorService::kMaxEndpoints; ++i) {
    ASSERT_TRUE(
        channel.AddEndpoint(MakeEndpoint(static_cast<std::uint8_t>(i + 1U), 7000U)).has_value());
  }
  const auto full = channel.AddEndpoint(MakeEndpoint(99U, 7099U));
  ASSERT_FALSE(full.has_value());
  EXPECT_EQ(full.error().Code(), StatusCode::kResourceExhausted);
}

TEST(ChannelCoordinatorService, ReconcileEndpointsPreservesHealthCursorAndDeduplicates) {
  ChannelCoordinatorService channel{kTiming, EndpointFailoverConfig{
                                                 .failure_threshold = 1U,
                                                 .base_cooldown_ns = 1'000U,
                                                 .max_cooldown_ns = 4'000U,
                                                 .policy = EndpointSelectionPolicy::kRoundRobin,
                                             }};
  const auto ep0 = MakeEndpoint(60U, 9400U);
  const auto ep1 = MakeEndpoint(61U, 9401U);
  const auto ep2 = MakeEndpoint(62U, 9402U);
  const auto ep3 = MakeEndpoint(63U, 9403U);
  const std::array<cares::Endpoint, 3> initial{ep0, ep1, ep2};
  ASSERT_TRUE(channel.SetEndpoints(initial).has_value());

  // Quarantine ep0, record EWMA RTT on ep1, mark ep2 unready, and latch cursor onto ep1.
  ASSERT_TRUE(channel.MarkFailure(ep0, kT0).has_value());
  const auto quarantine = channel.QuarantinedUntil(0U);
  ASSERT_EQ(quarantine, kT0 + 1'000U);
  ASSERT_TRUE(channel.RecordSuccess(ep1, 45'000U).has_value());
  ASSERT_TRUE(channel.SetReady(2U, false).has_value());
  EXPECT_EQ(channel.SelectEndpoint(kT0).value(), ep1);

  // Reconcile with reordered + duplicated + newly added ep3: [ep2, ep1, ep0, ep1, ep3].
  const std::array<cares::Endpoint, 5> refreshed{ep2, ep1, ep0, ep1, ep3};
  ASSERT_TRUE(channel.ReconcileEndpoints(refreshed).has_value());
  ASSERT_EQ(channel.EndpointCount(), 4U);
  EXPECT_EQ(channel.EndpointAt(0U), ep2);
  EXPECT_EQ(channel.EndpointAt(1U), ep1);
  EXPECT_EQ(channel.EndpointAt(2U), ep0);
  EXPECT_EQ(channel.EndpointAt(3U), ep3);

  // Surviving endpoints keep readiness, EWMA RTT, failure count, and quarantine; ep3 starts ready.
  EXPECT_FALSE(channel.IsReady(0U));
  EXPECT_TRUE(channel.IsReady(1U));
  EXPECT_EQ(channel.EwmaLatencyNs(1U), 45'000U);
  EXPECT_EQ(channel.EndpointFailures(2U), 1U);
  EXPECT_EQ(channel.QuarantinedUntil(2U), quarantine);
  EXPECT_FALSE(channel.IsHealthy(2U, quarantine - 1U));
  EXPECT_TRUE(channel.IsReady(3U));
  EXPECT_TRUE(channel.IsHealthy(3U, kT0));

  // Round-robin next-to-check cursor followed ep2 (now slot 0, unready), so selection visits ep1
  // (slot 1) then ep3 (slot 3) while skipping unready ep2 and quarantined ep0.
  EXPECT_EQ(channel.SelectEndpoint(quarantine - 1U).value(), ep1);
  EXPECT_EQ(channel.SelectEndpoint(quarantine - 1U).value(), ep3);

  // Under kSticky, SelectEndpoint latches onto ep1 (slot 1) and follows it to slot 2 even when
  // healthy ep3 moves to slot 0.
  channel.SetPolicy(EndpointSelectionPolicy::kSticky);
  EXPECT_EQ(channel.SelectEndpoint(quarantine - 1U).value(), ep1);
  const std::array<cares::Endpoint, 4> sticky_reordered{ep3, ep2, ep1, ep0};
  ASSERT_TRUE(channel.ReconcileEndpoints(sticky_reordered).has_value());
  EXPECT_EQ(channel.SelectEndpoint(quarantine - 1U).value(), ep1);
}

}  // namespace
}  // namespace grpcxx
