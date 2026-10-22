#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <optional>

#include "grpcxx/channel/call_deadline.h"
#include "grpcxx/channel/channel_coordinator_service.h"

// vcpkg gtest was built against glibc >= 2.38; this sysroot lacks the ISO C23
// strtol redirects that gtest references.
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

TEST(CallDeadline, ParsesGrpcTimeoutAndExpires) {
  auto parsed = CallDeadline::FromGrpcTimeout("1S", kT0);
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

TEST(ChannelCoordinatorService, ReconnectsAndSelectsEndpoints) {
  static_assert(ServiceConcept<ChannelCoordinatorService>);

  ChannelCoordinatorService channel{kTiming};
  EXPECT_EQ(channel.State(), ServiceState::kStopped);
  EXPECT_TRUE(channel.Stopped());

  ASSERT_TRUE(channel.Start().has_value());
  EXPECT_EQ(channel.State(), ServiceState::kRunning);
  EXPECT_EQ(channel.Channel(), ChannelState::kResolving);

  const auto first_endpoint = channel.AddEndpoint({.healthy = true, .ready = true});
  ASSERT_TRUE(first_endpoint.has_value());
  EXPECT_EQ(*first_endpoint, 0U);
  const auto second_endpoint = channel.AddEndpoint({.healthy = true, .ready = true});
  ASSERT_TRUE(second_endpoint.has_value());
  EXPECT_EQ(*second_endpoint, 1U);
  EXPECT_EQ(channel.EndpointCount(), 2U);
  EXPECT_EQ(channel.PickEndpoint(), std::optional<std::size_t>{0});
  EXPECT_EQ(channel.PickEndpoint(), std::optional<std::size_t>{0});

  channel.SetPolicy(LoadBalancingPolicy::kRoundRobin);
  EXPECT_EQ(channel.PickEndpoint(), std::optional<std::size_t>{0});
  EXPECT_EQ(channel.PickEndpoint(), std::optional<std::size_t>{1});
  EXPECT_EQ(channel.PickEndpoint(), std::optional<std::size_t>{0});

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

  const auto drain = channel.BeginShutdown(kT0 + 800U);
  ASSERT_TRUE(drain.has_value());
  EXPECT_EQ(channel.State(), ServiceState::kStopping);
  EXPECT_TRUE(channel.Stopped());
  const auto shutdown = channel.Shutdown();
  ASSERT_TRUE(shutdown.has_value());
  EXPECT_EQ(channel.State(), ServiceState::kStopped);
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

}  // namespace
}  // namespace grpcxx
