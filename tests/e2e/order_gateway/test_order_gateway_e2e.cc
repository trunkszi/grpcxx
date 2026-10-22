#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <ranges>

#include "grpcxx/core/timeout.h"
#include "order_gateway_service.h"
#include "order_transport.h"

namespace {
using grpcxx::StatusCode;
constexpr grpcxx::PollBudget kBudget{};
order_gateway::NewOrderSingle Order(std::uint64_t id) { return {id, 101, 5, {}}; }

template <typename Predicate>
void Until(order_gateway::TradingLoop& loop, std::uint64_t& now, Predicate done,
           grpcxx::PollBudget budget = {}) {
  for (int tick = 0; tick < 128; ++tick) {
    auto result = loop.PollOnce(budget, now++);
    ASSERT_TRUE(result) << static_cast<int>(result.error().Code());
    EXPECT_LE(result->commands, budget.commands);
    EXPECT_LE(result->frames, budget.frames);
    EXPECT_LE(result->completions, budget.completions);
    EXPECT_LE(result->socket_events, budget.socket_events);
    EXPECT_LE(result->timers, budget.timers);
    if (done()) return;
  }
  FAIL() << "bounded caller-driven progress did not complete";
}
void Stop(order_gateway::TradingLoop& loop, std::uint64_t& now) {
  ASSERT_TRUE(loop.BeginShutdown(now));
  bool quiescent = false;
  for (int step = 0; step < 128 && !quiescent; ++step) {
    auto drained = loop.PollShutdown(kBudget, now++);
    ASSERT_TRUE(drained);
    quiescent = *drained;
  }
  ASSERT_TRUE(quiescent);
  ASSERT_TRUE(loop.Shutdown());
  EXPECT_TRUE(loop.Stopped());
}

TEST(OrderGatewayCanonical, ReflectedMethodsAndDeferredCapabilitySurviveDispatchReturn) {
  static_assert(grpcxx::ServiceConcept<order_gateway::TradingLoop>);
  static_assert(grpcxx::ServiceConcept<order_gateway::LoopbackTransport>);
  order_gateway::TradingLoop loop{{.risk_check_ticks = 4}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.NewOrderSingle(Order(42), 1000));
  std::uint64_t now = 1;
  ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_EQ(loop.PendingCount(), 1U);
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  EXPECT_EQ(loop.Reports()[0].client_order_id, 42U);
  EXPECT_EQ(loop.Reports()[0].quantity, 5);
  EXPECT_EQ(loop.PendingCount(), 0U);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, CancelPendingOrderDeliversCancelledAndNeverAnExecution) {
  order_gateway::TradingLoop loop{{.risk_check_ticks = 8}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.NewOrderSingle(Order(7), 1000));
  const auto cancelled = loop.CancelOrder(order_gateway::CancelOrder{7});
  ASSERT_TRUE(cancelled);
  EXPECT_TRUE(cancelled->message.canceled);
  EXPECT_EQ(loop.PendingCount(), 0U);
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
  EXPECT_EQ(loop.Failures().front(), StatusCode::kCancelled);
  for (int tick = 0; tick < 12; ++tick) ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_TRUE(loop.Reports().empty());
  auto repeated = loop.CancelOrder(order_gateway::CancelOrder{7});
  ASSERT_FALSE(repeated);
  EXPECT_EQ(repeated.error().Code(), StatusCode::kNotFound);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, ExactDeadlineBoundaryAndZeroTimeoutExpire) {
  for (const std::string_view timeout : {"0n", "10n"}) {
    order_gateway::TradingLoop loop;
    ASSERT_TRUE(loop.Start());
    ASSERT_TRUE(loop.NewOrderWithTimeout(Order(1), timeout));
    std::uint64_t now = 10;
    ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
    EXPECT_TRUE(loop.DeadlineExceededSeen());
    EXPECT_TRUE(loop.Reports().empty());
    ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
  }
}
TEST(OrderGatewayCanonical, TimeoutCodecMalformedOverflowAndNoMutation) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  for (const std::string_view invalid : {"", "1x", "-1n", "123456789n", "99999999H"}) {
    auto result = loop.NewOrderWithTimeout(Order(1), invalid);
    ASSERT_FALSE(result) << invalid;
    EXPECT_EQ(result.error().Code(), StatusCode::kInvalidArgument);
  }
  ASSERT_TRUE(loop.PollOnce({}, std::numeric_limits<std::uint64_t>::max() - 1));
  EXPECT_FALSE(loop.NewOrderWithTimeout(Order(1), "2n"));
  auto formatted = grpcxx::GrpcTimeout::Parse("2m");
  ASSERT_TRUE(formatted);
  EXPECT_EQ(*formatted, 2'000'000U);
}
TEST(OrderGatewayCanonical, AdmissionBoundIsExplicitAndAcceptedOrdersAreNotLost) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  for (std::uint64_t id = 1; id <= 8; ++id) ASSERT_TRUE(loop.NewOrderSingle(Order(id), 1000));
  auto excess = loop.NewOrderSingle(Order(9), 1000);
  ASSERT_FALSE(excess);
  EXPECT_EQ(excess.error().Code(), StatusCode::kResourceExhausted);
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 8; }));
  EXPECT_TRUE(loop.Failures().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, BudgetOneProgressReportsPendingAndDeadline) {
  order_gateway::TradingLoop loop{{.risk_check_ticks = 2}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.NewOrderSingle(Order(10), 1000));
  ASSERT_TRUE(loop.NewOrderSingle(Order(11), 1000));
  const grpcxx::PollBudget small{1, 1, 1, 1, 1};
  std::uint64_t now = 1;
  auto polled = loop.PollOnce(small, now++);
  ASSERT_TRUE(polled);
  EXPECT_TRUE(polled->pending);
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 2; }, small));
  EXPECT_FALSE(loop.PollOnce({0, 1, 1, 1, 1}, now));
  EXPECT_FALSE(loop.PollOnce({}, now - 2));
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, ServerStreamPressureAndWriteReadyAreDrivenByActualDrain) {
  order_gateway::TradingLoop loop{{.stream_window = 1}};
  ASSERT_TRUE(loop.Start());
  EXPECT_FALSE(loop.WriteReadySeen());
  ASSERT_TRUE(loop.SubscribeMarketData(12));
  EXPECT_TRUE(loop.Ticks().empty());
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Ticks().size() == 12; }));
  EXPECT_TRUE(loop.HighWaterSeen());
  EXPECT_TRUE(loop.WriteReadySeen());
  for (std::size_t index = 0; index < 12; ++index)
    EXPECT_EQ(loop.Ticks()[index].sequence, index + 1);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, BidiPressureRejectsWithoutDropAndHalfClosePersists) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenOrderStream());
  auto first = loop.SendOrderEntry({1, 3});
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, grpcxx::WriteStatus::kQueued);
  auto second = loop.SendOrderEntry({2, 4});
  ASSERT_TRUE(second);
  EXPECT_EQ(*second, grpcxx::WriteStatus::kHighWaterMarkReached);
  auto rejected = loop.SendOrderEntry({3, 5});
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  ASSERT_TRUE(loop.DrainStreamWindow());
  EXPECT_TRUE(loop.WriteReadySeen());
  ASSERT_TRUE(loop.SendOrderEntry({3, 5}));
  ASSERT_TRUE(loop.HalfCloseOrderStream());
  EXPECT_FALSE(loop.SendOrderEntry({4, 1}));
  EXPECT_FALSE(loop.HalfCloseOrderStream());
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 3; }));
  for (std::size_t index = 0; index < 3; ++index)
    EXPECT_EQ(loop.Reports()[index].client_order_id, index + 1);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, CancelBidiPersistsAndDropsOnlyExplicitlyCancelledWork) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenOrderStream());
  ASSERT_TRUE(loop.SendOrderEntry({1, 1}));
  ASSERT_TRUE(loop.CancelOrderStream());
  EXPECT_FALSE(loop.SendOrderEntry({2, 1}));
  std::uint64_t now = 1;
  for (int step = 0; step < 8; ++step) ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, ParsedFramesDispatchExactlyOnceAndOversizeFailsAtHeader) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  auto frame = grpcxx::AlignedMessageBuf::Allocate(sizeof(order_gateway::NewOrderSingle));
  ASSERT_TRUE(frame);
  const auto order = Order(8);
  std::memcpy(frame->Payload().data(), &order, sizeof(order));
  ASSERT_TRUE(loop.PushRawFrame(frame->WireBytes().first(3)));
  ASSERT_TRUE(loop.PushRawFrame(frame->WireBytes().subspan(3)));
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  EXPECT_EQ(loop.Reports().front().client_order_id, 8U);
  const std::array<std::byte, 5> oversized{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{4},
                                           std::byte{1}};
  auto rejected = loop.PushRawFrame(oversized);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_TRUE(loop.ResourceExhaustedSeen());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, ShutdownGatesEveryAdmissionDrainsAndRestarts) {
  order_gateway::TradingLoop loop{{.risk_check_ticks = 5}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.NewOrderSingle(Order(1), 1000));
  ASSERT_TRUE(loop.BeginShutdown());
  EXPECT_FALSE(loop.NewOrderSingle(Order(2), 1000));
  EXPECT_FALSE(loop.CancelOrder(order_gateway::CancelOrder{1}));
  EXPECT_FALSE(loop.SubscribeMarketData(1));
  EXPECT_FALSE(loop.OpenOrderStream());
  EXPECT_FALSE(loop.SendOrderEntry({3, 1}));
  EXPECT_FALSE(loop.PushRawFrame({}));
  EXPECT_FALSE(loop.Shutdown());
  bool done = false;
  std::uint64_t now = 1;
  for (int step = 0; step < 32 && !done; ++step) {
    auto drained = loop.PollShutdown({}, now++);
    ASSERT_TRUE(drained);
    done = *drained;
  }
  ASSERT_TRUE(done);
  EXPECT_TRUE(loop.ShutdownDrainedResponder());
  ASSERT_EQ(loop.Reports().size(), 1U);
  ASSERT_TRUE(loop.Shutdown());
  ASSERT_TRUE(loop.Start());
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_TRUE(loop.NewOrderSingle(Order(2), 1000));
  now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical, FailedStartHasNoPartiallyStartedLifecycle) {
  order_gateway::TradingLoop loop{{.stream_window = 0}};
  EXPECT_FALSE(loop.Start());
  EXPECT_TRUE(loop.Stopped());
  EXPECT_FALSE(loop.NewOrderSingle(Order(1), 10));
  EXPECT_FALSE(loop.PollOnce({}, 1));
}
TEST(OrderGatewayCanonical,
     ShutdownWaitsForEmptyStreamControlFramesAndExpiredRiskIsNotDrainedSuccess) {
  order_gateway::TradingLoop loop{{.risk_check_ticks = 10}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenOrderStream());
  ASSERT_TRUE(loop.NewOrderSingle(Order(1), 1));
  ASSERT_TRUE(loop.BeginShutdown());
  EXPECT_FALSE(loop.Shutdown());
  std::uint64_t now = 1;
  bool done = false;
  for (int step = 0; step < 64 && !done; ++step) {
    auto drained = loop.PollShutdown({}, now++);
    ASSERT_TRUE(drained);
    done = *drained;
  }
  ASSERT_TRUE(done);
  EXPECT_TRUE(loop.DeadlineExceededSeen());
  EXPECT_FALSE(loop.ShutdownDrainedResponder());
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_TRUE(loop.Shutdown());
}
TEST(OrderGatewayCanonical, DuplicateIdDoesNotReplaceExistingResponder) {
  order_gateway::TradingLoop loop{{.risk_check_ticks = 4}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.NewOrderSingle(Order(1), 1000));
  ASSERT_TRUE(loop.NewOrderSingle(Order(1), 1000));
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(
      Until(loop, now, [&] { return loop.Reports().size() == 1 && loop.Failures().size() == 1; }));
  EXPECT_EQ(loop.Failures().front(), StatusCode::kAlreadyExists);
  EXPECT_EQ(loop.Reports().front().client_order_id, 1U);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(OrderGatewayCanonical,
     ObservationCapacityRejectsAdditionalWorkWithoutCorruptingAcceptedReports) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  std::uint64_t now = 1;
  for (std::uint64_t id = 0; id < 64; ++id) {
    ASSERT_TRUE(loop.NewOrderSingle(Order(id), now + 1000));
    ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == id + 1; }));
  }
  auto excess = loop.NewOrderSingle(Order(65), now + 1000);
  ASSERT_FALSE(excess);
  EXPECT_EQ(excess.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(loop.Reports().size(), 64U);
  ASSERT_TRUE(loop.OpenOrderStream());
  EXPECT_FALSE(loop.SendOrderEntry({66, 1}));
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}

TEST(OrderGatewayCanonical, TimeoutFormattingRoundupDoesNotExtendAbsoluteDeadline) {
  order_gateway::TradingLoop loop;
  ASSERT_TRUE(loop.Start());
  constexpr std::uint64_t deadline = 100'000'001;
  ASSERT_TRUE(loop.NewOrderSingle(Order(1), deadline));
  std::uint64_t now = deadline;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
  EXPECT_TRUE(loop.DeadlineExceededSeen());
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}

}  // namespace
