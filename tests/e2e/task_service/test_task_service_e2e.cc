#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <ranges>

#include "grpcxx/core/timeout.h"
#include "task_gateway_service.h"
#include "task_transport.h"

namespace {
using grpcxx::StatusCode;
constexpr grpcxx::PollBudget kBudget{};
task_gateway::SubmitTask MakeTask(std::uint64_t id) { return {id, 101, 5, {}}; }

template <typename Predicate>
void Until(task_gateway::TaskLoop& loop, std::uint64_t& now, Predicate done,
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
void Stop(task_gateway::TaskLoop& loop, std::uint64_t& now) {
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

TEST(TaskServiceCanonical, DeferredCapabilitySurvivesDispatch) {
  static_assert(grpcxx::ServiceConcept<task_gateway::TaskLoop>);
  static_assert(grpcxx::ServiceConcept<task_gateway::LoopbackTransport>);
  task_gateway::TaskLoop loop{{.validation_steps = 4}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(42), 1000));
  std::uint64_t now = 1;
  ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_EQ(loop.PendingCount(), 1U);
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  EXPECT_EQ(loop.Reports()[0].task_id, 42U);
  EXPECT_EQ(loop.Reports()[0].weight, 5);
  EXPECT_EQ(loop.PendingCount(), 0U);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, CancelSuppressesExecution) {
  task_gateway::TaskLoop loop{{.validation_steps = 8}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(7), 1000));
  const auto cancelled = loop.CancelTask(task_gateway::CancelTask{7});
  ASSERT_TRUE(cancelled);
  EXPECT_TRUE(cancelled->message.canceled);
  EXPECT_EQ(loop.PendingCount(), 0U);
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
  EXPECT_EQ(loop.Failures().front(), StatusCode::kCancelled);
  for (int tick = 0; tick < 12; ++tick) ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_TRUE(loop.Reports().empty());
  auto repeated = loop.CancelTask(task_gateway::CancelTask{7});
  ASSERT_FALSE(repeated);
  EXPECT_EQ(repeated.error().Code(), StatusCode::kNotFound);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, ExactDeadlineExpiry) {
  for (const std::string_view timeout : {"0n", "10n"}) {
    task_gateway::TaskLoop loop;
    ASSERT_TRUE(loop.Start());
    ASSERT_TRUE(loop.SubmitWithTimeout(MakeTask(1), timeout));
    std::uint64_t now = 10;
    ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
    EXPECT_TRUE(loop.DeadlineExceededSeen());
    EXPECT_TRUE(loop.Reports().empty());
    ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
  }
}
TEST(TaskServiceCanonical, TimeoutCodecRejectsMalformedInput) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  for (const std::string_view invalid : {"", "1x", "-1n", "123456789n", "99999999H"}) {
    auto result = loop.SubmitWithTimeout(MakeTask(1), invalid);
    ASSERT_FALSE(result) << invalid;
    EXPECT_EQ(result.error().Code(), StatusCode::kInvalidArgument);
  }
  ASSERT_TRUE(loop.PollOnce({}, std::numeric_limits<std::uint64_t>::max() - 1));
  EXPECT_FALSE(loop.SubmitWithTimeout(MakeTask(1), "2n"));
  auto formatted = grpcxx::GrpcTimeout::Parse("2m");
  ASSERT_TRUE(formatted);
  EXPECT_EQ(*formatted, 2'000'000U);
}
TEST(TaskServiceCanonical, AdmissionPreservesAcceptedTasks) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  for (std::uint64_t id = 1; id <= 8; ++id) ASSERT_TRUE(loop.SubmitTask(MakeTask(id), 1000));
  auto excess = loop.SubmitTask(MakeTask(9), 1000);
  ASSERT_FALSE(excess);
  EXPECT_EQ(excess.error().Code(), StatusCode::kResourceExhausted);
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 8; }));
  EXPECT_TRUE(loop.Failures().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, SingleBudgetReportsPendingDeadline) {
  task_gateway::TaskLoop loop{{.validation_steps = 2}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(10), 1000));
  ASSERT_TRUE(loop.SubmitTask(MakeTask(11), 1000));
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
TEST(TaskServiceCanonical, StreamDrainSignalsWriteReady) {
  task_gateway::TaskLoop loop{{.stream_window = 1}};
  ASSERT_TRUE(loop.Start());
  EXPECT_FALSE(loop.WriteReadySeen());
  ASSERT_TRUE(loop.SubscribeRecords(12));
  EXPECT_TRUE(loop.Events().empty());
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Events().size() == 12; }));
  EXPECT_TRUE(loop.HighWaterSeen());
  EXPECT_TRUE(loop.WriteReadySeen());
  for (std::size_t index = 0; index < 12; ++index)
    EXPECT_EQ(loop.Events()[index].sequence, index + 1);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, BidiPressurePreservesHalfClose) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenTaskStream());
  auto first = loop.SendTask({1, 3});
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, grpcxx::WriteStatus::kQueued);
  auto second = loop.SendTask({2, 4});
  ASSERT_TRUE(second);
  EXPECT_EQ(*second, grpcxx::WriteStatus::kHighWaterMarkReached);
  auto rejected = loop.SendTask({3, 5});
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  ASSERT_TRUE(loop.DrainStreamWindow());
  EXPECT_TRUE(loop.WriteReadySeen());
  ASSERT_TRUE(loop.SendTask({3, 5}));
  ASSERT_TRUE(loop.HalfCloseTaskStream());
  EXPECT_FALSE(loop.SendTask({4, 1}));
  EXPECT_FALSE(loop.HalfCloseTaskStream());
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 3; }));
  for (std::size_t index = 0; index < 3; ++index)
    EXPECT_EQ(loop.Reports()[index].task_id, index + 1);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, BidiCancelDropsCancelledWork) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenTaskStream());
  ASSERT_TRUE(loop.SendTask({1, 1}));
  ASSERT_TRUE(loop.CancelTaskStream());
  EXPECT_FALSE(loop.SendTask({2, 1}));
  std::uint64_t now = 1;
  for (int step = 0; step < 8; ++step) ASSERT_TRUE(loop.PollOnce({}, now++));
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, FramesDispatchOnce) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  auto frame = grpcxx::AlignedMessageBuf::Allocate(sizeof(task_gateway::SubmitTask));
  ASSERT_TRUE(frame);
  const auto task = MakeTask(8);
  std::memcpy(frame->Payload().data(), &task, sizeof(task));
  ASSERT_TRUE(loop.PushRawFrame(frame->WireBytes().first(3)));
  ASSERT_TRUE(loop.PushRawFrame(frame->WireBytes().subspan(3)));
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  EXPECT_EQ(loop.Reports().front().task_id, 8U);
  const std::array<std::byte, 5> oversized{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{4},
                                           std::byte{1}};
  auto rejected = loop.PushRawFrame(oversized);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_TRUE(loop.ResourceExhaustedSeen());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, ShutdownGatesAdmissionAndRestarts) {
  task_gateway::TaskLoop loop{{.validation_steps = 5}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(1), 1000));
  ASSERT_TRUE(loop.BeginShutdown());
  EXPECT_FALSE(loop.SubmitTask(MakeTask(2), 1000));
  EXPECT_FALSE(loop.CancelTask(task_gateway::CancelTask{1}));
  EXPECT_FALSE(loop.SubscribeRecords(1));
  EXPECT_FALSE(loop.OpenTaskStream());
  EXPECT_FALSE(loop.SendTask({3, 1}));
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
  ASSERT_TRUE(loop.SubmitTask(MakeTask(2), 1000));
  now = 1;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == 1; }));
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, FailedStartRollsBackLifecycle) {
  task_gateway::TaskLoop loop{{.stream_window = 0}};
  EXPECT_FALSE(loop.Start());
  EXPECT_TRUE(loop.Stopped());
  EXPECT_FALSE(loop.SubmitTask(MakeTask(1), 10));
  EXPECT_FALSE(loop.PollOnce({}, 1));
}
TEST(TaskServiceCanonical, ShutdownWaitsForControlDrain) {
  task_gateway::TaskLoop loop{{.validation_steps = 10}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.OpenTaskStream());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(1), 1));
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
TEST(TaskServiceCanonical, DuplicatePreservesResponder) {
  task_gateway::TaskLoop loop{{.validation_steps = 4}};
  ASSERT_TRUE(loop.Start());
  ASSERT_TRUE(loop.SubmitTask(MakeTask(1), 1000));
  ASSERT_TRUE(loop.SubmitTask(MakeTask(1), 1000));
  std::uint64_t now = 1;
  ASSERT_NO_FATAL_FAILURE(
      Until(loop, now, [&] { return loop.Reports().size() == 1 && loop.Failures().size() == 1; }));
  EXPECT_EQ(loop.Failures().front(), StatusCode::kAlreadyExists);
  EXPECT_EQ(loop.Reports().front().task_id, 1U);
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}
TEST(TaskServiceCanonical, ObservationCapacityPreservesReports) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  std::uint64_t now = 1;
  for (std::uint64_t id = 0; id < 64; ++id) {
    ASSERT_TRUE(loop.SubmitTask(MakeTask(id), now + 1000));
    ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return loop.Reports().size() == id + 1; }));
  }
  auto excess = loop.SubmitTask(MakeTask(65), now + 1000);
  ASSERT_FALSE(excess);
  EXPECT_EQ(excess.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(loop.Reports().size(), 64U);
  ASSERT_TRUE(loop.OpenTaskStream());
  EXPECT_FALSE(loop.SendTask({66, 1}));
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}

TEST(TaskServiceCanonical, TimeoutRoundingPreservesDeadline) {
  task_gateway::TaskLoop loop;
  ASSERT_TRUE(loop.Start());
  constexpr std::uint64_t kDeadline = 100'000'001;
  ASSERT_TRUE(loop.SubmitTask(MakeTask(1), kDeadline));
  std::uint64_t now = kDeadline;
  ASSERT_NO_FATAL_FAILURE(Until(loop, now, [&] { return !loop.Failures().empty(); }));
  EXPECT_TRUE(loop.DeadlineExceededSeen());
  EXPECT_TRUE(loop.Reports().empty());
  ASSERT_NO_FATAL_FAILURE(Stop(loop, now));
}

}  // namespace
