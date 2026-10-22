#include <grpcxx/uring/proactor_service.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <memory>

namespace {
using namespace grpcxx;

/** Socket creation and descriptor release are test setup, outside caller-driven ticks. */
class ProactorServiceIntegration : public testing::Test {
 protected:
  ProactorService proactor;
  std::array<int, 2> sockets{-1, -1};

  void SetUp() override {
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()),
              0);
    ASSERT_TRUE(proactor.Start().has_value()) << "A kernel with permitted io_uring is required";
  }

  void TearDown() override {
    const auto begin = proactor.BeginShutdown(UINT64_MAX);
    if (begin) {
      bool drained = false;
      for (unsigned i = 0; i < 100000 && !drained; ++i) {
        const auto result = proactor.PollShutdown(PollBudget{}, MonotonicTimestamp{});
        if (!result) break;
        drained = proactor.Stopped();
      }
      EXPECT_TRUE(drained);
      if (drained) {
        EXPECT_TRUE(proactor.Shutdown().has_value());
      }
    }
    for (const auto fd : sockets) {
      if (fd >= 0) {
        EXPECT_EQ(::close(fd), 0);
      }
    }
  }
};

TEST_F(ProactorServiceIntegration, KernelSendReceiveHonorsCompletionBudget) {
  constexpr std::array kOutgoing{std::byte{0x12}, std::byte{0}, std::byte{0x80}};
  std::array<std::byte, kOutgoing.size()> incoming{};
  unsigned callbacks = 0;
  int received = -1;
  int sent = -1;
  ASSERT_TRUE(proactor
                  .SubmitRecv(sockets[0], incoming,
                              [&callbacks, &received](int result) noexcept {
                                ++callbacks;
                                received = result;
                              })
                  .has_value());
  ASSERT_TRUE(proactor
                  .SubmitSend(sockets[1], kOutgoing,
                              [&callbacks, &sent](int result) noexcept {
                                ++callbacks;
                                sent = result;
                              })
                  .has_value());

  const PollBudget zero{.completions = 0, .frames = 0, .timers = 0};
  const auto rejections = proactor.RejectedTransitions();
  EXPECT_FALSE(proactor.PollOnce(zero, MonotonicTimestamp{}).has_value());
  EXPECT_EQ(proactor.RejectedTransitions(), rejections);
  EXPECT_EQ(callbacks, 0U);
  const PollBudget single{.completions = 1};
  for (unsigned i = 0; i < 100000 && callbacks != 2; ++i) {
    const auto before = callbacks;
    ASSERT_TRUE(proactor.PollOnce(single, MonotonicTimestamp{}).has_value());
    EXPECT_LE(callbacks - before, 1U);
  }
  ASSERT_EQ(callbacks, 2U);
  EXPECT_EQ(sent, static_cast<int>(kOutgoing.size()));
  EXPECT_EQ(received, static_cast<int>(kOutgoing.size()));
  EXPECT_EQ(incoming, kOutgoing);
}

TEST_F(ProactorServiceIntegration, ShutdownCancelsOutstandingKernelReceiveExactlyOnce) {
  std::array<std::byte, 8> incoming{};
  unsigned callbacks = 0;
  int result = 0;
  ASSERT_TRUE(proactor
                  .SubmitRecv(sockets[0], incoming,
                              [&callbacks, &result](int value) noexcept {
                                ++callbacks;
                                result = value;
                              })
                  .has_value());
  ASSERT_TRUE(proactor.PollOnce(PollBudget{}, MonotonicTimestamp{}).has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  EXPECT_FALSE(proactor.SubmitRecv(sockets[0], incoming, [](int) noexcept {}).has_value());
  bool drained = false;
  for (unsigned i = 0; i < 100000 && !drained; ++i) {
    const auto shutdown = proactor.PollShutdown(PollBudget{}, MonotonicTimestamp{});
    ASSERT_TRUE(shutdown.has_value());
    drained = proactor.Stopped();
  }
  ASSERT_TRUE(drained);
  EXPECT_EQ(callbacks, 1U);
  EXPECT_EQ(result, -ECANCELED);
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

TEST_F(ProactorServiceIntegration, TimersUseCallerTimestampAndRespectBudget) {
  unsigned callbacks = 0;
  int errors = 0;
  for (unsigned i = 0; i < 3; ++i) {
    ASSERT_TRUE(proactor
                    .ScheduleTimer(100,
                                   [&callbacks, &errors](int result) noexcept {
                                     ++callbacks;
                                     if (result != 0) ++errors;
                                   })
                    .has_value());
  }
  const PollBudget budget{.timers = 1};
  ASSERT_TRUE(proactor.PollOnce(budget, 99).has_value());
  EXPECT_EQ(callbacks, 0U);
  for (unsigned expected = 1; expected <= 3; ++expected) {
    ASSERT_TRUE(proactor.PollOnce(budget, 100).has_value());
    EXPECT_EQ(callbacks, expected);
  }
  ASSERT_TRUE(proactor.PollOnce(budget, 101).has_value());
  EXPECT_EQ(callbacks, 3U);
  EXPECT_EQ(errors, 0);
}

TEST_F(ProactorServiceIntegration, LinkedReceiveTimeoutCompletesAndRejectsStaleCancellation) {
  std::array<std::byte, 1> incoming{};
  unsigned callbacks = 0;
  int result = 0;
  const auto operation = proactor.SubmitRecv(
      sockets[0], incoming,
      [&callbacks, &result](int value) noexcept {
        ++callbacks;
        result = value;
      },
      std::chrono::nanoseconds{1});
  ASSERT_TRUE(operation.has_value());
  for (unsigned i = 0; i < 100000 && callbacks == 0; ++i)
    ASSERT_TRUE(proactor.PollOnce(PollBudget{}, 0).has_value());
  ASSERT_EQ(callbacks, 1U);
  /** Linked cancellation is surfaced as the timeout that caused it. */
  EXPECT_EQ(result, -ETIMEDOUT);
  const auto cancelled = proactor.Cancel(*operation);
  ASSERT_FALSE(cancelled.has_value());
  EXPECT_EQ(cancelled.error().Code(), StatusCode::kNotFound);
  ASSERT_TRUE(proactor.PollOnce(PollBudget{}, 0).has_value());
  EXPECT_EQ(callbacks, 1U);
}

TEST_F(ProactorServiceIntegration, CompletionCannotRecursivelyAdvanceTheService) {
  bool rejected = false;
  ASSERT_TRUE(proactor
                  .ScheduleTimer(0,
                                 [this, &rejected](int result) noexcept {
                                   if (result != 0) return;
                                   const auto nested = proactor.PollOnce(PollBudget{}, 0);
                                   rejected = !nested && nested.error().Code() ==
                                                             StatusCode::kFailedPrecondition;
                                 })
                  .has_value());
  ASSERT_TRUE(proactor.PollOnce(PollBudget{}, 0).has_value());
  EXPECT_TRUE(rejected);
  EXPECT_EQ(proactor.RejectedTransitions(), 1U);
}

TEST_F(ProactorServiceIntegration, CancelBeforeSubmissionCompletesOnlyOnTheNextTick) {
  std::array<std::byte, 1> incoming{};
  unsigned callbacks = 0;
  int completion = 0;
  const auto operation =
      proactor.SubmitRecv(sockets[0], incoming, [&callbacks, &completion](int result) noexcept {
        ++callbacks;
        completion = result;
      });
  ASSERT_TRUE(operation.has_value());
  ASSERT_TRUE(proactor.Cancel(*operation).has_value());
  EXPECT_EQ(callbacks, 0U);
  const auto result = proactor.PollOnce(PollBudget{.commands = 1, .completions = 1}, 0);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->commands, 0U);
  EXPECT_EQ(result->completions, 1U);
  EXPECT_EQ(result->socket_events, 1U);
  EXPECT_EQ(callbacks, 1U);
  EXPECT_EQ(completion, -ECANCELED);
  EXPECT_EQ(proactor.PendingOperations(), 0U);
  ASSERT_TRUE(proactor.PollOnce(PollBudget{}, 0).has_value());
  EXPECT_EQ(callbacks, 1U);
}

TEST(ProactorServiceUnitLifecycle, PollAndShutdownRejectUnstartedService) {
  ProactorService proactor;
  EXPECT_EQ(proactor.RejectedTransitions(), 0U);
  EXPECT_FALSE(proactor.PollOnce(PollBudget{}, MonotonicTimestamp{}).has_value());
  EXPECT_EQ(proactor.RejectedTransitions(), 1U);
  EXPECT_FALSE(proactor.PollShutdown(PollBudget{}, MonotonicTimestamp{}).has_value());
  EXPECT_EQ(proactor.RejectedTransitions(), 2U);
  EXPECT_FALSE(proactor.Shutdown().has_value());
  EXPECT_EQ(proactor.RejectedTransitions(), 3U);
}

struct TestThreadId {
  std::uint64_t value{1};
  auto Current() const noexcept -> std::uint64_t { return value; }
};

TEST(ProactorOwnershipIntegration, WrongOwnerCannotMutateAStartedService) {
  TestThreadId identity;
  ProactorService proactor{ProactorOptions{}, ThreadIdSeam{identity}};
  ASSERT_TRUE(proactor.Start().has_value());
  unsigned callbacks = 0;
  const auto timer = proactor.ScheduleTimer(100, [&callbacks](int result) noexcept {
    EXPECT_EQ(result, -ECANCELED);
    ++callbacks;
  });
  ASSERT_TRUE(timer.has_value());
  const auto rejections = proactor.RejectedTransitions();
  identity.value = 2;
  auto denied = [](const auto& result) noexcept {
    return !result && result.error().Code() == StatusCode::kPermissionDenied;
  };
  EXPECT_TRUE(denied(proactor.Start()));
  EXPECT_TRUE(denied(proactor.Start(ProactorOptions{})));
  EXPECT_TRUE(denied(proactor.PollOnce(PollBudget{}, 0)));
  EXPECT_TRUE(denied(proactor.BeginShutdown(UINT64_MAX)));
  EXPECT_TRUE(denied(proactor.PollShutdown(PollBudget{}, 0)));
  EXPECT_TRUE(denied(proactor.Shutdown()));
  EXPECT_TRUE(denied(proactor.Cancel(*timer)));
  EXPECT_TRUE(denied(proactor.ScheduleTimer(0, [](int) noexcept {})));
  EXPECT_TRUE(denied(proactor.SubmitRecv(-1, {}, [](int) noexcept {})));
  EXPECT_EQ(proactor.State(), ServiceState::kFailed);
  EXPECT_FALSE(proactor.Stopped());
  EXPECT_EQ(callbacks, 0U);
  identity.value = 1;
  EXPECT_EQ(proactor.RejectedTransitions(), rejections);
  EXPECT_EQ(proactor.PendingOperations(), 1U);
  EXPECT_EQ(proactor.State(), ServiceState::kRunning);
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  ASSERT_TRUE(proactor.PollShutdown(PollBudget{}, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
  EXPECT_EQ(callbacks, 1U);
}

TEST(ProactorOwnershipIntegration, FirstStartBindsOwnershipInsteadOfConstruction) {
  TestThreadId identity;
  ProactorService proactor{ProactorOptions{}, ThreadIdSeam{identity}};
  identity.value = 2;
  ASSERT_TRUE(proactor.Start().has_value());
  identity.value = 1;
  const auto rejected = proactor.BeginShutdown(UINT64_MAX);
  EXPECT_FALSE(rejected.has_value());
  if (!rejected) {
    EXPECT_EQ(rejected.error().Code(), StatusCode::kPermissionDenied);
  }
  identity.value = 2;
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  ASSERT_TRUE(proactor.PollShutdown(PollBudget{}, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

TEST(ProactorOwnershipUnit, NativeIdentityAndIndependentInjectedProviders) {
  ProactorService native;
  EXPECT_EQ(native.CallerThreadId(), static_cast<std::uint64_t>(::gettid()));
  TestThreadId first{10};
  TestThreadId second{20};
  ProactorService a{ProactorOptions{}, ThreadIdSeam{first}};
  ProactorService b{ProactorOptions{}, ThreadIdSeam{second}};
  first.value = 11;
  EXPECT_EQ(a.CallerThreadId(), 11U);
  EXPECT_EQ(b.CallerThreadId(), 20U);
  first.value = 0;
  const auto invalid = a.Start();
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().Code(), StatusCode::kInvalidArgument);
}

TEST(ProactorCapacityIntegration, StagedCapacityRecoversAndSubmissionHonorsCommandBudget) {
  ProactorService proactor;
  ASSERT_TRUE(
      proactor.Start({.ring_entries = 2, .max_operations = 2, .max_timers = 0}).has_value());
  std::array<int, 2> sockets{-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  std::array<std::array<std::byte, 1>, 3> buffers{};
  unsigned callbacks = 0;
  unsigned cancelled = 0;
  unsigned rejected_callbacks = 0;
  auto completion = [&](int result) noexcept {
    ++callbacks;
    if (result == -ECANCELED) ++cancelled;
  };
  const auto first = proactor.SubmitRecv(sockets[0], buffers[0], completion);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(proactor.SubmitRecv(sockets[0], buffers[1], completion).has_value());
  const auto rejected = proactor.SubmitRecv(
      sockets[0], buffers[2], [&rejected_callbacks](int) noexcept { ++rejected_callbacks; });
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(proactor.PendingOperations(), 2U);
  EXPECT_EQ(proactor.RejectedTransitions(), 0U);
  const PollBudget budget{.commands = 1};
  const auto submitted = proactor.PollOnce(budget, 0);
  ASSERT_TRUE(submitted.has_value());
  EXPECT_EQ(submitted->commands, 1U);
  ASSERT_TRUE(proactor.Cancel(*first).has_value());
  for (unsigned i = 0; i < 100000 && callbacks == 0; ++i) {
    const auto result = proactor.PollOnce(budget, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_LE(result->commands, 1U);
  }
  ASSERT_EQ(callbacks, 1U);
  EXPECT_EQ(cancelled, 1U);
  ASSERT_TRUE(proactor.SubmitRecv(sockets[0], buffers[2], completion).has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i) {
    const auto result = proactor.PollShutdown(budget, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_LE(result->commands, 1U);
  }
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_EQ(callbacks, 3U);
  EXPECT_EQ(cancelled, 3U);
  EXPECT_EQ(rejected_callbacks, 0U);
  EXPECT_EQ(proactor.RejectedTransitions(), 0U);
  ASSERT_TRUE(proactor.Shutdown().has_value());
  for (const auto fd : sockets) EXPECT_EQ(::close(fd), 0);
}

TEST(ProactorCapacityIntegration, FullKernelSubmissionQueueRetriesTheRemainingStagedRequests) {
  ProactorService proactor;
  ASSERT_TRUE(
      proactor.Start({.ring_entries = 2, .max_operations = 4, .max_timers = 0}).has_value());
  std::array<int, 2> sockets{-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  std::array<std::byte, 4> buffers{};
  unsigned callbacks = 0;
  unsigned cancelled = 0;
  for (unsigned i = 0; i < buffers.size(); ++i) {
    ASSERT_TRUE(proactor
                    .SubmitRecv(sockets[0], std::span{buffers}.subspan(i, 1),
                                [&callbacks, &cancelled](int result) noexcept {
                                  ++callbacks;
                                  if (result == -ECANCELED) ++cancelled;
                                })
                    .has_value());
  }
  const PollBudget submission_budget{.commands = 4};
  const auto first = proactor.PollOnce(submission_budget, 0);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->commands, 2U);
  EXPECT_EQ(proactor.PendingOperations(), 4U);
  EXPECT_EQ(callbacks, 0U);
  const auto second = proactor.PollOnce(submission_budget, 0);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->commands, 2U);
  EXPECT_EQ(proactor.PendingOperations(), 4U);
  EXPECT_EQ(callbacks, 0U);
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  const PollBudget drain_budget{.commands = 1, .completions = 1};
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i) {
    const auto result = proactor.PollShutdown(drain_budget, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_LE(result->commands, 1U);
  }
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_EQ(callbacks, 4U);
  EXPECT_EQ(cancelled, 4U);
  EXPECT_EQ(proactor.RejectedTransitions(), 0U);
  ASSERT_TRUE(proactor.Shutdown().has_value());
  for (const auto fd : sockets) {
    EXPECT_EQ(::close(fd), 0);
  }
}
}  // namespace
