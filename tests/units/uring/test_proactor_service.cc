#include <grpcxx/uring/proactor_service.h>
#include <gtest/gtest.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
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
TEST(UringRingIntegration, LinkedReservationRollsBackWhenOnlyOneSqeRemains) {
  UringRing ring;
  ASSERT_TRUE(ring.Open(2, 2));
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  std::array<std::byte, 2> bytes{};
  const UringRing::Request single{.opcode = UringRing::Opcode::kSend,
                                  .fd = sockets[0],
                                  .transmit = std::span<const std::byte>{bytes}.first(1),
                                  .token = 0x03400001};
  ASSERT_TRUE(ring.Queue(single));
  auto linked = single;
  linked.token = 0x01400002;
  linked.timeout_ns = 1'000'000'000;
  auto rejected = ring.Queue(linked);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  auto second = single;
  second.token = 0x02400003;
  ASSERT_TRUE(ring.Queue(second));
  ASSERT_TRUE(ring.Submit());
  std::array<UringRing::Completion, 2> completions{};
  std::size_t count = 0;
  for (unsigned i = 0; i < 100000 && count < completions.size(); ++i)
    count += ring.Peek(std::span{completions}.subspan(count));
  ASSERT_EQ(count, 2U);
  EXPECT_EQ(completions[0].result, 1);
  EXPECT_EQ(completions[1].result, 1);
  EXPECT_TRUE((completions[0].token == single.token && completions[1].token == second.token) ||
              (completions[1].token == single.token && completions[0].token == second.token));
  ring.Close();
  for (int fd : sockets) EXPECT_EQ(::close(fd), 0);
}

void CheckIdleWithoutKernelEntry() {
  UringRing ring;
  ProactorService proactor;
  if (!ring.Open(4, 1) || !proactor.Start()) _exit(1);
  // Any attempted entry becomes a visible error; the filter is confined to this child.
  sock_filter instructions[] = {
      BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr)),
      BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_io_uring_enter, 0, 1),
      BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
      BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
  };
  sock_fprog filter{.len = 4, .filter = instructions};
  if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 ||
      ::prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &filter) != 0)
    _exit(2);
  for (unsigned i = 0; i < 1000; ++i) {
    if (!ring.Submit()) _exit(3);
    const auto result = proactor.PollOnce(PollBudget{}, 0);
    if (!result || result->commands != 0 || result->pending) _exit(4);
  }
  if (!proactor.BeginShutdown(UINT64_MAX) || !proactor.Shutdown()) _exit(5);
  ring.Close();
  _exit(0);
}

TEST(UringRingIntegration, IdleSubmitAndPollNeverEnterKernel) {
  ASSERT_EXIT(CheckIdleWithoutKernelEntry(), testing::ExitedWithCode(0), "");
}

TEST_F(ProactorServiceIntegration, SubmittedSlotsAreRestagedForCancellationAndReuse) {
  std::array<std::byte, 130> buffers{};
  std::array<OperationId, 130> ids{};
  unsigned cancelled = 0;
  for (std::size_t i = 0; i < ids.size(); ++i) {
    auto operation = proactor.SubmitRecv(sockets[0], std::span{buffers}.subspan(i, 1),
                                         [&cancelled](int result) noexcept {
                                           EXPECT_EQ(result, -ECANCELED);
                                           ++cancelled;
                                         });
    ASSERT_TRUE(operation);
    ids[i] = *operation;
  }
  auto submitted = proactor.PollOnce(PollBudget{.commands = 130}, 0);
  ASSERT_TRUE(submitted);
  EXPECT_EQ(submitted->commands, 130U);
  auto idle = proactor.PollOnce(PollBudget{}, 0);
  ASSERT_TRUE(idle);
  EXPECT_EQ(idle->commands, 0U);
  EXPECT_EQ(proactor.PendingOperations(), ids.size());
  for (const auto index : {0U, 64U, 129U}) ASSERT_TRUE(proactor.Cancel(ids[index]));
  for (unsigned i = 0; i < 100000 && cancelled < 3; ++i)
    ASSERT_TRUE(proactor.PollOnce(PollBudget{.commands = 1}, 0));
  ASSERT_EQ(cancelled, 3U);
  EXPECT_FALSE(proactor.Cancel(ids[0]));
  auto reused = proactor.SubmitRecv(sockets[0], std::span{buffers}.first(1),
                                    [&cancelled](int result) noexcept {
                                      EXPECT_EQ(result, -ECANCELED);
                                      ++cancelled;
                                    });
  ASSERT_TRUE(reused);
  EXPECT_NE(reused->value, ids[0].value);
  ASSERT_TRUE(proactor.Cancel(*reused));
  auto completed = proactor.PollOnce(PollBudget{}, 0);
  ASSERT_TRUE(completed);
  EXPECT_EQ(completed->commands, 0U);
  EXPECT_EQ(cancelled, 4U);
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX));
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
    ASSERT_TRUE(proactor.PollShutdown(PollBudget{}, 0));
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_EQ(cancelled, 131U);
  ASSERT_TRUE(proactor.Shutdown());
}

TEST(UringRingIntegration, PeekDoesNotPublishQueuedRequests) {
  UringRing ring;
  ASSERT_TRUE(ring.Open(4, 1));
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  std::array<std::byte, 1> payload{std::byte{42}};
  std::array<std::byte, 1> incoming{};
  ASSERT_TRUE(ring.Queue(
      {.opcode = UringRing::Opcode::kRecv, .fd = sockets[0], .receive = incoming, .token = 16}));
  ASSERT_TRUE(ring.Submit());
  ASSERT_TRUE(ring.Queue(
      {.opcode = UringRing::Opcode::kSend, .fd = sockets[0], .transmit = payload, .token = 17}));
  ASSERT_EQ(::send(sockets[1], payload.data(), payload.size(), MSG_NOSIGNAL), 1);
  std::array<UringRing::Completion, 1> completion{};
  std::size_t count = 0;
  for (unsigned i = 0; i < 100000 && count == 0; ++i) count = ring.Peek(completion);
  ASSERT_EQ(count, 1U);
  EXPECT_EQ(completion[0].token, 16U);
  EXPECT_EQ(incoming, payload);
  for (unsigned i = 0; i < 16; ++i) EXPECT_EQ(ring.Peek(completion), 0U);
  std::byte received{};
  EXPECT_EQ(::recv(sockets[1], &received, 1, MSG_DONTWAIT), -1);
  EXPECT_EQ(errno, EAGAIN);
  ASSERT_TRUE(ring.Submit());
  count = 0;
  for (unsigned i = 0; i < 100000 && count == 0; ++i) count = ring.Peek(completion);
  ASSERT_EQ(count, 1U);
  EXPECT_EQ(completion[0].token, 17U);
  ring.Close();
  for (int fd : sockets) EXPECT_EQ(::close(fd), 0);
}

TEST(ProactorSparseIntegration, HierarchicalTimerSlotsReuseAcrossWordBoundaries) {
  ProactorService proactor{{.ring_entries = 8, .max_operations = 3, .max_timers = 4101}};
  ASSERT_TRUE(proactor.Start());
  std::array<OperationId, 4101> ids{};
  unsigned callbacks = 0;
  for (auto& id : ids) {
    auto created = proactor.ScheduleTimer(1000, [&callbacks](int) noexcept { ++callbacks; });
    ASSERT_TRUE(created);
    id = *created;
  }
  EXPECT_FALSE(proactor.ScheduleTimer(0, [](int) noexcept {}));
  for (std::size_t i : {0U, 63U, 64U, 4096U, 4100U}) ASSERT_TRUE(proactor.Cancel(ids[i]));
  auto first = proactor.PollOnce(PollBudget{.timers = 2}, 0);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->timers, 2U);
  EXPECT_EQ(callbacks, 2U);
  EXPECT_EQ(first->deadline, 1000U);
  auto replacement =
      proactor.ScheduleTimer(UINT64_MAX, [&callbacks](int) noexcept { ++callbacks; });
  ASSERT_TRUE(replacement);
  EXPECT_NE(replacement->value, ids[0].value);
  EXPECT_FALSE(proactor.Cancel(ids[0]));
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX));
  for (unsigned i = 0; i < 5000 && !proactor.Stopped(); ++i) {
    auto result = proactor.PollShutdown(PollBudget{.timers = 3}, 0);
    ASSERT_TRUE(result);
    EXPECT_LE(result->timers, 3U);
  }
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_EQ(callbacks, 4102U);
  ASSERT_TRUE(proactor.Shutdown());
}

TEST(ProactorSparseIntegration, InvalidConnectReturnsItsSlotAndCallbacksCanReuseTimers) {
  ProactorService proactor{{.ring_entries = 4, .max_operations = 1, .max_timers = 1}};
  ASSERT_TRUE(proactor.Start());
  std::array<std::byte, 1> address{};
  for (unsigned i = 0; i < 4; ++i)
    EXPECT_FALSE(proactor.SubmitConnect(1, address, [](int) noexcept {}));
  EXPECT_EQ(proactor.PendingOperations(), 0U);
  unsigned callbacks = 0;
  ASSERT_TRUE(proactor.ScheduleTimer(0, [&](int) noexcept {
    ++callbacks;
    EXPECT_TRUE(proactor.ScheduleTimer(UINT64_MAX, [&](int) noexcept { ++callbacks; }));
  }));
  auto polled = proactor.PollOnce(PollBudget{}, 0);
  ASSERT_TRUE(polled);
  EXPECT_EQ(callbacks, 1U);
  EXPECT_EQ(polled->deadline, UINT64_MAX);
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX));
  ASSERT_TRUE(proactor.PollShutdown(PollBudget{}, 0));
  EXPECT_EQ(callbacks, 2U);
  ASSERT_TRUE(proactor.Shutdown());
}
}  // namespace
