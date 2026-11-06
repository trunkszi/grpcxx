#include <dirent.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string_view>
#include <utility>

#include "grpcxx/dns/dns_service.h"

// vcpkg gtest calls the glibc 2.38 ISO C23 aliases. This toolchain sysroot
// exports only strtol/strtoull, which have the same signature.
extern "C" {
long __isoc23_strtol(const char* nptr, char** endptr, int base) {
  return std::strtol(nptr, endptr, base);
}

unsigned long long __isoc23_strtoull(const char* nptr, char** endptr, int base) {
  return std::strtoull(nptr, endptr, base);
}
}

namespace {
using grpcxx::DnsService;
using grpcxx::MonotonicTimestamp;
using grpcxx::PollBudget;
using grpcxx::ResolvedEndpoint;
using grpcxx::ServiceState;
using grpcxx::Status;
using grpcxx::StatusCode;

struct Capture {
  int calls{0};
  Status status{};
  std::array<ResolvedEndpoint, 8> endpoints{};
  std::size_t count{0};

  static void OnDone(void* user, Status status,
                     std::span<const ResolvedEndpoint> endpoints) noexcept {
    auto* self = static_cast<Capture*>(user);
    ++self->calls;
    self->status = std::move(status);
    self->count = 0;
    for (const ResolvedEndpoint& endpoint : endpoints) {
      if (self->count >= self->endpoints.size()) {
        break;
      }
      self->endpoints[self->count] = endpoint;
      ++self->count;
    }
  }
};

[[nodiscard]] bool HasNumeric(const Capture& capture, std::string_view numeric) noexcept {
  for (const ResolvedEndpoint& endpoint :
       std::span<const ResolvedEndpoint>(capture.endpoints).first(capture.count)) {
    if (endpoint.Numeric() == numeric) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] std::size_t TaskCount() noexcept {
  DIR* dir = ::opendir("/proc/self/task");
  if (dir == nullptr) {
    return 0;
  }
  std::size_t count = 0;
  while (const dirent* entry = ::readdir(dir)) {
    if (entry->d_name[0] == '.') {
      continue;
    }
    ++count;
  }
  ::closedir(dir);
  return count;
}

[[nodiscard]] bool Drain(DnsService& service) {
  const auto begin = service.BeginShutdown(MonotonicTimestamp{1});
  EXPECT_TRUE(begin.has_value());
  EXPECT_EQ(service.State(), ServiceState::kStopping);
  EXPECT_FALSE(service.Stopped());
  const auto drained = service.PollShutdown(PollBudget{}, MonotonicTimestamp{2});
  EXPECT_TRUE(drained.has_value());
  EXPECT_TRUE(drained.has_value() && *drained);
  EXPECT_TRUE(service.Stopped());
  EXPECT_EQ(service.PendingResolutions(), 0U);
  const auto shutdown = service.Shutdown();
  EXPECT_TRUE(shutdown.has_value());
  EXPECT_EQ(service.State(), ServiceState::kStopped);
  const auto again = service.Shutdown();
  EXPECT_TRUE(again.has_value());
  return begin.has_value() && drained.has_value() && *drained && shutdown.has_value() &&
         again.has_value();
}

}  // namespace

TEST(DnsService, ShutdownFromStoppedIsIdempotent) {
  DnsService service;
  EXPECT_EQ(service.State(), ServiceState::kStopped);
  EXPECT_TRUE(service.Stopped());
  EXPECT_EQ(service.PendingResolutions(), 0U);
  ASSERT_TRUE(service.Shutdown().has_value());
  EXPECT_EQ(service.State(), ServiceState::kStopped);
  EXPECT_TRUE(service.Stopped());
  ASSERT_TRUE(service.Shutdown().has_value());
}

TEST(DnsService, LifecycleRejectsMisorderedCalls) {
  DnsService service;
  Capture capture;
  const auto early_resolve = service.Resolve("127.0.0.1", 53, &Capture::OnDone, &capture);
  ASSERT_FALSE(early_resolve.has_value());
  EXPECT_EQ(early_resolve.error().Code(), StatusCode::kFailedPrecondition);
  const auto early_poll = service.PollOnce(PollBudget{}, MonotonicTimestamp{});
  ASSERT_FALSE(early_poll.has_value());
  EXPECT_EQ(early_poll.error().Code(), StatusCode::kFailedPrecondition);

  ASSERT_TRUE(service.Start().has_value());
  EXPECT_EQ(service.State(), ServiceState::kRunning);
  EXPECT_FALSE(service.Stopped());
  const auto second_start = service.Start();
  ASSERT_FALSE(second_start.has_value());
  EXPECT_EQ(second_start.error().Code(), StatusCode::kFailedPrecondition);

  const auto idle = service.PollOnce(PollBudget{}, MonotonicTimestamp{1});
  ASSERT_TRUE(idle.has_value());
  EXPECT_EQ(idle->completions, 0U);

  const auto empty = service.Resolve("", 1, &Capture::OnDone, &capture);
  ASSERT_FALSE(empty.has_value());
  EXPECT_EQ(empty.error().Code(), StatusCode::kInvalidArgument);

  const auto early_shutdown_poll = service.PollShutdown(PollBudget{}, MonotonicTimestamp{});
  ASSERT_FALSE(early_shutdown_poll.has_value());
  EXPECT_EQ(early_shutdown_poll.error().Code(), StatusCode::kFailedPrecondition);

  ASSERT_TRUE(Drain(service));

  ASSERT_TRUE(service.Start().has_value());
  EXPECT_EQ(service.State(), ServiceState::kRunning);
  ASSERT_TRUE(Drain(service));
}

TEST(DnsService, ShutdownWhileRunningMustDrainFirst) {
  DnsService service;
  ASSERT_TRUE(service.Start().has_value());
  const auto direct = service.Shutdown();
  ASSERT_FALSE(direct.has_value());
  EXPECT_EQ(direct.error().Code(), StatusCode::kFailedPrecondition);
  ASSERT_TRUE(Drain(service));
}

TEST(DnsService, ResolvesNumericAndLocalhostWithoutThreads) {
  DnsService service;
  ASSERT_TRUE(service.Start().has_value());
  const std::size_t tasks_before = TaskCount();

  Capture loopback;
  ASSERT_TRUE(service.Resolve("127.0.0.1", 5353, &Capture::OnDone, &loopback).has_value());
  if (loopback.calls == 0) {
    const auto polled = service.PollOnce(PollBudget{}, MonotonicTimestamp{10});
    ASSERT_TRUE(polled.has_value());
  }
  ASSERT_EQ(loopback.calls, 1);
  EXPECT_TRUE(loopback.status.Ok());
  EXPECT_TRUE(HasNumeric(loopback, "127.0.0.1"));
  ASSERT_GT(loopback.count, 0U);
  EXPECT_EQ(loopback.endpoints[0].port, 5353);
  EXPECT_EQ(service.PendingResolutions(), 0U);

  Capture local;
  ASSERT_TRUE(service.Resolve("localhost", 9, &Capture::OnDone, &local).has_value());
  if (local.calls == 0) {
    const auto polled = service.PollOnce(PollBudget{}, MonotonicTimestamp{20});
    ASSERT_TRUE(polled.has_value());
  }
  ASSERT_EQ(local.calls, 1);
  EXPECT_TRUE(local.status.Ok());
  EXPECT_TRUE(HasNumeric(local, "127.0.0.1") || HasNumeric(local, "::1"));
  ASSERT_GT(local.count, 0U);
  EXPECT_EQ(local.endpoints[0].port, 9);
  EXPECT_EQ(TaskCount(), tasks_before);

  ASSERT_TRUE(Drain(service));
}

TEST(DnsService, BeginShutdownCancelsOutstandingLookup) {
  DnsService service;
  ASSERT_TRUE(service.Start().has_value());
  Capture capture;
  ASSERT_TRUE(service.Resolve("ticket07.invalid", 53, &Capture::OnDone, &capture).has_value());
  if (capture.calls == 0) {
    EXPECT_GE(service.PendingResolutions(), 1U);
    ASSERT_TRUE(service.BeginShutdown(MonotonicTimestamp{30}).has_value());
    EXPECT_EQ(service.State(), ServiceState::kStopping);
    EXPECT_EQ(capture.calls, 1);
    EXPECT_EQ(capture.status.Code(), StatusCode::kCancelled);
    EXPECT_FALSE(service.Stopped());
    const auto drained = service.PollShutdown(PollBudget{}, MonotonicTimestamp{31});
    ASSERT_TRUE(drained.has_value());
    EXPECT_TRUE(*drained);
    EXPECT_TRUE(service.Stopped());
    EXPECT_EQ(service.PendingResolutions(), 0U);
    ASSERT_TRUE(service.Shutdown().has_value());
    return;
  }
  EXPECT_FALSE(capture.status.Ok());
  ASSERT_TRUE(Drain(service));
}
