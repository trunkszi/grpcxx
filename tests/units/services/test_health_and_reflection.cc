#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <ranges>
#include <span>
#include <string_view>
#include <type_traits>

#include "grpcxx/interceptor/interceptor_chain.h"
#include "grpcxx/services/health_service.h"
#include "grpcxx/services/reflection_service.h"

// vcpkg gtest was built against glibc 2.38. This toolchain sysroot's libc does
// not export the ISO C23 entry points, so the test provides them.
extern "C" long __isoc23_strtol(const char* text, char** end, int base) {
  return std::strtol(text, end, base);
}

extern "C" unsigned long long __isoc23_strtoull(const char* text, char** end, int base) {
  return std::strtoull(text, end, base);
}

namespace {

struct ClientGate {
  int* client_hits{nullptr};
  int* server_hits{nullptr};

  grpcxx::InterceptorAction OnClient(grpcxx::CallContext& context) noexcept {
    if (client_hits != nullptr) {
      ++(*client_hits);
    }
    if (auto added = context.AddMetadata("x-request-id", "req-7"); !added) {
      return grpcxx::InterceptorAction::kStop;
    }
    return grpcxx::InterceptorAction::kContinue;
  }

  grpcxx::InterceptorAction OnServer(grpcxx::CallContext& context) noexcept {
    if (server_hits != nullptr) {
      ++(*server_hits);
    }
    if (auto added = context.AddMetadata("x-server-id", "srv-7"); !added) {
      return grpcxx::InterceptorAction::kStop;
    }
    return grpcxx::InterceptorAction::kContinue;
  }
};

struct StopGate {
  int* hits{nullptr};

  grpcxx::InterceptorAction OnClient(grpcxx::CallContext& context) noexcept {
    if (hits != nullptr) {
      ++(*hits);
    }
    if (auto added = context.AddMetadata("x-gate", "closed"); !added) {
      return grpcxx::InterceptorAction::kStop;
    }
    return grpcxx::InterceptorAction::kStop;
  }

  grpcxx::InterceptorAction OnServer(grpcxx::CallContext& context) noexcept {
    if (hits != nullptr) {
      ++(*hits);
    }
    if (auto added = context.AddMetadata("x-gate", "closed"); !added) {
      return grpcxx::InterceptorAction::kStop;
    }
    return grpcxx::InterceptorAction::kStop;
  }
};

struct TailCall {
  int* hits{nullptr};

  grpcxx::InterceptorAction operator()(grpcxx::CallContext& context) noexcept {
    if (hits != nullptr) {
      ++(*hits);
    }
    if (auto added = context.AddMetadata("x-tail", "should-not"); !added) {
      return grpcxx::InterceptorAction::kStop;
    }
    return grpcxx::InterceptorAction::kContinue;
  }
};

[[nodiscard]] bool HasMetadata(const grpcxx::CallContext& context, std::string_view key,
                               std::string_view value) {
  const auto matches = [key, value](const grpcxx::MetadataEntry& entry) {
    return entry.key == key && entry.value == value;
  };
  return std::ranges::any_of(context.Metadata(), matches);
}

struct EchoContract {
  static constexpr std::string_view kServiceName{"grpcxx.Echo"};
  static constexpr std::array<grpcxx::ContractMethod, 2> kMethods{{
      {.name = "Echo", .kind = grpcxx::RpcKind::kUnary},
      {.name = "EchoStream", .kind = grpcxx::RpcKind::kServerStreaming},
  }};
};

struct HealthContract {
  static constexpr std::string_view kServiceName{"grpc.health.v1.Health"};
  static constexpr std::array<grpcxx::ContractMethod, 2> kMethods{{
      {.name = "Check", .kind = grpcxx::RpcKind::kUnary},
      {.name = "Watch", .kind = grpcxx::RpcKind::kServerStreaming},
  }};
};

}  // namespace

namespace grpcxx {

static_assert(ServiceConcept<HealthService>);
static_assert(ServiceConcept<ReflectionService>);

TEST(InterceptorChain, ShortCircuitInjectsMetadataAndSkipsTail) {
  static_assert(CallContext::kMetadataCapacity == 16);
  static_assert(StopsChain(InterceptorAction::kStop));
  static_assert(!StopsChain(InterceptorAction::kContinue));

  int client_hits = 0;
  int server_hits = 0;
  int stop_hits = 0;
  int tail_hits = 0;
  InterceptorChain chain{ClientGate{&client_hits, &server_hits}, StopGate{&stop_hits},
                         TailCall{&tail_hits}};
  static_assert(decltype(chain)::Size() == 3);

  CallContext client{};
  EXPECT_EQ(chain.RunClient(client), InterceptorAction::kStop);
  EXPECT_TRUE(client.short_circuited);
  EXPECT_EQ(client_hits, 1);
  EXPECT_EQ(server_hits, 0);
  EXPECT_EQ(stop_hits, 1);
  EXPECT_EQ(tail_hits, 0);
  EXPECT_TRUE(HasMetadata(client, "x-request-id", "req-7"));
  EXPECT_TRUE(HasMetadata(client, "x-gate", "closed"));
  EXPECT_FALSE(HasMetadata(client, "x-tail", "should-not"));

  client_hits = 0;
  server_hits = 0;
  stop_hits = 0;
  tail_hits = 0;
  CallContext server{};
  EXPECT_EQ(chain.RunServer(server), InterceptorAction::kStop);
  EXPECT_TRUE(server.short_circuited);
  EXPECT_EQ(client_hits, 0);
  EXPECT_EQ(server_hits, 1);
  EXPECT_EQ(stop_hits, 1);
  EXPECT_EQ(tail_hits, 0);
  EXPECT_TRUE(HasMetadata(server, "x-server-id", "srv-7"));
  EXPECT_TRUE(HasMetadata(server, "x-gate", "closed"));
  EXPECT_FALSE(HasMetadata(server, "x-tail", "should-not"));
}

TEST(HealthService, CheckAndWatchFollowServingStatus) {
  static_assert(!std::is_copy_constructible_v<grpcxx::HealthService>);
  static_assert(!std::is_move_constructible_v<grpcxx::HealthService>);
  static_assert(noexcept(std::declval<grpcxx::HealthService&>().Start()));
  static_assert(noexcept(std::declval<grpcxx::HealthService&>().Check(std::string_view{})));

  grpcxx::HealthService health;
  EXPECT_EQ(static_cast<int>(health.State()), static_cast<int>(grpcxx::ServiceState::kStopped));
  const auto before_start = health.Check("");
  ASSERT_FALSE(before_start);
  EXPECT_EQ(static_cast<int>(before_start.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kFailedPrecondition));

  ASSERT_TRUE(health.Start());
  EXPECT_EQ(static_cast<int>(health.State()), static_cast<int>(grpcxx::ServiceState::kRunning));
  const auto polled = health.PollOnce(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{10});
  ASSERT_TRUE(polled);

  const auto overall = health.Check("");
  ASSERT_TRUE(overall);
  EXPECT_EQ(static_cast<int>(*overall), static_cast<int>(grpcxx::ServingStatus::kServing));

  const auto missing = health.Check("orders");
  ASSERT_FALSE(missing);
  EXPECT_EQ(static_cast<int>(missing.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kNotFound));

  ASSERT_TRUE(health.SetServingStatus("payments", grpcxx::ServingStatus::kNotServing));
  const auto payments = health.Check("payments");
  ASSERT_TRUE(payments);
  EXPECT_EQ(static_cast<int>(*payments), static_cast<int>(grpcxx::ServingStatus::kNotServing));

  ASSERT_TRUE(health.SetServingStatus("", grpcxx::ServingStatus::kNotServing));
  const auto overall_down = health.Check("");
  ASSERT_TRUE(overall_down);
  EXPECT_EQ(static_cast<int>(*overall_down), static_cast<int>(grpcxx::ServingStatus::kNotServing));

  const auto watched = health.Watch("orders");
  ASSERT_TRUE(watched);
  std::array<grpcxx::ServingStatus, 4> events{};
  const auto initial = health.DrainWatch(*watched, events);
  ASSERT_TRUE(initial);
  EXPECT_EQ(*initial, 1U);
  EXPECT_EQ(static_cast<int>(events[0]), static_cast<int>(grpcxx::ServingStatus::kServiceUnknown));

  const auto other = health.Watch("payments");
  ASSERT_TRUE(other);
  const auto other_initial = health.DrainWatch(*other, events);
  ASSERT_TRUE(other_initial);
  EXPECT_EQ(*other_initial, 1U);
  EXPECT_EQ(static_cast<int>(events[0]), static_cast<int>(grpcxx::ServingStatus::kNotServing));

  ASSERT_TRUE(health.SetServingStatus("orders", grpcxx::ServingStatus::kServing));
  const auto updated = health.DrainWatch(*watched, events);
  ASSERT_TRUE(updated);
  EXPECT_EQ(*updated, 1U);
  EXPECT_EQ(static_cast<int>(events[0]), static_cast<int>(grpcxx::ServingStatus::kServing));
  const auto other_quiet = health.DrainWatch(*other, events);
  ASSERT_TRUE(other_quiet);
  EXPECT_EQ(*other_quiet, 0U);

  const auto orders = health.Check("orders");
  ASSERT_TRUE(orders);
  EXPECT_EQ(static_cast<int>(*orders), static_cast<int>(grpcxx::ServingStatus::kServing));

  ASSERT_TRUE(health.BeginShutdown(grpcxx::MonotonicTimestamp{15}));
  EXPECT_EQ(static_cast<int>(health.State()), static_cast<int>(grpcxx::ServiceState::kStopping));
  const auto drained = health.PollShutdown(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{20});
  ASSERT_TRUE(drained);
  EXPECT_TRUE(*drained);
  EXPECT_TRUE(health.Stopped());
  ASSERT_TRUE(health.Shutdown());
  EXPECT_EQ(static_cast<int>(health.State()), static_cast<int>(grpcxx::ServiceState::kStopped));
  EXPECT_TRUE(health.Stopped());
}

TEST(ReflectionService, ListServicesAndRejectsDuplicates) {
  static_assert(grpcxx::ReflectionTableContract<EchoContract>);
  static_assert(!std::is_copy_constructible_v<grpcxx::ReflectionService>);
  static_assert(!std::is_move_constructible_v<grpcxx::ReflectionService>);

  grpcxx::ReflectionService reflection;
  const auto early = reflection.ListServices({});
  ASSERT_FALSE(early);
  EXPECT_EQ(static_cast<int>(early.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kFailedPrecondition));

  ASSERT_TRUE(reflection.Start());
  ASSERT_TRUE(reflection.Register<EchoContract>());
  const auto duplicate = reflection.Register<EchoContract>();
  ASSERT_FALSE(duplicate);
  EXPECT_EQ(static_cast<int>(duplicate.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kAlreadyExists));
  ASSERT_TRUE(reflection.Register<HealthContract>());

  const auto too_small = reflection.ListServices({});
  ASSERT_FALSE(too_small);
  EXPECT_EQ(static_cast<int>(too_small.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kResourceExhausted));

  std::array<std::string_view, 4> names{};
  const auto listed = reflection.ListServices(names);
  ASSERT_TRUE(listed);
  EXPECT_EQ(*listed, 2U);
  EXPECT_EQ(names[0], EchoContract::kServiceName);
  EXPECT_EQ(names[1], HealthContract::kServiceName);

  std::array<grpcxx::ContractMethod, 4> methods{};
  const auto echo_methods = reflection.ListMethods("grpcxx.Echo", methods);
  ASSERT_TRUE(echo_methods);
  EXPECT_EQ(*echo_methods, 2U);
  EXPECT_EQ(methods[0].name, "Echo");
  EXPECT_EQ(static_cast<int>(methods[0].kind), static_cast<int>(grpcxx::RpcKind::kUnary));
  EXPECT_EQ(methods[1].name, "EchoStream");

  const auto one = reflection.ListMethods("grpcxx.Echo/Echo", methods);
  ASSERT_TRUE(one);
  EXPECT_EQ(*one, 1U);
  EXPECT_EQ(methods[0].name, "Echo");

  const auto dotted = reflection.ListMethods("grpc.health.v1.Health.Watch", methods);
  ASSERT_TRUE(dotted);
  EXPECT_EQ(*dotted, 1U);
  EXPECT_EQ(methods[0].name, "Watch");
  EXPECT_EQ(static_cast<int>(methods[0].kind), static_cast<int>(grpcxx::RpcKind::kServerStreaming));

  const auto unknown = reflection.ListMethods("missing.Service", methods);
  ASSERT_FALSE(unknown);
  EXPECT_EQ(static_cast<int>(unknown.error().Code()),
            static_cast<int>(grpcxx::StatusCode::kNotFound));

  ASSERT_TRUE(reflection.BeginShutdown(grpcxx::MonotonicTimestamp{6}));
  const auto drained = reflection.PollShutdown(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{5});
  ASSERT_TRUE(drained);
  EXPECT_TRUE(*drained);
  EXPECT_TRUE(reflection.Stopped());
  ASSERT_TRUE(reflection.Shutdown());
  EXPECT_EQ(static_cast<int>(reflection.State()), static_cast<int>(grpcxx::ServiceState::kStopped));
}

}  // namespace grpcxx
