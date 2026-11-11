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
static_assert(uarch::NoPadding<MetadataEntry>);
static_assert(uarch::NoPadding<uarch::BitWord<ReflectionServiceMetaSpec>> &&
              uarch::RegisterPassable<uarch::BitWord<ReflectionServiceMetaSpec>>);
static_assert(uarch::NoPadding<uarch::BitWord<HealthServiceMetaSpec>> &&
              uarch::RegisterPassable<uarch::BitWord<HealthServiceMetaSpec>>);

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
  EXPECT_EQ(health.State(), grpcxx::ServiceState::kStopped);
  const auto before_start = health.Check("");
  ASSERT_FALSE(before_start);
  EXPECT_EQ(before_start.error().Code(), grpcxx::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(health.Start());
  EXPECT_EQ(health.State(), grpcxx::ServiceState::kRunning);
  const auto polled = health.PollOnce(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{10});
  ASSERT_TRUE(polled);

  const auto overall = health.Check("");
  ASSERT_TRUE(overall);
  EXPECT_EQ(*overall, grpcxx::ServingStatus::kServing);

  const auto missing = health.Check("records");
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error().Code(), grpcxx::StatusCode::kNotFound);

  ASSERT_TRUE(health.SetServingStatus("metrics", grpcxx::ServingStatus::kNotServing));
  const auto metrics = health.Check("metrics");
  ASSERT_TRUE(metrics);
  EXPECT_EQ(*metrics, grpcxx::ServingStatus::kNotServing);

  ASSERT_TRUE(health.SetServingStatus("", grpcxx::ServingStatus::kNotServing));
  const auto overall_down = health.Check("");
  ASSERT_TRUE(overall_down);
  EXPECT_EQ(*overall_down, grpcxx::ServingStatus::kNotServing);

  const auto watched = health.Watch("records");
  ASSERT_TRUE(watched);
  std::array<grpcxx::ServingStatus, 4> events{};
  const auto initial = health.DrainWatch(*watched, events);
  ASSERT_TRUE(initial);
  EXPECT_EQ(*initial, 1U);
  EXPECT_EQ(events[0], grpcxx::ServingStatus::kServiceUnknown);

  const auto other = health.Watch("metrics");
  ASSERT_TRUE(other);
  const auto other_initial = health.DrainWatch(*other, events);
  ASSERT_TRUE(other_initial);
  EXPECT_EQ(*other_initial, 1U);
  EXPECT_EQ(events[0], grpcxx::ServingStatus::kNotServing);

  ASSERT_TRUE(health.SetServingStatus("records", grpcxx::ServingStatus::kServing));
  const auto updated = health.DrainWatch(*watched, events);
  ASSERT_TRUE(updated);
  EXPECT_EQ(*updated, 1U);
  EXPECT_EQ(events[0], grpcxx::ServingStatus::kServing);
  const auto other_quiet = health.DrainWatch(*other, events);
  ASSERT_TRUE(other_quiet);
  EXPECT_EQ(*other_quiet, 0U);

  const auto records = health.Check("records");
  ASSERT_TRUE(records);
  EXPECT_EQ(*records, grpcxx::ServingStatus::kServing);

  ASSERT_TRUE(health.BeginShutdown(grpcxx::MonotonicTimestamp{15}));
  EXPECT_EQ(health.State(), grpcxx::ServiceState::kStopping);
  const auto drained = health.PollShutdown(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{20});
  ASSERT_TRUE(drained);
  EXPECT_TRUE(*drained);
  EXPECT_TRUE(health.Stopped());
  ASSERT_TRUE(health.Shutdown());
  EXPECT_EQ(health.State(), grpcxx::ServiceState::kStopped);
  EXPECT_TRUE(health.Stopped());
}

TEST(ReflectionService, ListServicesAndRejectsDuplicates) {
  static_assert(grpcxx::ReflectionTableContract<EchoContract>);
  static_assert(!std::is_copy_constructible_v<grpcxx::ReflectionService>);
  static_assert(!std::is_move_constructible_v<grpcxx::ReflectionService>);

  grpcxx::ReflectionService reflection;
  const auto early = reflection.ListServices({});
  ASSERT_FALSE(early);
  EXPECT_EQ(early.error().Code(), grpcxx::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(reflection.Start());
  ASSERT_TRUE(reflection.Register<EchoContract>());
  const auto duplicate = reflection.Register<EchoContract>();
  ASSERT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().Code(), grpcxx::StatusCode::kAlreadyExists);
  ASSERT_TRUE(reflection.Register<HealthContract>());

  const auto too_small = reflection.ListServices({});
  ASSERT_FALSE(too_small);
  EXPECT_EQ(too_small.error().Code(), grpcxx::StatusCode::kResourceExhausted);

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
  EXPECT_EQ(methods[0].kind, grpcxx::RpcKind::kUnary);
  EXPECT_EQ(methods[1].name, "EchoStream");

  const auto one = reflection.ListMethods("grpcxx.Echo/Echo", methods);
  ASSERT_TRUE(one);
  EXPECT_EQ(*one, 1U);
  EXPECT_EQ(methods[0].name, "Echo");

  const auto dotted = reflection.ListMethods("grpc.health.v1.Health.Watch", methods);
  ASSERT_TRUE(dotted);
  EXPECT_EQ(*dotted, 1U);
  EXPECT_EQ(methods[0].name, "Watch");
  EXPECT_EQ(methods[0].kind, grpcxx::RpcKind::kServerStreaming);

  const auto unknown = reflection.ListMethods("missing.Service", methods);
  ASSERT_FALSE(unknown);
  EXPECT_EQ(unknown.error().Code(), grpcxx::StatusCode::kNotFound);

  ASSERT_TRUE(reflection.BeginShutdown(grpcxx::MonotonicTimestamp{6}));
  const auto drained = reflection.PollShutdown(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{5});
  ASSERT_TRUE(drained);
  EXPECT_TRUE(*drained);
  EXPECT_TRUE(reflection.Stopped());
  ASSERT_TRUE(reflection.Shutdown());
  EXPECT_EQ(reflection.State(), grpcxx::ServiceState::kStopped);
}

template <std::size_t Index>
struct IndexedContract {
  static constexpr std::array<char, 19> kNameStorage = [] consteval {
    std::array<char, 19> buf{'g', 'r', 'p', 'c', 'x', 'x', '.', 'S', 'v', 'c',
                             '0', '0', '.', 'v', '1', '.', 'A', 'p', 'i'};
    buf[10] = static_cast<char>('0' + (Index / 10U));
    buf[11] = static_cast<char>('0' + (Index % 10U));
    return buf;
  }();
  static constexpr std::string_view kServiceName{kNameStorage.data(), kNameStorage.size()};
  static constexpr std::array<grpcxx::ContractMethod, 1> kMethods{{
      {.name = "Call", .kind = grpcxx::RpcKind::kUnary},
  }};
};

TEST(ReflectionService, Bitmap64AndFnv1a64PreFilterUpToMaxCapacityAndResetOnShutdown) {
  grpcxx::ReflectionService reflection;
  ASSERT_TRUE(reflection.Start());

  []<std::size_t... Is>(grpcxx::ReflectionService& svc, std::index_sequence<Is...>) {
    ([&] { EXPECT_TRUE(svc.Register<IndexedContract<Is>>()); }(), ...);
  }(reflection, std::make_index_sequence<grpcxx::ReflectionService::kMaxServices>{});

  const auto overflow = reflection.Register<EchoContract>();
  ASSERT_FALSE(overflow);
  EXPECT_EQ(overflow.error().Code(), grpcxx::StatusCode::kResourceExhausted);

  std::array<std::string_view, grpcxx::ReflectionService::kMaxServices> listed{};
  const auto count = reflection.ListServices(listed);
  ASSERT_TRUE(count);
  EXPECT_EQ(*count, grpcxx::ReflectionService::kMaxServices);
  EXPECT_EQ(listed[0], IndexedContract<0>::kServiceName);
  EXPECT_EQ(listed[31], IndexedContract<31>::kServiceName);

  // Lookup same-length names that share prefix/suffix to exercise fnv1a64 pre-filtering.
  std::array<grpcxx::ContractMethod, 2> methods{};
  const auto hit = reflection.ListMethods(IndexedContract<17>::kServiceName, methods);
  ASSERT_TRUE(hit);
  EXPECT_EQ(*hit, 1U);
  EXPECT_EQ(methods[0].name, "Call");

  const auto same_len_miss = reflection.ListMethods("grpcxx.Svc99.v1.Api", methods);
  ASSERT_FALSE(same_len_miss);
  EXPECT_EQ(same_len_miss.error().Code(), grpcxx::StatusCode::kNotFound);

  ASSERT_TRUE(reflection.BeginShutdown(grpcxx::MonotonicTimestamp{10}));
  EXPECT_FALSE(reflection.Stopped());
  ASSERT_TRUE(reflection.PollShutdown(grpcxx::PollBudget{}, grpcxx::MonotonicTimestamp{11}));
  EXPECT_TRUE(reflection.Stopped());
  ASSERT_TRUE(reflection.Shutdown());
  ASSERT_TRUE(reflection.Start());
  const auto after_reset = reflection.ListServices(listed);
  ASSERT_TRUE(after_reset);
  EXPECT_EQ(*after_reset, 0U);
}

}  // namespace grpcxx
