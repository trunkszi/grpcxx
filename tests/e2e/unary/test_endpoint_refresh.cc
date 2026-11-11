#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/server/grpc_server_service.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>

namespace {
struct RefreshRig {
  uring::Result<uring::Proactor> ring{uring::Proactor::Create(256)};
  nexora::CompletionRoutes<> routes;
  std::unique_ptr<grpcxx::GrpcServerService> a, b;
  std::unique_ptr<grpcxx::ClientChannelService> client;
  std::optional<cares::Resolver<>> resolver;
  grpcxx::MonotonicTimestamp now{5'000'000'000ULL};
  unsigned delayed{}, old_done{}, new_done{};
  int old_status{-1}, new_status{-1};
  static constexpr std::array<std::byte, 1> kPayload{std::byte{0x42}};

  static void Delay(void* ctx, grpcxx::GrpcServerService&, std::size_t, std::int32_t, std::uint64_t,
                    std::string_view, std::span<const std::byte>) noexcept {
    ++static_cast<RefreshRig*>(ctx)->delayed;
  }
  static void Echo(void*, grpcxx::GrpcServerService& server, std::size_t connection,
                   std::int32_t stream, std::uint64_t generation, std::string_view,
                   std::span<const std::byte> payload) noexcept {
    static_cast<void>(server.Complete(connection, stream, generation, payload));
  }
  static void OldDone(void* ctx, std::int32_t, int status, std::span<const std::byte>) noexcept {
    auto& self = *static_cast<RefreshRig*>(ctx);
    ++self.old_done;
    self.old_status = status;
  }
  static void NewDone(void* ctx, std::int32_t, int status,
                      std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<RefreshRig*>(ctx);
    ++self.new_done;
    self.new_status = status;
    EXPECT_EQ(payload.size(), kPayload.size());
    if (!payload.empty()) EXPECT_EQ(payload.front(), kPayload.front());
  }
  void OnForeignDeliveries(std::span<const uring::Delivery> deliveries) noexcept {
    static_cast<void>(routes.DispatchBatch(deliveries));
  }
  bool Step() {
    return ring->Tick(routes).has_value() && a->PollOnce({}, now).has_value() &&
           b->PollOnce({}, now).has_value() && client->PollOnce({}, now).has_value();
  }
  template <typename Predicate>
  bool Until(Predicate predicate) {
    for (unsigned tick = 0; tick < 20000 && !predicate(); ++tick)
      if (!Step()) return false;
    return predicate();
  }
  ~RefreshRig() {
    if (!ring) return;
    if (client) static_cast<void>(client->BeginShutdown(0));
    if (a) static_cast<void>(a->BeginShutdown(0));
    if (b) static_cast<void>(b->BeginShutdown(0));
    for (unsigned tick = 0; tick < 20000; ++tick) {
      static_cast<void>(ring->Tick(routes));
      if (client) static_cast<void>(client->PollShutdown({}, now));
      if (a) static_cast<void>(a->PollShutdown({}, now));
      if (b) static_cast<void>(b->PollShutdown({}, now));
      if ((!client || client->Stopped()) && (!a || a->Stopped()) && (!b || b->Stopped())) break;
    }
    if (client) static_cast<void>(client->Shutdown());
    if (a) static_cast<void>(a->Shutdown());
    if (b) static_cast<void>(b->Shutdown());
  }
};

void CheckRefresh(bool dns) {
  RefreshRig rig;
  ASSERT_TRUE(rig.ring);
  rig.a = std::make_unique<grpcxx::GrpcServerService>(
      *rig.ring, rig.routes,
      grpcxx::GrpcServerService::Options{.context = &rig, .handler = &RefreshRig::Delay});
  ASSERT_TRUE(rig.a->Start());
  const auto first = rig.a->LocalAddress();
  ASSERT_TRUE(first);
  const cares::Endpoint other{cares::IpAddress::FromV4({127, 0, 0, 2}), first->port};
  rig.b = std::make_unique<grpcxx::GrpcServerService>(
      *rig.ring, rig.routes,
      grpcxx::GrpcServerService::Options{.endpoint = other, .handler = &RefreshRig::Echo});
  ASSERT_TRUE(rig.b->Start());
  grpcxx::ClientChannelService::Options options{.endpoint = *first};
  if (dns) {
    auto resolver = cares::Resolver<>::FromSystem(*rig.ring);
    ASSERT_TRUE(resolver);
    rig.resolver.emplace(std::move(*resolver));
    rig.resolver->BindForeignRouter(&rig);
    ASSERT_TRUE(rig.resolver->InsertStaticHost("refresh.test", first->ip));
    options.target = "refresh.test";
    options.port = first->port;
    rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes,
                                                                *rig.resolver, options);
  } else {
    rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, options);
  }
  ASSERT_TRUE(rig.client->Start());
  const auto ready = [&] {
    return rig.client->GetState() == grpcxx::ClientChannelService::ChannelState::kReady;
  };
  ASSERT_TRUE(rig.Until(ready));
  ASSERT_TRUE(
      rig.client->CallUnary("/echo/Unary", RefreshRig::kPayload, &rig, &RefreshRig::OldDone));
  ASSERT_TRUE(rig.Until([&] { return rig.delayed == 1; }));
  ASSERT_EQ(rig.old_done, 0U);
  if (dns) {
    auto replacement = cares::Resolver<>::FromSystem(
        *rig.ring, cares::ResolverConfig{.subsystem_id = 8,
                                         .foreign_observer = rig.resolver->ForeignObserver()});
    ASSERT_TRUE(replacement);
    ASSERT_TRUE(replacement->InsertStaticHost("refresh.test", other.ip));
    *rig.resolver = std::move(*replacement);
    ASSERT_TRUE(rig.client->ReResolve());
  } else {
    const std::array endpoints{other};
    ASSERT_TRUE(rig.client->ReconcileEndpoints(endpoints));
  }
  ASSERT_TRUE(rig.Until([&] { return rig.old_done != 0 && ready(); }));
  EXPECT_EQ(rig.old_done, 1U);
  EXPECT_EQ(rig.old_status, static_cast<int>(grpcxx::StatusCode::kUnavailable));
  for (unsigned call = 1; call <= 2; ++call) {
    ASSERT_TRUE(
        rig.client->CallUnary("/echo/Unary", RefreshRig::kPayload, &rig, &RefreshRig::NewDone));
    ASSERT_TRUE(rig.Until([&] { return rig.new_done == call; }));
    EXPECT_EQ(rig.new_status, 0);
    EXPECT_EQ(rig.old_done, 1U);
  }
}
TEST(EndpointRefreshTest, ReconcileFailsRetiredCallsOnce) { CheckRefresh(false); }
TEST(EndpointRefreshTest, ReResolveFailsRetiredCallsOnce) { CheckRefresh(true); }
}  // namespace
