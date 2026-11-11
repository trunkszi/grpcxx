#include <limits>

#include "transport_fixture.h"
namespace {
using grpcxx::test::Harness;
TEST(ProactorUnary, PersistentLargeDeferredReplies) {
  Harness h;
  ASSERT_TRUE(h.Start(false));
  for (unsigned round = 0; round < 10; ++round) {
    std::vector<std::byte> payload(round == 0 ? 5uz * 1024uz * 1024uz : 32768uz, std::byte{0x51});
    h.completed = false;
    ASSERT_TRUE(h.client->CallUnary("/echo/Unary", payload, &h, &Harness::Response));
    ASSERT_TRUE(h.Wait());
    EXPECT_EQ(h.status, 0);
    EXPECT_EQ(h.received, payload);
    EXPECT_FALSE(h.server->IsCurrent(h.connection, h.stream, h.generation));
  }
  EXPECT_TRUE(h.valid);
  EXPECT_TRUE(h.reject_reentry);
}
TEST(ProactorUnary, TlsPreservesLargePayload) {
  Harness h;
  ASSERT_TRUE(h.Start(true));
  std::vector<std::byte> payload(131072, std::byte{0x37});
  ASSERT_TRUE(h.client->CallUnary("/echo/Unary", payload, &h, &Harness::Response));
  ASSERT_TRUE(h.Wait());
  EXPECT_EQ(h.status, 0);
  EXPECT_EQ(h.received, payload);
}
TEST(ProactorUnary, DeadlineShutdownDetachesDeferredRequest) {
  Harness h;
  h.deferred = true;
  ASSERT_TRUE(h.Start(false));
  const std::array payload{std::byte{1}, std::byte{2}};
  ASSERT_TRUE(h.client->CallUnary("/echo/Unary", payload, &h, &Harness::Response));
  for (unsigned tick = 0; tick < 20000 && h.generation == 0; ++tick) ASSERT_TRUE(h.Step());
  ASSERT_NE(h.generation, 0U);
  ASSERT_TRUE(h.client->BeginShutdown(0));
  ASSERT_TRUE(h.client->PollShutdown({}, 1));
  EXPECT_TRUE(h.completed);
  EXPECT_EQ(h.status, 4);
}
TEST(ProactorUnary, ForeignCompletionRemainsOnCallerRoutes) {
  Harness h;
  ASSERT_TRUE(h.Start(false));
  struct Foreign {
    bool delivered{};
    void OnCompletion(nexora::CompletionEvent) noexcept { delivered = true; }
  } foreign;
  auto route = nexora::CompletionRoute::Make<17>(
      &foreign, +[](void* context, nexora::CompletionEvent event) noexcept {
        static_cast<Foreign*>(context)->OnCompletion(event);
      });
  ASSERT_TRUE(h.routes.Register(route));
  ASSERT_TRUE(h.ring->Arm(17, uring::ops::PrepareNop()));
  for (unsigned tick = 0; tick < 10000 && !foreign.delivered; ++tick) ASSERT_TRUE(h.Step());
  EXPECT_TRUE(foreign.delivered);
}
TEST(ProactorUnary, SharedRingDnsEndpoints) {
  Harness h;
  h.dns = true;
  ASSERT_TRUE(h.Start(false));
  const std::array payload{std::byte{0x19}};
  ASSERT_TRUE(h.client->CallUnary("/echo/Unary", payload, &h, &Harness::Response));
  ASSERT_TRUE(h.Wait());
  EXPECT_EQ(h.status, 0);
  EXPECT_EQ(h.received, (std::vector<std::byte>{std::byte{0x19}}));
}
TEST(ProactorUnary, GracefulShutdownFlushesAnAcceptedResponse) {
  Harness h;
  ASSERT_TRUE(h.Start(false));
  std::vector<std::byte> payload(131072, std::byte{0x59});
  ASSERT_TRUE(h.client->CallUnary("/echo/Unary", payload, &h, &Harness::Response));
  for (unsigned tick = 0; tick < 100000 && !h.generation; ++tick) ASSERT_TRUE(h.Step());
  ASSERT_NE(h.generation, 0U);
  ASSERT_TRUE(h.server->BeginShutdown(std::numeric_limits<std::uint64_t>::max()));
  ASSERT_TRUE(h.client->BeginShutdown(std::numeric_limits<std::uint64_t>::max()));
  ASSERT_TRUE(h.Wait());
  EXPECT_EQ(h.status, 0);
  EXPECT_EQ(h.received, payload);
}
}  // namespace
