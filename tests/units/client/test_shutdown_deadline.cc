#include <grpcxx/client/client_channel_service.h>
#include <gtest/gtest.h>

#include <array>
#include <cares/cares.hpp>
#include <cstdint>
#include <memory>
#include <nexora/dispatch/completion_routes.hpp>
#include <nexora/socket/tcp_transport.hpp>
#include <span>
#include <uring/uring.hpp>

namespace {

struct Watch {
  int completions{0};
  int status{-1};

  static void Done(void* context, std::int32_t, int code, std::span<const std::byte>) noexcept {
    auto& self = *static_cast<Watch*>(context);
    ++self.completions;
    self.status = code;
  }
};

TEST(ClientChannelShutdown, DeadlineCancelsPendingOutput) {
  auto ring = uring::Proactor::Create(64);
  ASSERT_TRUE(ring);
  nexora::CompletionRoutes<> routes;
  auto listener = nexora::TcpListener::Bind({cares::IpAddress::FromV4({127, 0, 0, 1}), 0});
  ASSERT_TRUE(listener);
  grpcxx::ClientChannelService::Options options;
  options.endpoint = listener->LocalEndpoint();
  auto client = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, options);
  ASSERT_TRUE(client->Start());
  for (unsigned step = 0;
       step < 10000 && client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++step) {
    ASSERT_TRUE(ring->Tick(routes));
    ASSERT_TRUE(client->PollOnce({}, 1));
  }
  ASSERT_EQ(client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);
  auto peer = listener->Accept(1000);
  ASSERT_TRUE(peer);

  Watch watch;
  std::array<std::byte, 8> payload{};
  ASSERT_TRUE(client->CallUnary("/echo/Unary", payload, &watch, &Watch::Done).has_value());
  constexpr grpcxx::MonotonicTimestamp kDeadline = 5'000;
  ASSERT_TRUE(client->BeginShutdown(kDeadline));
  ASSERT_TRUE(client->PollOnce({}, 1'000));
  EXPECT_EQ(watch.completions, 0);
  ASSERT_TRUE(client->PollOnce({}, kDeadline));
  EXPECT_EQ(watch.completions, 1);
  EXPECT_EQ(watch.status, static_cast<int>(grpcxx::StatusCode::kDeadlineExceeded));
  for (unsigned step = 0; step < 10000 && !client->Stopped(); ++step) {
    ASSERT_TRUE(ring->Tick(routes));
    ASSERT_TRUE(client->PollOnce({}, kDeadline));
  }
  EXPECT_TRUE(client->Stopped());
}

}  // namespace
