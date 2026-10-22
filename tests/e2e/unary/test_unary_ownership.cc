#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/server/grpc_server_service.h>
#include <gtest/gtest.h>
#include <netinet/in.h>

#include <array>

namespace {
using namespace grpcxx;

struct CallerIdentity {
  std::uint64_t value{1};
  auto Current() const noexcept -> std::uint64_t { return value; }
};

TEST(UnaryOwnershipIntegration, WrongOwnerCannotAdvanceEitherService) {
  CallerIdentity identity;
  ProactorService proactor{ProactorOptions{}, ThreadIdSeam{identity}};
  ASSERT_TRUE(proactor.Start().has_value());
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  GrpcServerService server{proactor, {.address = std::as_bytes(std::span{&endpoint, 1})}};
  ASSERT_TRUE(server.Start().has_value());
  std::array<std::byte, sizeof(sockaddr_in)> address{};
  const auto size = server.LocalAddress(address);
  ASSERT_TRUE(size.has_value());
  ClientChannelService client{proactor, {.address = std::span{address}.first(*size)}};
  ASSERT_TRUE(client.Start().has_value());

  identity.value = 2;
  auto denied = [](const auto& result) noexcept {
    return !result && result.error().Code() == StatusCode::kPermissionDenied;
  };
  EXPECT_TRUE(denied(server.Start()));
  EXPECT_TRUE(denied(server.LocalAddress(address)));
  EXPECT_TRUE(denied(server.PollOnce(PollBudget{}, 0)));
  EXPECT_TRUE(denied(server.BeginShutdown(UINT64_MAX)));
  EXPECT_TRUE(denied(server.PollShutdown(PollBudget{}, 0)));
  EXPECT_TRUE(denied(server.Shutdown()));
  EXPECT_TRUE(denied(server.Complete(0, 1, 1, {})));
  EXPECT_FALSE(server.IsCurrent(0, 1, 1));
  EXPECT_EQ(server.State(), ServiceState::kFailed);
  EXPECT_FALSE(server.Stopped());
  EXPECT_TRUE(denied(client.Start()));
  EXPECT_TRUE(denied(client.PollOnce(PollBudget{}, 0)));
  EXPECT_TRUE(denied(client.BeginShutdown(UINT64_MAX)));
  EXPECT_TRUE(denied(client.PollShutdown(PollBudget{}, 0)));
  EXPECT_TRUE(denied(client.Shutdown()));
  EXPECT_TRUE(denied(client.CallUnary("/test/Call", {}, nullptr, nullptr)));
  EXPECT_EQ(client.State(), ServiceState::kFailed);
  EXPECT_FALSE(client.Stopped());

  identity.value = 1;
  EXPECT_EQ(server.RejectedTransitions(), 0U);
  EXPECT_EQ(client.RejectedTransitions(), 0U);
  EXPECT_EQ(server.State(), ServiceState::kRunning);
  EXPECT_EQ(client.State(), ServiceState::kStarting);
  ASSERT_TRUE(client.BeginShutdown(UINT64_MAX).has_value());
  ASSERT_TRUE(server.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned n = 0; n < 100000 && (!client.Stopped() || !server.Stopped()); ++n) {
    ASSERT_TRUE(proactor.PollOnce(PollBudget{}, 0).has_value());
    ASSERT_TRUE(client.PollShutdown(PollBudget{}, 0).has_value());
    ASSERT_TRUE(server.PollShutdown(PollBudget{}, 0).has_value());
  }
  ASSERT_TRUE(client.Stopped());
  ASSERT_TRUE(server.Stopped());
  EXPECT_TRUE(client.Shutdown().has_value());
  EXPECT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned n = 0; n < 100000 && !proactor.Stopped(); ++n)
    ASSERT_TRUE(proactor.PollShutdown(PollBudget{}, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
}
}  // namespace
