#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/server/grpc_server_service.h>
#include <gtest/gtest.h>

#include <thread>
TEST(ProactorOwnership, OtherThreadCannotStartServices) {
  auto ring = uring::Proactor::Create(128);
  ASSERT_TRUE(ring);
  nexora::CompletionRoutes<> routes;
  grpcxx::GrpcServerService server(*ring, routes, {});
  ASSERT_TRUE(server.Start());
  bool denied = false;
  std::jthread other([&] {
    auto result = server.LocalAddress();
    denied = !result && result.error().Code() == grpcxx::StatusCode::kPermissionDenied;
  });
  other.join();
  EXPECT_TRUE(denied);
  ASSERT_TRUE(server.BeginShutdown(0));
  ASSERT_TRUE(server.PollShutdown({}, 1));
  ASSERT_TRUE(server.Shutdown());
}
