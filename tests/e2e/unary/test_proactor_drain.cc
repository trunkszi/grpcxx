#include <grpcxx/h2/h2_client_session.h>

#include <bit>
#include <nexora/http2/connection.hpp>
#include <nexora/socket/tcp_transport.hpp>

#include "transport_fixture.h"

namespace {
TEST(ProactorDrain, IdleShutdownSendsGoaway) {
  grpcxx::test::Harness harness;
  ASSERT_TRUE(harness.ring);
  auto listener = nexora::TcpListener::Bind({cares::IpAddress::FromV4({127, 0, 0, 1}), 0});
  ASSERT_TRUE(listener);
  grpcxx::ClientChannelService::Options options;
  options.endpoint = listener->LocalEndpoint();
  harness.client =
      std::make_unique<grpcxx::ClientChannelService>(*harness.ring, harness.routes, options);
  ASSERT_TRUE(harness.client->Start());
  for (unsigned step = 0; step < 10000 && harness.client->GetState() !=
                                              grpcxx::ClientChannelService::ChannelState::kReady;
       ++step) {
    ASSERT_TRUE(harness.ring->Tick(harness.routes));
    ASSERT_TRUE(harness.client->PollOnce({}, 1));
  }
  ASSERT_EQ(harness.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);
  auto peer = listener->Accept(1000);
  ASSERT_TRUE(peer);
  ASSERT_TRUE(harness.client->BeginShutdown(1'000'000'000ULL));
  auto receiver = std::make_unique<nexora::Http2Connection>(nexora::Http2Role::kServer);
  bool goaway = false, eof = false;
  std::array<std::uint8_t, 16384> input;
  for (unsigned step = 0; step < 20000 && !(harness.client->Stopped() && eof); ++step) {
    ASSERT_TRUE(harness.ring->Tick(harness.routes));
    ASSERT_TRUE(harness.client->PollShutdown({}, 2));
    auto read = peer->ReadSome(input, 0);
    if (read) {
      eof |= *read == 0;
      ASSERT_TRUE(receiver->FeedBytes(std::span(input).first(*read)));
      while (auto event = receiver->PeekEventView()) {
        goaway |= event->kind == nexora::Http2Event::Kind::kGoaway;
        receiver->ConsumeEvent();
      }
    }
  }
  EXPECT_TRUE(harness.client->Stopped());
  EXPECT_TRUE(eof);
  EXPECT_TRUE(goaway);
}

TEST(ProactorDrain, EofBoundsDeferredResponseDrain) {
  grpcxx::test::Harness harness;
  harness.deferred = true;
  ASSERT_TRUE(harness.Start(false));
  auto endpoint = harness.server->LocalAddress();
  ASSERT_TRUE(endpoint);
  auto peer = nexora::TcpTransport::ConnectTo(*endpoint, 1000);
  ASSERT_TRUE(peer);
  grpcxx::H2ClientSession session;
  ASSERT_TRUE(session.Submit(
      "/echo/Unary", {}, nullptr,
      +[](void*, std::int32_t, int, std::span<const std::byte>) noexcept {}));
  for (unsigned step = 0; step < 8; ++step) {
    auto wire = session.Send();
    ASSERT_TRUE(wire);
    if (wire->empty()) break;
    ASSERT_TRUE(peer->WriteAll({std::bit_cast<const std::uint8_t*>(wire->data()), wire->size()}));
  }
  for (unsigned step = 0; step < 1000; ++step) ASSERT_TRUE(harness.Step());
  ASSERT_TRUE(harness.server->IsCurrent(harness.connection, harness.stream, harness.generation));
  peer->ShutdownWrite();
  for (unsigned step = 0; step < 1000; ++step) ASSERT_TRUE(harness.Step());
  EXPECT_EQ(harness.server->ActiveConnections(), 2U);
  for (unsigned step = 0; step < 1000; ++step) {
    ASSERT_TRUE(harness.ring->Tick(harness.routes));
    ASSERT_TRUE(harness.server->PollOnce({}, 6'000'000'001ULL));
    ASSERT_TRUE(harness.client->PollOnce({}, 1));
  }
  EXPECT_EQ(harness.server->ActiveConnections(), 1U);
}
}  // namespace
