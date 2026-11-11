#include "transport_fixture.h"
namespace {
TEST(ProactorStreaming, NativeBidirectionalStreaming) {
  for (const bool secure : {false, true}) {
    for (const bool aggregate : {false, true}) {
      grpcxx::test::Harness h;
      h.streaming = true;
      h.aggregate = aggregate;
      ASSERT_TRUE(h.Start(secure));
      std::vector<std::vector<std::byte>> received;
      auto opened = h.client->OpenStream(
          "/echo/Stream",
          grpcxx::ClientChannelService::MessageCompletion{
              [&](std::int32_t, std::span<const std::byte> payload) noexcept {
                received.emplace_back(payload.begin(), payload.end());
              }},
          grpcxx::ClientChannelService::UnaryCompletion{
              [&](std::int32_t, int status, std::span<const std::byte>) noexcept {
                h.completed = true;
                h.status = status;
              }});
      ASSERT_TRUE(opened);
      const unsigned requests = aggregate ? 3 : 1;
      for (unsigned index = 0; index < requests; ++index) {
        std::vector<std::byte> payload(index == 0 ? 131072uz : 17uz, std::byte{0x41});
        ASSERT_TRUE(h.client->SendMessage(*opened, payload));
        for (unsigned tick = 0; tick < 100000 && h.messages.size() <= index; ++tick)
          ASSERT_TRUE(h.Step());
        ASSERT_EQ(h.messages.size(), index + 1);
        EXPECT_EQ(h.messages.back(), payload);
        if (!aggregate) {
          for (unsigned tick = 0; tick < 100000 && received.empty(); ++tick) ASSERT_TRUE(h.Step());
          ASSERT_EQ(received.size(), 1U);
          EXPECT_EQ(received.front(), payload);
          h.replies.push_back({std::byte{0x42}});
          for (unsigned tick = 0; tick < 100000 && received.size() < 2; ++tick)
            ASSERT_TRUE(h.Step());
          ASSERT_EQ(received.size(), 2U);
        }
      }
      ASSERT_TRUE(h.client->HalfClose(*opened));
      ASSERT_TRUE(h.Wait());
      EXPECT_EQ(h.status, 0);
      if (aggregate) {
        ASSERT_EQ(received.size(), 1U);
        EXPECT_EQ(received[0][0], std::byte{3});
      }
    }
  }
}
TEST(ProactorStreaming, CancellationInvalidatesPeer) {
  grpcxx::test::Harness h;
  h.streaming = true;
  h.aggregate = true;
  ASSERT_TRUE(h.Start(false));
  unsigned messages = 0;
  auto id = h.client->OpenStream(
      "/echo/Stream",
      grpcxx::ClientChannelService::MessageCompletion{
          [&](std::int32_t, std::span<const std::byte>) noexcept { ++messages; }},
      grpcxx::ClientChannelService::UnaryCompletion{
          [&](std::int32_t, int status, std::span<const std::byte>) noexcept {
            h.status = status;
            h.completed = true;
          }});
  ASSERT_TRUE(id);
  const std::array payload{std::byte{0x27}};
  ASSERT_TRUE(h.client->SendMessage(*id, payload));
  for (unsigned tick = 0; tick < 100000 && !h.generation; ++tick) ASSERT_TRUE(h.Step());
  ASSERT_NE(h.generation, 0U);
  ASSERT_TRUE(h.client->Cancel(*id));
  ASSERT_TRUE(h.Wait());
  EXPECT_EQ(h.status, 1);
  EXPECT_EQ(messages, 0U);
  for (unsigned tick = 0;
       tick < 100000 && h.server->IsCurrent(h.connection, h.stream, h.generation); ++tick)
    ASSERT_TRUE(h.Step());
  EXPECT_FALSE(h.server->IsCurrent(h.connection, h.stream, h.generation));
}
}  // namespace
