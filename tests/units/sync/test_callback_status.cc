#include <grpcxx/sync/in_memory_channel_pair.h>
#include <grpcxx/sync/synchronous_channel.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <nexora/http2/connection.hpp>

namespace grpcxx {
namespace {
TEST(SyncLifetimeTest, ResetHeadersPreservesCallbackViews) {
  auto pair = InMemoryChannelPair::Create();
  ASSERT_TRUE(pair);
  bool called = false;
  (*pair)->Server().SetHeadersHandler([&](SynchronousServer& server, std::int32_t id,
                                          std::string_view path,
                                          const MetadataMap& metadata) noexcept {
    called = true;
    EXPECT_EQ(path, "/test.Service/Bidi");
    EXPECT_EQ(metadata.Get("x-test"), "keep-alive");
    EXPECT_TRUE(server.ResetStream(id));
    EXPECT_EQ(path, "/test.Service/Bidi");
    EXPECT_EQ(metadata.Get("x-test"), "keep-alive");
  });
  CallOptions options;
  options.timeout = std::chrono::milliseconds{100};
  ASSERT_TRUE(options.metadata.Add("x-test", "keep-alive"));
  auto stream = (*pair)->Channel().BidiStreamCall("/test.Service/Bidi", options);
  ASSERT_TRUE(stream);
  for (unsigned step = 0; step < 10 && !called; ++step)
    static_cast<void>((*pair)->Channel().PollOnce());
  EXPECT_TRUE(called);
  EXPECT_EQ((*pair)->Server().ActiveStreams(), 0U);
}

TEST(SyncStatusTest, CardinalityAbortKeepsConnectionHealthy) {
  unsigned calls = 0;
  auto pair = InMemoryChannelPair::Create(
      [&](SynchronousServer& server, const ServerRequest& request) noexcept {
        if (request.event == ServerRequest::Event::kMessage) {
          EXPECT_TRUE(server.Send(request.stream_id, request.payload));
          if (calls == 0) EXPECT_TRUE(server.Send(request.stream_id, request.payload));
        } else if (request.event == ServerRequest::Event::kHalfClose) {
          ++calls;
          EXPECT_TRUE(server.Finish(request.stream_id, Status{}));
        }
      });
  ASSERT_TRUE(pair);
  constexpr std::array payload{std::byte{0x42}};
  auto first = (*pair)->Channel().Unary("/test.Service/Unary", payload);
  EXPECT_EQ(first ? first->status.Code() : first.error().Code(), StatusCode::kInternal);
  auto second = (*pair)->Channel().Unary("/test.Service/Unary", payload);
  ASSERT_TRUE(second);
  EXPECT_TRUE(second->status.Ok());
  EXPECT_EQ(second->payload.size(), payload.size());
  EXPECT_EQ(calls, 2U);
}

TEST(SyncStatusTest, Non200RejectsStreamingFinish) {
  for (const bool bidi : {false, true}) {
    auto pipes = nexora::MemoryPipe::CreatePair(262144);
    ASSERT_TRUE(pipes);
    auto peer = std::move(pipes->server);
    auto h2 = std::make_unique<nexora::Http2Connection>(nexora::Http2Role::kServer);
    ASSERT_TRUE(h2->InitiateHandshake());
    auto channel = SynchronousChannel::FromMemoryPipe(std::move(pipes->client));
    ASSERT_TRUE(channel);
    bool replied = false;
    (*channel)->SetProgressHook([&]() noexcept -> std::expected<void, Status> {
      std::array<std::uint8_t, 262144> buffer;
      auto read = peer.ReadSome(buffer, 0);
      if (!read) return std::unexpected(Status{StatusCode::kInternal});
      std::size_t offset = 0;
      while (offset < *read || h2->IsIngressPaused()) {
        auto fed = h2->FeedBytes(std::span{buffer}.first(*read).subspan(offset));
        if (!fed.Ok()) return std::unexpected(Status{StatusCode::kInternal});
        offset += fed.Consumed();
        while (auto event = h2->PeekEventView()) {
          if (event->kind == nexora::Http2Event::Kind::kHeaders && !replied) {
            nexora::Headers<> headers;
            if (!headers.Add("content-type", "application/grpc") ||
                !h2->SubmitResponseHeaders(event->stream_id, nexora::StatusCode{201}, headers,
                                           true))
              return std::unexpected(Status{StatusCode::kInternal});
            replied = true;
          }
          h2->ConsumeEvent();
        }
      }
      auto output = h2->PendingOutput();
      if (!output.empty()) {
        auto written = peer.Write(output);
        if (!written) return std::unexpected(Status{StatusCode::kInternal});
        h2->ConsumeOutput(*written);
      }
      return {};
    });
    CallOptions options;
    options.timeout = std::chrono::milliseconds{200};
    auto stream = bidi ? (*channel)->BidiStreamCall("/test.Service/Bidi", options)
                       : (*channel)->ServerStreamCall("/test.Service/Stream", {}, options);
    ASSERT_TRUE(stream);
    auto finished = (*stream)->Finish();
    EXPECT_TRUE(replied);
    EXPECT_EQ(finished ? finished->status.Code() : finished.error().Code(), StatusCode::kUnknown);
  }
}
}  // namespace
}  // namespace grpcxx
