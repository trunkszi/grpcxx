#include "session_fixture.h"
namespace {
TEST(H2MemoryUnary, ImmediateRoundtripAndFragmentedData) {
  for (const auto chunk : std::array<std::size_t, 4>{1, 3, 17, 65536}) {
    MemoryPair pair;
    pair.chunk = chunk;
    ASSERT_TRUE(pair.client.Ready());
    ASSERT_TRUE(pair.server.Ready());
    ServerHandler handler{.server = &pair.server};
    pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    std::vector<std::byte> message(32768, std::byte{0x65});
    Result result;
    ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", message, &result, &Result::Complete));
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(handler.calls, 1);
    EXPECT_EQ(handler.failures, 0);
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status, 0);
    EXPECT_EQ(result.payload, message);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
}

TEST(H2MemoryUnary, DeferredReplyRejectsStaleGeneration) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  const std::array message{std::byte{0x42}, std::byte{0x19}};
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", message, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(handler.calls, 1);
  EXPECT_EQ(pair.client.ActiveStreams(), 1);
  EXPECT_EQ(pair.server.ActiveStreams(), 1);
  EXPECT_EQ(result.calls, 0);
  EXPECT_TRUE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation + 1, handler.pending));
  ASSERT_TRUE(pair.server.Complete(handler.stream, handler.generation, handler.pending));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 0);
  EXPECT_EQ(result.payload, (std::vector<std::byte>{std::byte{0x42}, std::byte{0x19}}));
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_FALSE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation, handler.pending));
}

TEST(H2MemoryUnary, UnknownPathReturnsTrailersOnlyUnimplemented) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Missing", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(handler.calls, 1);
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 12);
  EXPECT_TRUE(result.payload.empty());
}

TEST(H2MemoryUnary, GoawayDrainsExistingStreams) {
  MemoryPair pair;
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(pair.client.Ping());
  ASSERT_TRUE(pair.server.Ping());
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(pair.client.RejectedTransitions(), 0);
  EXPECT_EQ(pair.server.RejectedTransitions(), 0);
  ASSERT_TRUE(pair.server.GoAway());
  ASSERT_TRUE(pair.Drive());
  EXPECT_NE(pair.client.GetState(), H2ClientSession::State::kOpen);
  Result result;
  EXPECT_FALSE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  EXPECT_EQ(result.calls, 0);
}

TEST(H2MemoryUnary, PeerResetInvalidatesDeferredGeneration) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  auto stream = pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete);
  ASSERT_TRUE(stream);
  ASSERT_TRUE(pair.Drive());
  const auto stream_id = static_cast<std::uint32_t>(*stream);
  const std::array reset{std::byte{0},
                         std::byte{0},
                         std::byte{4},
                         std::byte{3},
                         std::byte{0},
                         std::byte{static_cast<unsigned char>((stream_id >> 24) & 0x7f)},
                         std::byte{static_cast<unsigned char>((stream_id >> 16) & 0xff)},
                         std::byte{static_cast<unsigned char>((stream_id >> 8) & 0xff)},
                         std::byte{static_cast<unsigned char>(stream_id & 0xff)},
                         std::byte{0},
                         std::byte{0},
                         std::byte{0},
                         std::byte{8}};
  auto received = pair.server.Receive(reset);
  ASSERT_TRUE(received);
  EXPECT_EQ(*received, reset.size());
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_FALSE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation, {}));
}

TEST(H2MemoryUnary, CancellationInvalidatesServerHandle) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  auto stream = pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete);
  ASSERT_TRUE(stream);
  ASSERT_TRUE(pair.Drive());
  EXPECT_TRUE(pair.server.IsCurrent(handler.stream, handler.generation));
  ASSERT_TRUE(pair.client.Cancel(*stream));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.calls, 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_FALSE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation, {}));
}

TEST(H2MemoryUnary, RecursivePumpPreservesCompletion) {
  MemoryPair pair;
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  ServerHandler handler{.server = &pair.server, .probe_recursion = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  result.client = &pair.client;
  const std::array message{std::byte{42}};
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", message, &result, &Result::Complete));
  EXPECT_EQ(pair.client.ActiveStreams(), 1);
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 0);
  EXPECT_EQ(result.payload, (std::vector<std::byte>{std::byte{42}}));
  EXPECT_EQ(handler.recursion_rejected, 2);
  EXPECT_EQ(result.recursion_rejected, 2);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_EQ(pair.client.RejectedTransitions(), 2);
  EXPECT_EQ(pair.server.RejectedTransitions(), 2);
}

TEST(H2MemoryUnary, CapacityFlowControlAndReuse) {
  MemoryPair pair;
  pair.chunk = 257;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  std::array<Result, H2ClientSession::kMaxStreams> results;
  std::vector<std::byte> maximum(H2ClientSession::kMaxMessageBytes, std::byte{0x5a});
  const std::array small{std::byte{1}, std::byte{2}, std::byte{3}};
  for (auto index : std::views::iota(std::size_t{0}, results.size())) {
    const auto payload =
        index == 0 ? std::span<const std::byte>{maximum} : std::span<const std::byte>{small};
    ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", payload, &results[index], &Result::Complete));
  }
  EXPECT_EQ(pair.client.ActiveStreams(), results.size());
  Result excess;
  auto rejected = pair.client.Submit("/rpc.Test/Echo", {}, &excess, &Result::Complete);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().Code(), grpcxx::StatusCode::kResourceExhausted);
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(handler.calls, results.size());
  EXPECT_EQ(handler.failures, 0);
  for (auto index : std::views::iota(std::size_t{0}, results.size())) {
    EXPECT_EQ(results[index].calls, 1);
    EXPECT_EQ(results[index].status, 0);
    if (index == 0) {
      EXPECT_EQ(results[index].payload, maximum);
    } else {
      EXPECT_TRUE(std::ranges::equal(results[index].payload, small));
    }
  }
  EXPECT_EQ(excess.calls, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &excess, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(excess.calls, 1);
  EXPECT_EQ(excess.status, 0);
  EXPECT_TRUE(excess.payload.empty());
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}
}  // namespace
