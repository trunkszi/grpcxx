#include "session_fixture.h"
namespace {
TEST(H2MemoryUnary, OversizedStreamPreservesIndependentRequest) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result bad;
  Result good;
  std::vector<std::byte> maximum(H2ClientSession::kMaxMessageBytes, std::byte{0x5a});
  const std::array normal{std::byte{9}, std::byte{7}};
  auto first = pair.client.Submit("/rpc.Test/Echo", maximum, &bad, &Result::Complete);
  ASSERT_TRUE(first);
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", normal, &good, &Result::Complete));
  ExcessData mutation{.stream = *first};
  pair.mutation_context = &mutation;
  pair.mutate = &ExcessData::Mutate;
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(mutation.changed);
  EXPECT_EQ(bad.calls, 1);
  EXPECT_NE(bad.status, 0);
  EXPECT_EQ(good.calls, 1);
  EXPECT_EQ(good.status, 0);
  EXPECT_TRUE(std::ranges::equal(good.payload, normal));
  EXPECT_EQ(handler.calls, 1);
  EXPECT_EQ(pair.server.RejectedTransitions(), 1);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_TRUE(pair.client.Ready());
  EXPECT_TRUE(pair.server.Ready());
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(bad.calls, 1);
  EXPECT_EQ(good.calls, 1);
}

TEST(H2MemoryUnary, NonterminalGrpcStatusRejected) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  PrematureStatus mutation;
  pair.mutation_context = &mutation;
  pair.mutate = &PrematureStatus::Mutate;
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(mutation.changed);
  EXPECT_EQ(result.calls, 1);
  EXPECT_NE(result.status, 0);
  EXPECT_GT(pair.client.RejectedTransitions(), 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_TRUE(pair.client.Ready());
  EXPECT_TRUE(pair.server.Ready());
}

TEST(H2MemoryUnary, FatalFloodInvalidatesDeferredHandles) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(pair.server.IsCurrent(handler.stream, handler.generation));
  const std::array ping{std::byte{0}, std::byte{0}, std::byte{8}, std::byte{6}, std::byte{0},
                        std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
                        std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
                        std::byte{0}, std::byte{0}};
  bool failed = false;
  for (auto attempt : std::views::iota(0, 20000)) {
    static_cast<void>(attempt);
    if (auto received = pair.server.Receive(ping); !received) {
      failed = true;
      break;
    }
  }
  ASSERT_TRUE(failed);
  EXPECT_EQ(pair.server.GetState(), H2ServerSession::State::kFailed);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_FALSE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation, {}));
}

TEST(H2MemoryUnary, HttpFallbackStatusPrecedence) {
  struct Case {
    int http;
    int grpc;
    int expected;
  };
  for (const auto item :
       std::array{Case{401, -1, 16}, Case{403, -1, 7}, Case{404, -1, 12}, Case{429, -1, 14},
                  Case{502, -1, 14}, Case{503, -1, 14}, Case{504, -1, 14}, Case{201, -1, 13},
                  Case{503, 7, 14}, Case{503, 12, 14}}) {
    MemoryPair pair;
    ServerHandler handler{.server = &pair.server};
    struct Failure {
      H2ServerSession* server;
      int code;
    } failure{&pair.server, item.grpc};
    if (item.grpc < 0)
      pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    else
      pair.server.SetRequestHandler(
          &failure, [](void* opaque, std::int32_t stream, std::uint64_t gen, std::string_view,
                       std::span<const std::byte>) noexcept {
            auto& self = *static_cast<Failure*>(opaque);
            auto completed = self.server->Complete(stream, gen, {}, self.code);
            EXPECT_TRUE(completed);
          });
    HttpStatusMutation mutation{.status = item.http, .omit_grpc_status = item.grpc < 0};
    pair.mutation_context = &mutation;
    pair.mutate = &HttpStatusMutation::Mutate;
    Result result;
    ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
    ASSERT_TRUE(pair.Drive());
    ASSERT_TRUE(mutation.changed);
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status, item.expected) << item.http << "/" << item.grpc;
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
}

TEST(H2MemoryUnary, CodecTypesAndHeaderOwnership) {
  for (auto type : {"application/grpc+proto", "application/grpc+fory", "application/grpc+capnp"}) {
    std::string temporary_type(type);
    MemoryPair pair(temporary_type);
    pair.chunk = 1;
    ServerHandler handler{.server = &pair.server};
    pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    Result result;
    const std::array message{std::byte{0x73}};
    ASSERT_TRUE(
        pair.client.Submit("/rpc.Test/Echo", message, &result, &Result::Complete, temporary_type));
    temporary_type.assign(temporary_type.size(), 'x');
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status, 0);
    EXPECT_EQ(handler.calls, 1);
    EXPECT_EQ(result.payload.size(), 1);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
}

TEST(H2MemoryUnary, InvalidCodecAvoidsStreamAllocation) {
  MemoryPair pair;
  Result result;
  for (const auto type : {"", "application/grpc+", "text/plain", "application/grpc+proto\r\nx: y",
                          "application/grpc+proto;bad"}) {
    H2ServerSession invalid(type);
    EXPECT_FALSE(invalid.Ready());
    auto submitted = pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete, type);
    ASSERT_FALSE(submitted);
    EXPECT_EQ(submitted.error().Code(), grpcxx::StatusCode::kInvalidArgument);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
  }
  const auto oversized = std::string("application/grpc+") + std::string(128, 'x');
  EXPECT_FALSE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete, oversized));
  EXPECT_EQ(result.calls, 0);
}

TEST(H2MemoryUnary, GenericSessionsKeepBaseMimeCompatibility) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 0);
  EXPECT_EQ(handler.calls, 1);
}

TEST(H2MemoryUnary, SessionRecyclesAcrossManySequentialRpcs) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  const std::array payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44}};
  for (int round = 0; round < 64; ++round) {
    Result result;
    ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", payload, &result, &Result::Complete));
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status, 0);
    EXPECT_TRUE(std::ranges::equal(result.payload, payload));
    EXPECT_EQ(pair.client.ActiveStreams(), 0U);
    EXPECT_EQ(pair.server.ActiveStreams(), 0U);
  }
  EXPECT_EQ(handler.calls, 64);
}
}  // namespace
