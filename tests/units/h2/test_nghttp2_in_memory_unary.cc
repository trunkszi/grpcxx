#include <fmt/format.h>
#include <grpcxx/h2/nghttp2_client_session.h>
#include <grpcxx/h2/nghttp2_server_session.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <type_traits>
#include <vector>

namespace {
using grpcxx::Nghttp2ClientSession;
using grpcxx::Nghttp2ServerSession;
struct Result {
  int calls{};
  int status{-1};
  Nghttp2ClientSession* client{};
  int recursion_rejected{};
  std::vector<std::byte> payload;
  static void Complete(void* context, std::int32_t, int code,
                       std::span<const std::byte> bytes) noexcept {
    auto& self = *static_cast<Result*>(context);
    ++self.calls;
    self.status = code;
    self.payload.assign_range(bytes);
    if (self.client != nullptr) {
      auto send = self.client->Send();
      auto receive = self.client->Receive({});
      if (!send && send.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
      if (!receive && receive.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
    }
  }
};

/** Both directions use native HTTP/2 serialization; transport segmentation is controlled here. */
class MemoryPair {
 public:
  explicit MemoryPair(std::string_view content_type = "application/grpc") : server(content_type) {}
  Nghttp2ClientSession client;
  Nghttp2ServerSession server;
  std::size_t chunk{std::numeric_limits<std::size_t>::max()};
  void* mutation_context{};
  void (*mutate)(void*, bool, std::vector<std::byte>&){};
  bool Drive() {
    for (auto tick : std::views::iota(0, 256)) {
      static_cast<void>(tick);
      auto outbound = Transfer(client, server);
      if (!outbound) return false;
      auto inbound = Transfer(server, client);
      if (!inbound) return false;
      if (!*outbound && !*inbound) return true;
    }
    ADD_FAILURE() << "HTTP/2 session did not quiesce";
    return false;
  }

 private:
  template <typename From, typename To>
  std::optional<bool> Transfer(From& from, To& to) {
    auto output = from.Send();
    if (!output) {
      ADD_FAILURE() << fmt::format("native serialization failed: {}",
                                   static_cast<int>(output.error().Code()));
      return std::nullopt;
    }
    if (output->empty()) return false;
    std::vector<std::byte> stable(std::from_range, *output);
    if (mutate != nullptr)
      mutate(mutation_context, std::is_same_v<From, Nghttp2ClientSession>, stable);
    for (std::size_t offset = 0; offset < stable.size();) {
      const auto length = std::min(chunk, stable.size() - offset);
      auto input = std::span<const std::byte>(stable).subspan(offset, length);
      auto consumed = to.Receive(input);
      if (!consumed || *consumed != input.size()) {
        ADD_FAILURE() << "native input did not consume full fragment";
        return std::nullopt;
      }
      offset += length;
    }
    return true;
  }
};

struct ServerHandler {
  Nghttp2ServerSession* server{};
  int calls{};
  int failures{};
  bool deferred{};
  bool probe_recursion{};
  int recursion_rejected{};
  std::int32_t stream{};
  std::uint64_t generation{};
  std::vector<std::byte> pending{};
  static void Invoke(void* context, std::int32_t id, std::uint64_t gen, std::string_view path,
                     std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<ServerHandler*>(context);
    ++self.calls;
    self.stream = id;
    self.generation = gen;
    if (self.probe_recursion) {
      auto send = self.server->Send();
      auto receive = self.server->Receive({});
      if (!send && send.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
      if (!receive && receive.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
    }
    if (self.deferred) {
      self.pending.assign_range(payload);
      return;
    }
    const int code = path == "/test.Order/Echo" ? 0 : 12;
    auto completed =
        self.server->Complete(id, gen, code == 0 ? payload : std::span<const std::byte>{}, code);
    if (!completed) ++self.failures;
  }
};

TEST(Nghttp2MemoryUnary, ImmediateRoundtripAndFragmentedData) {
  for (const auto chunk : std::array<std::size_t, 4>{1, 3, 17, 65536}) {
    MemoryPair pair;
    pair.chunk = chunk;
    ASSERT_TRUE(pair.client.Ready());
    ASSERT_TRUE(pair.server.Ready());
    ServerHandler handler{.server = &pair.server};
    pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    std::vector<std::byte> message(32768, std::byte{0x65});
    Result result;
    ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", message, &result, &Result::Complete));
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

TEST(Nghttp2MemoryUnary, DeferredReplyRequiresLaterCallerTickAndRejectsStaleGeneration) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  const std::array message{std::byte{0x42}, std::byte{0x19}};
  Result result;
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", message, &result, &Result::Complete));
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

TEST(Nghttp2MemoryUnary, UnknownPathReturnsTrailersOnlyUnimplemented) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/test.Order/Missing", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(handler.calls, 1);
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 12);
  EXPECT_TRUE(result.payload.empty());
}

TEST(Nghttp2MemoryUnary, PingAndGoAwayDrainWithoutNewStreams) {
  MemoryPair pair;
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(pair.client.Ping());
  ASSERT_TRUE(pair.server.Ping());
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(pair.client.RejectedTransitions(), 0);
  EXPECT_EQ(pair.server.RejectedTransitions(), 0);
  ASSERT_TRUE(pair.server.GoAway());
  ASSERT_TRUE(pair.Drive());
  EXPECT_NE(pair.client.GetState(), Nghttp2ClientSession::State::kOpen);
  Result result;
  EXPECT_FALSE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete));
  EXPECT_EQ(result.calls, 0);
}

TEST(Nghttp2MemoryUnary, PeerResetInvalidatesDeferredGeneration) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  auto stream = pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete);
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
TEST(Nghttp2MemoryUnary, ClientCancellationDetachesCompletionAndInvalidatesServerHandle) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  auto stream = pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete);
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

TEST(Nghttp2MemoryUnary, RecursivePumpIsRejectedWithoutLosingCompletion) {
  MemoryPair pair;
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  ServerHandler handler{.server = &pair.server, .probe_recursion = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  result.client = &pair.client;
  const std::array message{std::byte{42}};
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", message, &result, &Result::Complete));
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

TEST(Nghttp2MemoryUnary, ConcurrentCapacityFlowControlMaximumPayloadAndSlotReuse) {
  MemoryPair pair;
  pair.chunk = 257;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  std::array<Result, Nghttp2ClientSession::kMaxStreams> results;
  std::vector<std::byte> maximum(Nghttp2ClientSession::kMaxMessageBytes, std::byte{0x5a});
  const std::array small{std::byte{1}, std::byte{2}, std::byte{3}};
  for (auto index : std::views::iota(std::size_t{0}, results.size())) {
    const auto payload =
        index == 0 ? std::span<const std::byte>{maximum} : std::span<const std::byte>{small};
    ASSERT_TRUE(
        pair.client.Submit("/test.Order/Echo", payload, &results[index], &Result::Complete));
  }
  EXPECT_EQ(pair.client.ActiveStreams(), results.size());
  Result excess;
  auto rejected = pair.client.Submit("/test.Order/Echo", {}, &excess, &Result::Complete);
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
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", {}, &excess, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(excess.calls, 1);
  EXPECT_EQ(excess.status, 0);
  EXPECT_TRUE(excess.payload.empty());
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

/** Extends one native DATA sequence past the application cap without invalidating HTTP/2 framing.
 */
struct ExcessData {
  std::int32_t stream{};
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<ExcessData*>(opaque);
    if (!from_client || self.changed || wire.size() < 9 || wire[3] != std::byte{0}) return;
    std::uint32_t id{};
    for (auto byte : std::span{wire}.subspan(5, 4))
      id = (id << 8) | std::to_integer<std::uint32_t>(byte);
    if (id != static_cast<std::uint32_t>(self.stream) || (wire[4] & std::byte{1}) == std::byte{0})
      return;
    wire[4] &= std::byte{0xfe};
    const std::array extra{std::byte{0}, std::byte{0}, std::byte{1}, std::byte{0}, std::byte{1},
                           wire[5],      wire[6],      wire[7],      wire[8],      std::byte{0x55}};
    wire.append_range(extra);
    self.changed = true;
  }
};

TEST(Nghttp2MemoryUnary, OversizedPeerStreamDoesNotAbortIndependentRequest) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result bad;
  Result good;
  std::vector<std::byte> maximum(Nghttp2ClientSession::kMaxMessageBytes, std::byte{0x5a});
  const std::array normal{std::byte{9}, std::byte{7}};
  auto first = pair.client.Submit("/test.Order/Echo", maximum, &bad, &Result::Complete);
  ASSERT_TRUE(first);
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", normal, &good, &Result::Complete));
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

/** Adds grpc-status to nonterminal initial headers without changing the HPACK dynamic table. */
struct PrematureStatus {
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<PrematureStatus*>(opaque);
    if (from_client || self.changed || wire.size() < 9 || wire[3] != std::byte{1} ||
        (wire[4] & std::byte{1}) != std::byte{0})
      return;
    const std::string_view name = "grpc-status";
    wire.push_back(std::byte{0});
    wire.push_back(std::byte{11});
    for (const auto character : name) wire.push_back(static_cast<std::byte>(character));
    wire.push_back(std::byte{1});
    wire.push_back(std::byte{'0'});
    const auto length = wire.size() - 9;
    wire[0] = std::byte{static_cast<unsigned char>((length >> 16) & 0xff)};
    wire[1] = std::byte{static_cast<unsigned char>((length >> 8) & 0xff)};
    wire[2] = std::byte{static_cast<unsigned char>(length & 0xff)};
    self.changed = true;
  }
};

TEST(Nghttp2MemoryUnary, GrpcStatusInNonterminalInitialHeadersIsRejected) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  PrematureStatus mutation;
  pair.mutation_context = &mutation;
  pair.mutate = &PrematureStatus::Mutate;
  Result result;
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete));
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

TEST(Nghttp2MemoryUnary, FatalPeerFloodInvalidatesEveryDeferredServerHandle) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(pair.server.IsCurrent(handler.stream, handler.generation));
  const std::array ping{std::byte{0}, std::byte{0}, std::byte{8}, std::byte{6}, std::byte{0},
                        std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
                        std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
                        std::byte{0}, std::byte{0}};
  bool failed = false;
  for (auto attempt : std::views::iota(0, 10000)) {
    static_cast<void>(attempt);
    if (auto received = pair.server.Receive(ping); !received) {
      failed = true;
      break;
    }
  }
  ASSERT_TRUE(failed);
  EXPECT_EQ(pair.server.GetState(), Nghttp2ServerSession::State::kFailed);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  EXPECT_FALSE(pair.server.IsCurrent(handler.stream, handler.generation));
  EXPECT_FALSE(pair.server.Complete(handler.stream, handler.generation, {}));
}

struct HttpStatusMutation {
  int status{};
  bool omit_grpc_status{true};
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<HttpStatusMutation*>(opaque);
    if (from_client || wire.size() < 9) return;
    if (wire[3] == std::byte{1} && !self.changed) {
      if (wire.size() < 10 || wire[9] != std::byte{0x88}) return;
      const std::array literal{std::byte{8}, std::byte{3},
                               static_cast<std::byte>('0' + self.status / 100),
                               static_cast<std::byte>('0' + (self.status / 10) % 10),
                               static_cast<std::byte>('0' + self.status % 10)};
      std::vector<std::byte> replacement;
      replacement.append_range(std::span{wire}.first(9));
      replacement.append_range(literal);
      replacement.append_range(std::span{wire}.subspan(10));
      wire.swap(replacement);
      const auto length = wire.size() - 9;
      wire[0] = std::byte{static_cast<unsigned char>((length >> 16) & 0xff)};
      wire[1] = std::byte{static_cast<unsigned char>((length >> 8) & 0xff)};
      wire[2] = std::byte{static_cast<unsigned char>(length & 0xff)};
      self.changed = true;
    } else if (self.omit_grpc_status && wire[3] == std::byte{1}) {
      wire.clear();
    }
    if (self.omit_grpc_status && !wire.empty() && wire[3] == std::byte{0}) wire[4] |= std::byte{1};
  }
};

TEST(Nghttp2MemoryUnary, HttpFallbackStatusAndExplicitGrpcFailurePrecedence) {
  struct Case {
    int http;
    int grpc;
    int expected;
  };
  for (const auto item :
       std::array{Case{401, -1, 16}, Case{403, -1, 7}, Case{404, -1, 12}, Case{429, -1, 14},
                  Case{502, -1, 14}, Case{503, -1, 14}, Case{504, -1, 14}, Case{201, -1, 13},
                  Case{503, 7, 7}, Case{503, 12, 12}}) {
    MemoryPair pair;
    ServerHandler handler{.server = &pair.server};
    struct Failure {
      Nghttp2ServerSession* server;
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
    ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete));
    ASSERT_TRUE(pair.Drive());
    ASSERT_TRUE(mutation.changed);
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status, item.expected) << item.http << "/" << item.grpc;
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
}

TEST(Nghttp2MemoryUnary, ConcreteCodecTypesMatchAndHeaderStorageIsOwned) {
  for (auto type : {"application/grpc+proto", "application/grpc+fory", "application/grpc+capnp"}) {
    std::string temporary_type(type);
    MemoryPair pair(temporary_type);
    pair.chunk = 1;
    ServerHandler handler{.server = &pair.server};
    pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    Result result;
    const std::array message{std::byte{0x73}};
    ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", message, &result, &Result::Complete,
                                   temporary_type));
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

TEST(Nghttp2MemoryUnary, InvalidCodecTypesFailBeforeAllocatingStreams) {
  MemoryPair pair;
  Result result;
  for (const auto type : {"", "application/grpc+", "text/plain", "application/grpc+proto\r\nx: y",
                          "application/grpc+proto;bad"}) {
    Nghttp2ServerSession invalid(type);
    EXPECT_FALSE(invalid.Ready());
    auto submitted = pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete, type);
    ASSERT_FALSE(submitted);
    EXPECT_EQ(submitted.error().Code(), grpcxx::StatusCode::kInvalidArgument);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
  }
  const auto oversized = std::string("application/grpc+") + std::string(128, 'x');
  EXPECT_FALSE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete, oversized));
  EXPECT_EQ(result.calls, 0);
}

TEST(Nghttp2MemoryUnary, GenericSessionsKeepBaseMimeCompatibility) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/test.Order/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 0);
  EXPECT_EQ(handler.calls, 1);
}

}  // namespace
