#include <nexora/http2/connection.hpp>

#include "session_fixture.h"

namespace {

void AppendFrameHeader(std::vector<std::byte>& out, std::uint32_t len, std::uint8_t type,
                       std::uint8_t flags, std::uint32_t id) {
  const std::array<std::byte, 9> hdr{
      static_cast<std::byte>((len >> 16U) & 0xffU),
      static_cast<std::byte>((len >> 8U) & 0xffU),
      static_cast<std::byte>(len & 0xffU),
      static_cast<std::byte>(type),
      static_cast<std::byte>(flags),
      static_cast<std::byte>((id >> 24U) & 0x7fU),
      static_cast<std::byte>((id >> 16U) & 0xffU),
      static_cast<std::byte>((id >> 8U) & 0xffU),
      static_cast<std::byte>(id & 0xffU),
  };
  out.append_range(hdr);
}

std::span<const std::byte> AsBytes(std::span<const std::uint8_t> bytes) noexcept {
  return std::as_bytes(bytes);
}

std::span<const std::uint8_t> AsU8(std::span<const std::byte> bytes) noexcept {
  return {std::bit_cast<const std::uint8_t*>(bytes.data()), bytes.size()};
}

template <typename Session>
bool PumpToPeer(Session& from, nexora::Http2Connection& to) {
  auto out = from.Send();
  if (!out) return false;
  if (out->empty()) return true;
  std::vector<std::uint8_t> copy(AsU8(*out).begin(), AsU8(*out).end());
  return to.FeedBytes(copy).Complete();
}

template <typename Session>
bool PumpFromPeer(nexora::Http2Connection& from, Session& to) {
  auto pending = from.PendingOutput();
  if (pending.empty()) return true;
  std::vector<std::uint8_t> copy(pending.begin(), pending.end());
  from.ConsumeOutput(copy.size());
  auto received = to.Receive(AsBytes(copy));
  return received.has_value() && *received == copy.size();
}

TEST(H2ProtocolSecurity, CancelledStreamsReplenishWindow) {
  MemoryPair pair;
  ServerHandler deferred{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&deferred, &ServerHandler::Invoke);

  // Four 48 KiB cancelled requests + padded DATA frames exceed the initial 65,535 B window.
  for (int round = 0; round < 4; ++round) {
    Result ignored;
    std::vector<std::byte> chunk(48uz * 1024uz, std::byte{0x21});
    auto id = pair.client.Submit("/rpc.Test/Echo", chunk, &ignored, &Result::Complete);
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(pair.Drive());
    ASSERT_TRUE(pair.client.Cancel(*id).has_value());
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(ignored.calls, 0);
  }

  // Also verify padded DATA frames on a server stream replenish the full wire frame length.
  struct NoopStream {
    static void OnMsg(void*, std::int32_t, std::span<const std::byte>) noexcept {}
    static void OnDone(void*, std::int32_t, int, std::span<const std::byte>) noexcept {}
  };
  auto padded_sid =
      pair.client.Open("/rpc.Test/Echo", nullptr, &NoopStream::OnMsg, &NoopStream::OnDone);
  ASSERT_TRUE(padded_sid.has_value());
  ASSERT_TRUE(pair.Drive());
  const auto pid = static_cast<std::uint32_t>(*padded_sid);
  std::vector<std::byte> padded_frame;
  AppendFrameHeader(padded_frame, 46U, 0U, 0x09U, pid);
  padded_frame.push_back(std::byte{40});
  padded_frame.resize(9 + 46, std::byte{0});
  ASSERT_TRUE(pair.server.Receive(padded_frame).has_value());
  ASSERT_TRUE(pair.client.Cancel(*padded_sid).has_value());
  ASSERT_TRUE(pair.Drive());

  ServerHandler echo{.server = &pair.server, .deferred = false};
  pair.server.SetRequestHandler(&echo, &ServerHandler::Invoke);
  Result final_res;
  std::vector<std::byte> big_payload(256uz * 1024uz, std::byte{0x6d});
  ASSERT_TRUE(
      pair.client.Submit("/rpc.Test/Echo", big_payload, &final_res, &Result::Complete).has_value());
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(final_res.calls, 1);
  EXPECT_EQ(final_res.status, 0);
  EXPECT_EQ(final_res.payload, big_payload);
}

TEST(H2ProtocolSecurity, TimeoutStatusAndReentrantCancel) {
  MemoryPair pair;
  ASSERT_TRUE(pair.client.SetTarget("https", "rpc.internal:8443").has_value());
  pair.client.SetDefaultTimeoutNs(250'000'000ULL);

  struct InspectHandler {
    H2ServerSession* srv{};
    std::string scheme{};
    std::string authority{};
    std::uint64_t timeout_ns{};
    static void Invoke(void* ctx, std::int32_t id, std::uint64_t gen, std::string_view,
                       std::span<const std::byte>) noexcept {
      auto& self = *static_cast<InspectHandler*>(ctx);
      self.scheme = std::string(self.srv->StreamScheme(id));
      self.authority = std::string(self.srv->StreamAuthority(id));
      self.timeout_ns = self.srv->StreamTimeoutNs(id);
      static_cast<void>(self.srv->Complete(id, gen, {}, 8, {}, "rate limit: 100% full"));
    }
  } inspect{.srv = &pair.server};
  pair.server.SetRequestHandler(&inspect, &InspectHandler::Invoke);

  Result res;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &res, &Result::Complete).has_value());
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(inspect.scheme, "https");
  EXPECT_EQ(inspect.authority, "rpc.internal:8443");
  EXPECT_EQ(inspect.timeout_ns, 250'000'000ULL);
  EXPECT_EQ(res.status, 8);
  EXPECT_EQ(pair.client.LastStatusMessage(), "rate limit: 100% full");

  for (const auto [http_code, expected_grpc] :
       std::array<std::pair<int, int>, 2>{{{400, 13}, {200, 13}}}) {
    MemoryPair fallback_pair;
    ServerHandler handler{.server = &fallback_pair.server};
    fallback_pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
    HttpStatusMutation mut{.status = http_code, .omit_grpc_status = true};
    fallback_pair.mutation_context = &mut;
    fallback_pair.mutate = &HttpStatusMutation::Mutate;
    Result r;
    ASSERT_TRUE(
        fallback_pair.client.Submit("/rpc.Test/Echo", {}, &r, &Result::Complete).has_value());
    ASSERT_TRUE(fallback_pair.Drive());
    EXPECT_EQ(r.status, expected_grpc) << http_code;
  }

  // Verify RST_STREAM(ENHANCE_YOUR_CALM) maps to kResourceExhausted (8).
  {
    H2ClientSession calm_client;
    Result calm_res;
    auto cid = calm_client.Submit("/rpc.Test/Echo", {}, &calm_res, &Result::Complete);
    ASSERT_TRUE(cid.has_value());
    nexora::Http2Connection raw_srv(nexora::Http2Role::kServer);
    ASSERT_TRUE(raw_srv.InitiateHandshake().has_value());
    ASSERT_TRUE(PumpToPeer(calm_client, raw_srv));
    while (raw_srv.PeekEventView()) raw_srv.ConsumeEvent();
    ASSERT_TRUE(raw_srv
                    .SubmitRstStream(static_cast<std::uint32_t>(*cid),
                                     nexora::Http2ErrorCode::kEnhanceYourCalm)
                    .has_value());
    ASSERT_TRUE(PumpFromPeer(raw_srv, calm_client));
    EXPECT_EQ(calm_res.calls, 1);
    EXPECT_EQ(calm_res.status, 8);
  }

  // Reentrant Cancel(current) + Open(replacement) during multi-message DATA decode.
  MemoryPair reentrant_pair;
  struct ServerMulti {
    H2ServerSession* srv{};
    static void OnStream(void* ctx, std::int32_t id, std::uint64_t gen, std::string_view path,
                         std::span<const std::byte>, bool end) noexcept {
      if (!end) return;
      auto* s = static_cast<ServerMulti*>(ctx)->srv;
      const std::array<std::byte, 1> m1{std::byte{1}};
      const std::array<std::byte, 1> m2{std::byte{2}};
      static_cast<void>(s->SendMessage(id, gen, m1));
      if (path == "/rpc.Test/First") static_cast<void>(s->SendMessage(id, gen, m2));
      static_cast<void>(s->Finish(id, gen, 0));
    }
  } multi{.srv = &reentrant_pair.server};
  reentrant_pair.server.SetStreamingHandler(&multi, &ServerMulti::OnStream);

  struct ReentrantClient {
    H2ClientSession* cli{};
    std::int32_t first_id{};
    std::int32_t second_id{};
    int first_msgs{};
    int second_msgs{};
    int second_status{-1};
    static void OnFirstMsg(void* ctx, std::int32_t id, std::span<const std::byte>) noexcept {
      auto& self = *static_cast<ReentrantClient*>(ctx);
      ++self.first_msgs;
      static_cast<void>(self.cli->Cancel(id));
      auto next = self.cli->Open("/rpc.Test/Second", &self, &OnSecondMsg, &OnSecondDone);
      if (next) {
        self.second_id = *next;
        static_cast<void>(self.cli->HalfClose(*next));
      }
    }
    static void OnFirstDone(void*, std::int32_t, int, std::span<const std::byte>) noexcept {
      ADD_FAILURE() << "cancelled stream must not invoke completion";
    }
    static void OnSecondMsg(void* ctx, std::int32_t, std::span<const std::byte>) noexcept {
      ++static_cast<ReentrantClient*>(ctx)->second_msgs;
    }
    static void OnSecondDone(void* ctx, std::int32_t, int st, std::span<const std::byte>) noexcept {
      static_cast<ReentrantClient*>(ctx)->second_status = st;
    }
  } rc{.cli = &reentrant_pair.client};

  auto first = reentrant_pair.client.Open("/rpc.Test/First", &rc, &ReentrantClient::OnFirstMsg,
                                          &ReentrantClient::OnFirstDone);
  ASSERT_TRUE(first.has_value());
  rc.first_id = *first;
  ASSERT_TRUE(reentrant_pair.client.HalfClose(*first).has_value());
  ASSERT_TRUE(reentrant_pair.Drive());
  EXPECT_EQ(rc.first_msgs, 1);
  EXPECT_GT(rc.second_id, 0);
  EXPECT_EQ(rc.second_msgs, 1);
  EXPECT_EQ(rc.second_status, 0);
  EXPECT_EQ(reentrant_pair.client.ActiveStreams(), 0U);
}

TEST(H2Status, HttpFailureOverridesGrpcSuccess) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  HttpStatusMutation mutation{.status = 503, .omit_grpc_status = false};
  pair.mutation_context = &mutation;
  pair.mutate = &HttpStatusMutation::Mutate;
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(mutation.changed);
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.status, 14);
}

TEST(H2Eof, CancelsIncompleteStreamingIngress) {
  MemoryPair pair;
  struct Observer {
    H2ServerSession* server;
    int cancelled{};
    static void Handle(void* ctx, std::int32_t id, std::uint64_t generation, std::string_view path,
                       std::span<const std::byte>, bool end) noexcept {
      auto& self = *static_cast<Observer*>(ctx);
      if (end && !self.server->IsCurrent(id, generation)) {
        ++self.cancelled;
        EXPECT_EQ(path, "/rpc.Test/Echo");
      }
    }
  } observer{&pair.server};
  pair.server.SetStreamingHandler(&observer, &Observer::Handle);
  Result result;
  ASSERT_TRUE(pair.client.Open("/rpc.Test/Echo", &result, nullptr, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_EQ(pair.server.ActiveStreams(), 1U);
  ASSERT_TRUE(pair.server.OnPeerEof());
  EXPECT_EQ(observer.cancelled, 1);
  EXPECT_EQ(pair.server.ActiveStreams(), 0U);
  ASSERT_TRUE(pair.server.OnPeerEof());
  EXPECT_EQ(observer.cancelled, 1);
}

TEST(H2Eof, PreservesCompletedDeferredRequest) {
  MemoryPair pair;
  ServerHandler handler{.server = &pair.server, .deferred = true};
  pair.server.SetRequestHandler(&handler, &ServerHandler::Invoke);
  Result result;
  ASSERT_TRUE(pair.client.Submit("/rpc.Test/Echo", {}, &result, &Result::Complete));
  ASSERT_TRUE(pair.Drive());
  ASSERT_EQ(handler.calls, 1);
  ASSERT_TRUE(pair.server.OnPeerEof());
  EXPECT_TRUE(pair.server.IsCurrent(handler.stream, handler.generation));
  ASSERT_TRUE(pair.server.Complete(handler.stream, handler.generation, {}));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.status, 0);
  EXPECT_EQ(result.calls, 1);
}

}  // namespace
