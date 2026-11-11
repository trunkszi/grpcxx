#include <nexora/http2/connection.hpp>

#include "session_fixture.h"

namespace {

std::vector<std::uint8_t> FrameGrpcMessage(std::span<const std::uint8_t> body) {
  std::vector<std::uint8_t> out(5 + body.size());
  out[0] = 0U;
  const auto len = static_cast<std::uint32_t>(body.size());
  out[1] = static_cast<std::uint8_t>((len >> 24U) & 0xffU);
  out[2] = static_cast<std::uint8_t>((len >> 16U) & 0xffU);
  out[3] = static_cast<std::uint8_t>((len >> 8U) & 0xffU);
  out[4] = static_cast<std::uint8_t>(len & 0xffU);
  if (!body.empty()) std::ranges::copy(body, out.begin() + 5);
  return out;
}

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

std::optional<std::uint32_t> ExtractMaxConcurrentStreams(std::span<const std::byte> wire) {
  std::size_t pos = 0;
  while (pos + 9 <= wire.size()) {
    const std::size_t len = (std::to_integer<std::size_t>(wire[pos]) << 16U) |
                            (std::to_integer<std::size_t>(wire[pos + 1]) << 8U) |
                            std::to_integer<std::size_t>(wire[pos + 2]);
    if (pos + 9 + len > wire.size()) break;
    if (wire[pos + 3] == std::byte{0x04} && (wire[pos + 4] & std::byte{0x01}) == std::byte{0}) {
      for (std::size_t off = pos + 9; off + 6 <= pos + 9 + len; off += 6) {
        const std::uint16_t id = (std::to_integer<std::uint16_t>(wire[off]) << 8U) |
                                 std::to_integer<std::uint16_t>(wire[off + 1]);
        if (id == 0x03U) {
          return (std::to_integer<std::uint32_t>(wire[off + 2]) << 24U) |
                 (std::to_integer<std::uint32_t>(wire[off + 3]) << 16U) |
                 (std::to_integer<std::uint32_t>(wire[off + 4]) << 8U) |
                 std::to_integer<std::uint32_t>(wire[off + 5]);
        }
      }
    }
    pos += 9 + len;
  }
  return std::nullopt;
}

TEST(H2ProtocolSecurity, ExcessStreamPreservesActiveCalls) {
  H2ServerSession server;
  struct DeferredBatch {
    struct Item {
      std::int32_t id{};
      std::uint64_t gen{};
    };
    std::vector<Item> items;
    static void Invoke(void* ctx, std::int32_t id, std::uint64_t gen, std::string_view,
                       std::span<const std::byte>) noexcept {
      static_cast<DeferredBatch*>(ctx)->items.push_back({id, gen});
    }
  } batch;
  server.SetRequestHandler(&batch, &DeferredBatch::Invoke);

  auto server_preface = server.Send();
  ASSERT_TRUE(server_preface);
  std::vector<std::byte> preface_bytes(server_preface->begin(), server_preface->end());
  EXPECT_EQ(ExtractMaxConcurrentStreams(preface_bytes), 16U);

  nexora::Http2Limits limits{.coalesce_message_body = false};
  nexora::Http2Connection peer(nexora::Http2Role::kClient, {}, limits);
  ASSERT_TRUE(peer.InitiateHandshake().has_value());

  nexora::Headers<> req_headers;
  ASSERT_TRUE(req_headers.Append("content-type", "application/grpc").has_value());
  ASSERT_TRUE(req_headers.Append("te", "trailers").has_value());
  const auto empty_msg = FrameGrpcMessage({});

  std::vector<std::uint32_t> opened;
  for (int i = 0; i < 17; ++i) {
    auto sid = peer.OpenStream();
    ASSERT_TRUE(sid.has_value());
    opened.push_back(*sid);
    ASSERT_TRUE(peer.SubmitRequestHeaders(*sid, nexora::Method::kPost, "http", "127.0.0.1:50051",
                                          "/rpc.Test/Echo", req_headers, false)
                    .has_value());
    auto sent = peer.SubmitData(*sid, empty_msg, true);
    ASSERT_TRUE(sent.has_value());
    EXPECT_EQ(*sent, empty_msg.size());
  }

  ASSERT_TRUE(peer.FeedBytes(AsU8(preface_bytes)).Complete());
  while (peer.PeekEventView()) peer.ConsumeEvent();
  ASSERT_TRUE(PumpFromPeer(peer, server));
  EXPECT_TRUE(server.Ready());
  EXPECT_EQ(server.ActiveStreams(), 16U);
  ASSERT_EQ(batch.items.size(), 16U);

  ASSERT_TRUE(PumpToPeer(server, peer));
  bool saw_refused = false;
  while (auto ev = peer.PollEvent()) {
    if (ev->kind == nexora::Http2Event::Kind::kStreamReset && ev->stream_id == opened.back() &&
        ev->error_code == nexora::Http2ErrorCode::kRefusedStream) {
      saw_refused = true;
    }
  }
  EXPECT_TRUE(saw_refused);

  for (const auto& item : batch.items) {
    ASSERT_TRUE(server.Complete(item.id, item.gen, {}).has_value());
  }
  ASSERT_TRUE(PumpToPeer(server, peer));
  std::size_t ok_streams = 0;
  while (auto ev = peer.PollEvent()) {
    if (ev->kind == nexora::Http2Event::Kind::kMessageComplete) ++ok_streams;
  }
  EXPECT_EQ(ok_streams, 16U);
  EXPECT_EQ(server.ActiveStreams(), 0U);
  EXPECT_TRUE(server.Ready());

  H2ClientSession client;
  std::array<Result, 9> results{};
  std::array<std::int32_t, 9> client_ids{};
  for (std::size_t i = 0; i < 9; ++i) {
    auto id = client.Submit("/rpc.Test/Echo", {}, &results[i], &Result::Complete);
    ASSERT_TRUE(id.has_value());
    client_ids[i] = *id;
  }
  nexora::Http2Settings srv_settings;
  srv_settings.max_concurrent_streams = 8U;
  nexora::Http2Connection raw_server(nexora::Http2Role::kServer, srv_settings, limits);
  ASSERT_TRUE(raw_server.InitiateHandshake().has_value());
  ASSERT_TRUE(PumpToPeer(client, raw_server));
  while (raw_server.PeekEventView()) raw_server.ConsumeEvent();

  nexora::Headers<> resp_h;
  ASSERT_TRUE(resp_h.Append("content-type", "application/grpc").has_value());
  nexora::Headers<> trailers_ok;
  ASSERT_TRUE(trailers_ok.Append("grpc-status", "0").has_value());
  for (std::size_t i = 0; i < 8; ++i) {
    const auto sid = static_cast<std::uint32_t>(client_ids[i]);
    ASSERT_TRUE(raw_server.SubmitResponseHeaders(sid, 200, resp_h, false).has_value());
    ASSERT_TRUE(raw_server.SubmitData(sid, empty_msg, false).has_value());
    ASSERT_TRUE(raw_server.SubmitTrailers(sid, trailers_ok).has_value());
  }
  ASSERT_TRUE(PumpFromPeer(raw_server, client));
  for (std::size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(results[i].calls, 1);
    EXPECT_EQ(results[i].status, 0);
  }
  EXPECT_EQ(results[8].calls, 1);
  EXPECT_EQ(results[8].status, 14);
  EXPECT_EQ(client.ActiveStreams(), 0U);
}

TEST(H2ProtocolSecurity, FrameBurstDrainsSafely) {
  MemoryPair pair;
  struct StreamCapture {
    H2ServerSession* server{};
    std::int32_t id{};
    std::uint64_t gen{};
    int messages{};
    bool ended{};
    static void OnServer(void* ctx, std::int32_t sid, std::uint64_t gen, std::string_view,
                         std::span<const std::byte> body, bool end) noexcept {
      auto& self = *static_cast<StreamCapture*>(ctx);
      self.id = sid;
      self.gen = gen;
      if (end)
        self.ended = true;
      else if (body.size() == 1 && body[0] == std::byte{0x42})
        ++self.messages;
    }
  } server_cap{.server = &pair.server};
  pair.server.SetStreamingHandler(&server_cap, &StreamCapture::OnServer);

  struct ClientCapture {
    int messages{};
    int status{-1};
    bool done{};
    static void OnMsg(void* ctx, std::int32_t, std::span<const std::byte> body) noexcept {
      if (body.size() == 1 && body[0] == std::byte{0x7a})
        ++static_cast<ClientCapture*>(ctx)->messages;
    }
    static void OnDone(void* ctx, std::int32_t, int status, std::span<const std::byte>) noexcept {
      auto& self = *static_cast<ClientCapture*>(ctx);
      self.status = status;
      self.done = true;
    }
  } client_cap;

  auto sid = pair.client.Open("/rpc.Test/Bidi", &client_cap, &ClientCapture::OnMsg,
                              &ClientCapture::OnDone);
  ASSERT_TRUE(sid.has_value());
  ASSERT_TRUE(pair.Drive());

  const std::array<std::uint8_t, 1> req_byte{0x42U};
  const auto framed_req = FrameGrpcMessage(req_byte);
  std::vector<std::byte> client_burst;
  const auto id = static_cast<std::uint32_t>(*sid);
  for (int msg = 0; msg < 20; ++msg) {
    for (std::size_t b = 0; b < framed_req.size(); ++b) {
      const bool last = (msg == 19 && b + 1 == framed_req.size());
      AppendFrameHeader(client_burst, 1U, 0U, last ? 0x01U : 0x00U, id);
      client_burst.push_back(static_cast<std::byte>(framed_req[b]));
    }
  }
  auto srv_consumed = pair.server.Receive(client_burst);
  ASSERT_TRUE(srv_consumed.has_value());
  EXPECT_EQ(*srv_consumed, client_burst.size());
  EXPECT_EQ(server_cap.messages, 20);
  EXPECT_TRUE(server_cap.ended);
  EXPECT_TRUE(pair.server.Ready());

  const std::array<std::byte, 1> resp_byte{std::byte{0x7a}};
  ASSERT_TRUE(pair.server.SendMessage(server_cap.id, server_cap.gen, resp_byte).has_value());
  auto initial_resp = pair.server.Send();
  ASSERT_TRUE(initial_resp.has_value());
  std::vector<std::byte> initial_copy(initial_resp->begin(), initial_resp->end());
  ASSERT_TRUE(pair.client.Receive(initial_copy).has_value());
  EXPECT_EQ(client_cap.messages, 1);

  const std::array<std::uint8_t, 1> raw_resp_byte{0x7aU};
  const auto framed_resp = FrameGrpcMessage(raw_resp_byte);
  std::vector<std::byte> server_burst;
  for (int msg = 0; msg < 24; ++msg) {
    for (std::size_t b = 0; b < framed_resp.size(); ++b) {
      AppendFrameHeader(server_burst, 1U, 0U, 0U, id);
      server_burst.push_back(static_cast<std::byte>(framed_resp[b]));
    }
  }
  auto cli_consumed = pair.client.Receive(server_burst);
  ASSERT_TRUE(cli_consumed.has_value());
  EXPECT_EQ(*cli_consumed, server_burst.size());
  EXPECT_EQ(client_cap.messages, 25);

  ASSERT_TRUE(pair.server.Finish(server_cap.id, server_cap.gen, 0).has_value());
  ASSERT_TRUE(pair.Drive());
  EXPECT_TRUE(client_cap.done);
  EXPECT_EQ(client_cap.status, 0);
}

TEST(H2ProtocolSecurity, BackpressurePreservesPingHeadroom) {
  MemoryPair pair;
  struct StreamSlot {
    std::int32_t id{};
    std::uint64_t gen{};
    bool ended{};
    static void OnServer(void* ctx, std::int32_t sid, std::uint64_t gen, std::string_view,
                         std::span<const std::byte>, bool end) noexcept {
      auto& self = *static_cast<StreamSlot*>(ctx);
      self.id = sid;
      self.gen = gen;
      if (end) self.ended = true;
    }
  } slot;
  pair.server.SetStreamingHandler(&slot, &StreamSlot::OnServer);

  struct ClientRecv {
    std::vector<std::byte> payload;
    int status{-1};
    bool done{};
    static void OnMsg(void* ctx, std::int32_t, std::span<const std::byte> body) noexcept {
      static_cast<ClientRecv*>(ctx)->payload.assign_range(body);
    }
    static void OnDone(void* ctx, std::int32_t, int status, std::span<const std::byte>) noexcept {
      auto& self = *static_cast<ClientRecv*>(ctx);
      self.status = status;
      self.done = true;
    }
  } recv;

  auto sid =
      pair.client.Open("/rpc.Test/LargeStream", &recv, &ClientRecv::OnMsg, &ClientRecv::OnDone);
  ASSERT_TRUE(sid.has_value());
  ASSERT_TRUE(pair.client.HalfClose(*sid).has_value());
  ASSERT_TRUE(pair.Drive());
  ASSERT_TRUE(slot.ended);

  // Raise server's peer send windows so out_ buffer headroom—not RFC flow control—caps flushing.
  const std::array<std::byte, 26> win_boost{
      std::byte{0},
      std::byte{0},
      std::byte{4},
      std::byte{8},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{0x10},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{0},
      std::byte{4},
      std::byte{8},
      std::byte{0},
      static_cast<std::byte>((static_cast<std::uint32_t>(slot.id) >> 24U) & 0x7fU),
      static_cast<std::byte>((static_cast<std::uint32_t>(slot.id) >> 16U) & 0xffU),
      static_cast<std::byte>((static_cast<std::uint32_t>(slot.id) >> 8U) & 0xffU),
      static_cast<std::byte>(static_cast<std::uint32_t>(slot.id) & 0xffU),
      std::byte{0},
      std::byte{0x10},
      std::byte{0},
      std::byte{0},
  };
  ASSERT_TRUE(pair.server.Receive(win_boost).has_value());

  std::vector<std::byte> large(512uz * 1024uz, std::byte{0x3c});
  ASSERT_TRUE(pair.server.SendMessage(slot.id, slot.gen, large).has_value());
  ASSERT_TRUE(pair.server.Finish(slot.id, slot.gen, 0).has_value());
  EXPECT_TRUE(pair.server.HasPendingOutput());

  const std::array<std::byte, 17> ping_frame{
      std::byte{0}, std::byte{0}, std::byte{8}, std::byte{6}, std::byte{0}, std::byte{0},
      std::byte{0}, std::byte{0}, std::byte{0}, std::byte{1}, std::byte{2}, std::byte{3},
      std::byte{4}, std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8},
  };
  for (int i = 0; i < 32; ++i) {
    auto res = pair.server.Receive(ping_frame);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(*res, ping_frame.size());
    EXPECT_TRUE(pair.server.Ready());
  }

  ASSERT_TRUE(pair.Drive());
  EXPECT_TRUE(recv.done);
  EXPECT_EQ(recv.status, 0);
  EXPECT_EQ(recv.payload, large);
  EXPECT_TRUE(pair.server.Ready());
}

TEST(H2ProtocolSecurity, RejectedHeadersSuppressDataDispatch) {
  H2ServerSession server("application/grpc+proto");
  struct Guard {
    H2ServerSession* srv{};
    int unary_calls{};
    int stream_calls{};
    static void OnUnary(void* ctx, std::int32_t, std::uint64_t, std::string_view,
                        std::span<const std::byte>) noexcept {
      ++static_cast<Guard*>(ctx)->unary_calls;
    }
    static void OnStream(void* ctx, std::int32_t id, std::uint64_t gen, std::string_view,
                         std::span<const std::byte> body, bool end) noexcept {
      auto& self = *static_cast<Guard*>(ctx);
      ++self.stream_calls;
      if (!end) static_cast<void>(self.srv->SendMessage(id, gen, body));
      if (end) static_cast<void>(self.srv->Finish(id, gen, 0));
    }
  } guard{.srv = &server};
  server.SetRequestHandler(&guard, &Guard::OnUnary);
  server.SetStreamingHandler(&guard, &Guard::OnStream);

  nexora::Http2Limits limits{.coalesce_message_body = false};
  nexora::Http2Connection peer(nexora::Http2Role::kClient, {}, limits);
  ASSERT_TRUE(peer.InitiateHandshake().has_value());
  ASSERT_TRUE(PumpToPeer(server, peer));
  ASSERT_TRUE(PumpFromPeer(peer, server));
  while (peer.PeekEventView()) peer.ConsumeEvent();

  const std::array<std::uint8_t, 3> sample{0xaaU, 0xbbU, 0xccU};
  const auto framed = FrameGrpcMessage(sample);

  auto send_invalid = [&](nexora::Method method, std::string_view path,
                          std::string_view content_type, std::string_view timeout) {
    auto sid = peer.OpenStream();
    EXPECT_TRUE(sid.has_value());
    nexora::Headers<> h;
    if (!content_type.empty()) {
      EXPECT_TRUE(h.Append("content-type", content_type).has_value());
    }
    EXPECT_TRUE(h.Append("te", "trailers").has_value());
    if (!timeout.empty()) {
      EXPECT_TRUE(h.Append("grpc-timeout", timeout).has_value());
    }
    EXPECT_TRUE(peer.SubmitRequestHeaders(*sid, method, "http", "127.0.0.1:50051", path, h, false)
                    .has_value());
    EXPECT_TRUE(peer.SubmitData(*sid, framed, true).has_value());
    EXPECT_TRUE(PumpFromPeer(peer, server));
    EXPECT_TRUE(PumpToPeer(server, peer));
    return *sid;
  };

  const auto s_get =
      send_invalid(nexora::Method::kGet, "/echo/Stream", "application/grpc+proto", {});
  const auto s_mime = send_invalid(nexora::Method::kPost, "/echo/Stream", "text/plain", {});
  const auto s_noslash =
      send_invalid(nexora::Method::kPost, "echo/Stream", "application/grpc+proto", {});
  const std::string huge_path = "/" + std::string(1200, 'p');
  const auto s_path = send_invalid(nexora::Method::kPost, huge_path, "application/grpc+proto", {});
  const auto s_timeout =
      send_invalid(nexora::Method::kPost, "/echo/Stream", "application/grpc+proto", "bad");

  EXPECT_EQ(guard.unary_calls, 0);
  EXPECT_EQ(guard.stream_calls, 0);
  EXPECT_TRUE(server.Ready());
  EXPECT_EQ(server.ActiveStreams(), 0U);

  int seen_get = 0, seen_mime = 0, seen_noslash = 0, seen_path = 0, seen_timeout = 0;
  while (auto ev = peer.PollEvent()) {
    if (ev->kind == nexora::Http2Event::Kind::kHeaders) {
      const auto st = ev->headers.Get("grpc-status").value_or("");
      if (ev->stream_id == s_get && st == "13") ++seen_get;
      if (ev->stream_id == s_mime && st == "13") ++seen_mime;
      if (ev->stream_id == s_noslash && st == "12") ++seen_noslash;
      if (ev->stream_id == s_path && st == "12") ++seen_path;
      if (ev->stream_id == s_timeout && st == "3") ++seen_timeout;
    }
  }
  EXPECT_EQ(seen_get, 1);
  EXPECT_EQ(seen_mime, 1);
  EXPECT_EQ(seen_noslash, 1);
  EXPECT_EQ(seen_path, 1);
  EXPECT_EQ(seen_timeout, 1);

  auto good_sid = peer.OpenStream();
  ASSERT_TRUE(good_sid.has_value());
  nexora::Headers<> good_h;
  ASSERT_TRUE(good_h.Append("content-type", "application/grpc+proto").has_value());
  ASSERT_TRUE(good_h.Append("te", "trailers").has_value());
  ASSERT_TRUE(peer.SubmitRequestHeaders(*good_sid, nexora::Method::kPost, "http", "127.0.0.1:50051",
                                        "/echo/Stream", good_h, false)
                  .has_value());
  ASSERT_TRUE(peer.SubmitData(*good_sid, framed, true).has_value());
  ASSERT_TRUE(PumpFromPeer(peer, server));
  ASSERT_TRUE(PumpToPeer(server, peer));
  EXPECT_EQ(guard.stream_calls, 2);
  EXPECT_EQ(guard.unary_calls, 0);
}

}  // namespace
