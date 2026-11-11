#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "grpcxx/sync/in_memory_channel_pair.h"

namespace grpcxx {
namespace {

std::span<const std::byte> BytesOf(std::string_view text) noexcept {
  return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

std::string StringOf(std::span<const std::byte> bytes) {
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

struct MockWireMessage final {
  std::string body{};

  [[nodiscard]] std::size_t ByteSizeLong() const noexcept { return body.size(); }
  [[nodiscard]] bool SerializeToArray(void* data, int size) const noexcept {
    if (size < 0 || static_cast<std::size_t>(size) != body.size()) return false;
    if (!body.empty()) std::memcpy(data, body.data(), body.size());
    return true;
  }
  [[nodiscard]] bool ParseFromArray(const void* data, int size) {
    if (size < 0) return false;
    body.assign(static_cast<const char*>(data), static_cast<std::size_t>(size));
    return true;
  }
};

TEST(SyncInMemoryRpcTest, UnaryMetadataAndErrorStatus) {
  auto pair =
      InMemoryChannelPair::Create([](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage) {
          if (req.path == "/test.v1.EchoService/Unary") {
            MetadataMap init{};
            static_cast<void>(init.Add("x-srv-init", "ok"));
            static_cast<void>(srv.SetInitialMetadata(req.stream_id, init));
            static_cast<void>(srv.Send(req.stream_id, req.payload));
          }
          return;
        }
        if (req.event == ServerRequest::Event::kHalfClose) {
          if (req.path == "/test.v1.EchoService/Unary") {
            MetadataMap tr{};
            if (const auto db = req.metadata.Get("x-db-name")) {
              static_cast<void>(tr.Add("x-echo-db", *db));
            }
            ASSERT_TRUE(srv.Finish(req.stream_id, Status::OkStatus(), tr).has_value());
            ASSERT_EQ(req.metadata.Get("x-db-name"), "analytics");
            EXPECT_EQ(req.path, "/test.v1.EchoService/Unary");
          } else if (req.path == "/test.v1.EchoService/Fail") {
            MetadataMap tr{};
            static_cast<void>(tr.Add("x-err-detail", "table_missing"));
            static_cast<void>(
                srv.Finish(req.stream_id, Status{StatusCode::kNotFound, "table missing"}, tr));
          } else if (req.path == "/test.v1.EchoService/Trailers") {
            static_cast<void>(srv.FinishTrailersOnly(
                req.stream_id, Status{StatusCode::kPermissionDenied, "denied token"}));
          }
        }
      });
  ASSERT_TRUE(pair.has_value());

  CallOptions opts{};
  ASSERT_TRUE(opts.metadata.Add("x-db-name", "analytics").has_value());
  MockWireMessage req{.body = "hello-zero-glue"};
  auto typed = (*pair)->Channel().UnaryProto<MockWireMessage, MockWireMessage>(
      "/test.v1.EchoService/Unary", req, opts);
  ASSERT_TRUE(typed.has_value());
  EXPECT_EQ(typed->message.body, "hello-zero-glue");
  EXPECT_EQ(typed->initial_metadata.Get("x-srv-init"), "ok");
  EXPECT_EQ(typed->trailing_metadata.Get("x-echo-db"), "analytics");

  auto fail_resp = (*pair)->Channel().Unary("/test.v1.EchoService/Fail", BytesOf("q"));
  ASSERT_TRUE(fail_resp.has_value());
  EXPECT_EQ(fail_resp->status.Code(), StatusCode::kNotFound);
  EXPECT_EQ(fail_resp->status.Message(), "table missing");
  EXPECT_EQ(fail_resp->trailing_metadata.WireValue("grpc-status"), "5");
  EXPECT_EQ(fail_resp->trailing_metadata.Get("x-err-detail"), "table_missing");

  auto to_resp = (*pair)->Channel().Unary("/test.v1.EchoService/Trailers", BytesOf("q"));
  ASSERT_TRUE(to_resp.has_value());
  EXPECT_EQ(to_resp->status.Code(), StatusCode::kPermissionDenied);
  EXPECT_EQ(to_resp->status.Message(), "denied token");
  EXPECT_EQ(to_resp->trailing_metadata.WireValue("grpc-status"), "7");
}

TEST(SyncInMemoryRpcTest, ClientServerAndBidiStreams) {
  std::size_t accum_bytes = 0uz;
  auto pair = InMemoryChannelPair::Create(
      [&accum_bytes](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.path == "/svc.Stream/ClientUpload") {
          if (req.event == ServerRequest::Event::kMessage) {
            accum_bytes += req.payload.size();
          } else if (req.event == ServerRequest::Event::kHalfClose) {
            const auto reply = std::to_string(accum_bytes);
            static_cast<void>(srv.Send(req.stream_id, BytesOf(reply)));
            static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
          }
          return;
        }
        if (req.path == "/svc.Stream/ServerFeed") {
          if (req.event == ServerRequest::Event::kHalfClose) {
            for (int i = 0; i < 4; ++i) {
              const auto item = "row-" + std::to_string(i);
              static_cast<void>(srv.Send(req.stream_id, BytesOf(item)));
            }
            static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
          }
          return;
        }
        if (req.path == "/svc.Stream/BidiChat") {
          if (req.event == ServerRequest::Event::kMessage) {
            const std::string echoed = "ack:" + StringOf(req.payload);
            static_cast<void>(srv.Send(req.stream_id, BytesOf(echoed)));
          } else if (req.event == ServerRequest::Event::kHalfClose) {
            static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
          }
        }
      });
  ASSERT_TRUE(pair.has_value());

  auto cs = (*pair)->Channel().ClientStreamCall("/svc.Stream/ClientUpload");
  ASSERT_TRUE(cs.has_value());
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE((*cs)->Send(BytesOf("chunk10byt")).has_value());
  }
  auto cs_fin = (*cs)->Finish();
  ASSERT_TRUE(cs_fin.has_value());
  EXPECT_TRUE(cs_fin->status.Ok());
  EXPECT_EQ(StringOf(cs_fin->payload), "50");

  auto ss = (*pair)->Channel().ServerStreamCall("/svc.Stream/ServerFeed", BytesOf("select *"));
  ASSERT_TRUE(ss.has_value());
  std::vector<std::string> rows{};
  for (;;) {
    auto next = (*ss)->RecvNext();
    ASSERT_TRUE(next.has_value());
    if (!next->has_value()) break;
    rows.push_back(StringOf(**next));
  }
  ASSERT_EQ(rows.size(), 4uz);
  EXPECT_EQ(rows[0], "row-0");
  EXPECT_EQ(rows[3], "row-3");
  auto ss_fin = (*ss)->Finish();
  ASSERT_TRUE(ss_fin.has_value());
  EXPECT_TRUE(ss_fin->status.Ok());

  auto bidi = (*pair)->Channel().BidiStreamCall("/svc.Stream/BidiChat");
  ASSERT_TRUE(bidi.has_value());
  ASSERT_TRUE((*bidi)->Send(BytesOf("m1")).has_value());
  auto b1 = (*bidi)->RecvNext();
  ASSERT_TRUE(b1.has_value() && b1->has_value());
  EXPECT_EQ(StringOf(**b1), "ack:m1");
  ASSERT_TRUE((*bidi)->Send(BytesOf("m2")).has_value());
  ASSERT_TRUE((*bidi)->HalfClose().has_value());
  auto b2 = (*bidi)->RecvNext();
  ASSERT_TRUE(b2.has_value() && b2->has_value());
  EXPECT_EQ(StringOf(**b2), "ack:m2");
  auto b_end = (*bidi)->RecvNext();
  ASSERT_TRUE(b_end.has_value());
  EXPECT_FALSE(b_end->has_value());
  ASSERT_TRUE((*bidi)->Finish().has_value());
}

TEST(SyncInMemoryRpcTest, CompressionAndLargeMetadata) {
  SynchronousServerOptions srv_opts{};
  srv_opts.response_compression = CompressionAlgorithm::kZstd;

  MetadataMap captured_req_meta{};
  auto pair = InMemoryChannelPair::Create(
      [&captured_req_meta](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage) {
          captured_req_meta = req.metadata;
          static_cast<void>(srv.Send(req.stream_id, req.payload));
        } else if (req.event == ServerRequest::Event::kHalfClose) {
          static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
        }
      },
      srv_opts);
  ASSERT_TRUE(pair.has_value());

  const std::string compressible(32768uz, 'Z');
  for (const auto alg :
       {CompressionAlgorithm::kGzip, CompressionAlgorithm::kZstd, CompressionAlgorithm::kLz4}) {
    CallOptions opts{};
    opts.compression = alg;
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(opts.metadata.Add("x-meta-block-" + std::to_string(i), std::string(7000uz, 'm'))
                      .has_value());
    }
    auto resp =
        (*pair)->Channel().Unary("/svc.Compress/Echo", BytesOf(compressible), std::move(opts));
    ASSERT_TRUE(resp.has_value());
    ASSERT_TRUE(resp->status.Ok());
    EXPECT_EQ(StringOf(resp->payload), compressible);
    EXPECT_EQ(captured_req_meta.Get("x-meta-block-7")->size(), 7000uz);
  }
}

TEST(SyncInMemoryRpcTest, LargePayloadFlowControl) {
  constexpr std::size_t kPayloadBytes = 6uz * 1024uz * 1024uz;
  std::vector<std::byte> received_on_server{};
  auto pair = InMemoryChannelPair::Create(
      [&received_on_server](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage) {
          received_on_server.assign(req.payload.begin(), req.payload.end());
          static_cast<void>(srv.Send(req.stream_id, req.payload));
        } else if (req.event == ServerRequest::Event::kHalfClose) {
          static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
        }
      });
  ASSERT_TRUE(pair.has_value());

  std::vector<std::byte> flight_batch(kPayloadBytes);
  for (std::size_t i = 0uz; i < kPayloadBytes; ++i) {
    flight_batch[i] = static_cast<std::byte>((i * 131uz + 17uz) & 0xffuz);
  }

  auto resp =
      (*pair)->Channel().Unary("/arrow.flight.protocol.FlightService/DoExchange", flight_batch);
  ASSERT_TRUE(resp.has_value());
  ASSERT_TRUE(resp->status.Ok());
  ASSERT_EQ(received_on_server.size(), kPayloadBytes);
  ASSERT_EQ(resp->payload.size(), kPayloadBytes);
  EXPECT_TRUE(std::equal(resp->payload.begin(), resp->payload.end(), flight_batch.begin()));
  EXPECT_GT((*pair)->Channel().WindowUpdatesReceived(), 0uz);
  EXPECT_GT((*pair)->Server().WindowUpdatesReceived(), 0uz);
}

TEST(SyncInMemoryRpcTest, TlsMemoryPipeUnaryRoundtrip) {
  auto srv_tls = nexora::TlsServerConfig::SelfSignedForLocalhost();
  ASSERT_TRUE(srv_tls.has_value());
  ASSERT_TRUE(srv_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());
  auto cli_tls = SyncTlsClientOptions{.insecure_skip_verify = true}.BuildConfig();
  ASSERT_TRUE(cli_tls.has_value());

  auto pair = InMemoryChannelPair::CreateTls(
      *srv_tls, *cli_tls, [](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage) {
          static_cast<void>(srv.Send(req.stream_id, req.payload));
        } else if (req.event == ServerRequest::Event::kHalfClose) {
          static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
        }
      });
  ASSERT_TRUE(pair.has_value());

  auto resp = (*pair)->Channel().Unary("/svc.TlsMem/Ping", BytesOf("encrypted-memory-pipe"));
  ASSERT_TRUE(resp.has_value());
  EXPECT_TRUE(resp->status.Ok());
  EXPECT_EQ(StringOf(resp->payload), "encrypted-memory-pipe");
}

TEST(SyncInMemoryRpcTest, SelfResetKeepsRequestView) {
  int phase = 0;
  auto pair = InMemoryChannelPair::Create(
      [&phase](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage && phase == 0) {
          const std::string path{req.path};
          const auto pin = req.metadata.Get("x-pin");
          ASSERT_TRUE(srv.ResetStream(req.stream_id).has_value());
          EXPECT_EQ(req.path, path);
          ASSERT_TRUE(pin.has_value());
          EXPECT_EQ(req.metadata.Get("x-pin"), *pin);
          phase = 1;
          return;
        }
        if (req.event == ServerRequest::Event::kMessage) {
          ASSERT_TRUE(srv.Send(req.stream_id, req.payload).has_value());
        } else if (req.event == ServerRequest::Event::kHalfClose) {
          ASSERT_TRUE(srv.Finish(req.stream_id, Status::OkStatus()).has_value());
        }
      });
  ASSERT_TRUE(pair.has_value());

  CallOptions pinned{};
  ASSERT_TRUE(pinned.metadata.Add("x-pin", "keep").has_value());
  auto reset = (*pair)->Channel().Unary("/svc.Reset/Own", BytesOf("first"), pinned);
  EXPECT_FALSE(reset.has_value());
  EXPECT_EQ(phase, 1);
  EXPECT_EQ((*pair)->Server().ActiveStreams(), 0uz);

  auto again = (*pair)->Channel().Unary("/svc.Reset/Own", BytesOf("second"));
  ASSERT_TRUE(again.has_value());
  EXPECT_TRUE(again->status.Ok());
  EXPECT_EQ(StringOf(again->payload), "second");
  EXPECT_EQ((*pair)->Server().ActiveStreams(), 0uz);
}

}  // namespace
}  // namespace grpcxx
