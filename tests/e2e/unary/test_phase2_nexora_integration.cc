#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/server/grpc_server_service.h>
#include <grpcxx/sync/in_memory_channel_pair.h>
#include <grpcxx/sync/synchronous_server.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <nexora/http/uri.hpp>
#include <nexora/http2/connection.hpp>
#include <nexora/socket/memory_pipe.hpp>
#include <nexora/socket/tcp_transport.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

std::optional<cares::Endpoint> ReserveDeadLoopbackEndpoint() noexcept {
  auto bound = nexora::TcpListener::Bind({cares::IpAddress::FromV4({127, 0, 0, 1}), 0});
  if (!bound) return std::nullopt;
  return bound->LocalEndpoint();
}

struct ClaimSubscriber final {
  uring::UserDataToken expected{};
  std::size_t claimed{0};

  [[nodiscard]] bool OnCompletion(const uring::Delivery& delivery) noexcept {
    if (delivery.completion.token != expected) return false;
    ++claimed;
    return true;
  }
};

struct TagServerCtx final {
  std::byte tag{};
  std::size_t handled{0};

  static void Handle(void* raw, grpcxx::GrpcServerService& srv, std::size_t conn,
                     std::int32_t stream, std::uint64_t gen, std::string_view,
                     std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<TagServerCtx*>(raw);
    ++self.handled;
    std::vector<std::byte> out(payload.begin(), payload.end());
    out.push_back(self.tag);
    static_cast<void>(srv.Complete(conn, stream, gen, out));
  }
};

struct ReplyCapture final {
  bool done{false};
  int status{-1};
  std::vector<std::byte> body{};

  void Reset() noexcept {
    done = false;
    status = -1;
    body.clear();
  }

  static void OnDone(void* raw, std::int32_t, int code, std::span<const std::byte> bytes) noexcept {
    auto& self = *static_cast<ReplyCapture*>(raw);
    self.status = code;
    self.body.assign(bytes.begin(), bytes.end());
    self.done = true;
  }
};

void ShutdownServer(uring::Proactor& ring, nexora::CompletionRoutes<>& routes,
                    std::unique_ptr<grpcxx::GrpcServerService>& srv,
                    grpcxx::ClientChannelService* peer_cli, grpcxx::MonotonicTimestamp now) {
  if (!srv) return;
  static_cast<void>(srv->BeginShutdown(0));
  for (unsigned tick = 0; tick < 20000 && !srv->Stopped(); ++tick) {
    static_cast<void>(ring.Tick(routes));
    static_cast<void>(srv->PollShutdown({}, now));
    if (peer_cli) static_cast<void>(peer_cli->PollOnce({}, now));
  }
  static_cast<void>(srv->Shutdown());
  srv.reset();
}

void ShutdownClient(uring::Proactor& ring, nexora::CompletionRoutes<>& routes,
                    std::unique_ptr<grpcxx::ClientChannelService>& cli,
                    grpcxx::GrpcServerService* peer_srv, grpcxx::MonotonicTimestamp now) {
  if (!cli) return;
  static_cast<void>(cli->BeginShutdown(0));
  for (unsigned tick = 0; tick < 20000 && !cli->Stopped(); ++tick) {
    static_cast<void>(ring.Tick(routes));
    static_cast<void>(cli->PollShutdown({}, now));
    if (peer_srv) static_cast<void>(peer_srv->PollOnce({}, now));
  }
  static_cast<void>(cli->Shutdown());
  cli.reset();
}

TEST(Phase2NexoraIntegration, MultiSubscriberBindClaimingAndUnregisterCoexistence) {
  auto ring = uring::Proactor::Create(256);
  ASSERT_TRUE(ring.has_value());
  nexora::CompletionRoutes<> routes{};
  const grpcxx::MonotonicTimestamp now{1'000'000'000ULL};

  TagServerCtx tag_a{.tag = std::byte{0xA1}};
  TagServerCtx tag_b{.tag = std::byte{0xB2}};
  grpcxx::GrpcServerService::Options opts_a{.context = &tag_a, .handler = &TagServerCtx::Handle};
  grpcxx::GrpcServerService::Options opts_b{.context = &tag_b, .handler = &TagServerCtx::Handle};
  auto srv_a = std::make_unique<grpcxx::GrpcServerService>(*ring, routes, opts_a);
  auto srv_b = std::make_unique<grpcxx::GrpcServerService>(*ring, routes, opts_b);
  ASSERT_TRUE(srv_a->Start().has_value());
  ASSERT_TRUE(srv_b->Start().has_value());
  const auto ep_a = srv_a->LocalAddress();
  const auto ep_b = srv_b->LocalAddress();
  ASSERT_TRUE(ep_a.has_value());
  ASSERT_TRUE(ep_b.has_value());

  grpcxx::ClientChannelService::Options cli_opts_a{.endpoint = *ep_a};
  grpcxx::ClientChannelService::Options cli_opts_b{.endpoint = *ep_b};
  auto cli_a = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, cli_opts_a);
  auto cli_b = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, cli_opts_b);
  ASSERT_TRUE(cli_a->Start().has_value());
  ASSERT_TRUE(cli_b->Start().has_value());

  ClaimSubscriber sub_a{};
  ClaimSubscriber sub_b{};
  ASSERT_TRUE(
      routes
          .Register(nexora::CompletionRoute::BindClaiming<19, ClaimSubscriber,
                                                          &ClaimSubscriber::OnCompletion>(&sub_a))
          .has_value());
  ASSERT_TRUE(
      routes
          .Register(nexora::CompletionRoute::BindClaiming<19, ClaimSubscriber,
                                                          &ClaimSubscriber::OnCompletion>(&sub_b))
          .has_value());
  EXPECT_EQ(routes.Size(), 6uz);

  auto step_all = [&]() {
    if (!ring->Tick(routes)) return false;
    if (srv_a && !srv_a->PollOnce({}, now)) return false;
    if (srv_b && !srv_b->PollOnce({}, now)) return false;
    if (cli_a && !cli_a->PollOnce({}, now)) return false;
    if (cli_b && !cli_b->PollOnce({}, now)) return false;
    return true;
  };

  for (unsigned tick = 0;
       tick < 20000 && (cli_a->GetState() != grpcxx::ClientChannelService::ChannelState::kReady ||
                        cli_b->GetState() != grpcxx::ClientChannelService::ChannelState::kReady);
       ++tick) {
    ASSERT_TRUE(step_all());
  }
  ASSERT_EQ(cli_a->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);
  ASSERT_EQ(cli_b->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const auto tok_b = ring->Arm(19, uring::ops::PrepareNop());
  const auto tok_a = ring->Arm(19, uring::ops::PrepareNop());
  ASSERT_TRUE(tok_b.has_value());
  ASSERT_TRUE(tok_a.has_value());
  sub_b.expected = *tok_b;
  sub_a.expected = *tok_a;

  const std::array<std::byte, 2> payload{std::byte{0x11}, std::byte{0x22}};
  ReplyCapture rep_a{};
  ReplyCapture rep_b{};
  ASSERT_TRUE(cli_a->CallUnary("/echo/Unary", payload, &rep_a, &ReplyCapture::OnDone).has_value());
  ASSERT_TRUE(cli_b->CallUnary("/echo/Unary", payload, &rep_b, &ReplyCapture::OnDone).has_value());
  for (unsigned tick = 0;
       tick < 20000 && (!rep_a.done || !rep_b.done || sub_a.claimed < 1 || sub_b.claimed < 1);
       ++tick) {
    ASSERT_TRUE(step_all());
  }
  ASSERT_TRUE(rep_a.done);
  ASSERT_TRUE(rep_b.done);
  EXPECT_EQ(rep_a.status, 0);
  EXPECT_EQ(rep_b.status, 0);
  EXPECT_EQ(rep_a.body.back(), std::byte{0xA1});
  EXPECT_EQ(rep_b.body.back(), std::byte{0xB2});
  EXPECT_EQ(sub_a.claimed, 1uz);
  EXPECT_EQ(sub_b.claimed, 1uz);

  // Unregister channel A, server A, and claiming subscriber A; sibling B routes stay intact.
  ShutdownClient(*ring, routes, cli_a, srv_a.get(), now);
  ShutdownServer(*ring, routes, srv_a, nullptr, now);
  EXPECT_TRUE(routes.Unregister(&sub_a));
  EXPECT_EQ(routes.Size(), 3uz);

  const auto tok_b2 = ring->Arm(19, uring::ops::PrepareNop());
  ASSERT_TRUE(tok_b2.has_value());
  sub_b.expected = *tok_b2;
  rep_b.Reset();
  ASSERT_TRUE(cli_b->CallUnary("/echo/Unary", payload, &rep_b, &ReplyCapture::OnDone).has_value());
  for (unsigned tick = 0; tick < 20000 && (!rep_b.done || sub_b.claimed < 2); ++tick) {
    ASSERT_TRUE(step_all());
  }
  ASSERT_TRUE(rep_b.done);
  EXPECT_EQ(rep_b.status, 0);
  EXPECT_EQ(rep_b.body.back(), std::byte{0xB2});
  EXPECT_EQ(sub_a.claimed, 1uz);
  EXPECT_EQ(sub_b.claimed, 2uz);

  ShutdownClient(*ring, routes, cli_b, srv_b.get(), now);
  ShutdownServer(*ring, routes, srv_b, nullptr, now);
  EXPECT_TRUE(routes.Unregister(&sub_b));
  EXPECT_TRUE(routes.Empty());
}

TEST(Phase2NexoraIntegration, NonResettingDnsReResolutionAndEndpointReconcile) {
  const auto dead_ep = ReserveDeadLoopbackEndpoint();
  ASSERT_TRUE(dead_ep.has_value());

  auto ring = uring::Proactor::Create(256);
  ASSERT_TRUE(ring.has_value());
  nexora::CompletionRoutes<> routes{};
  grpcxx::MonotonicTimestamp now{5'000'000'000ULL};

  TagServerCtx tag_a{.tag = std::byte{0xA1}};
  TagServerCtx tag_b{.tag = std::byte{0xB2}};
  auto srv_a = std::make_unique<grpcxx::GrpcServerService>(
      *ring, routes,
      grpcxx::GrpcServerService::Options{.context = &tag_a, .handler = &TagServerCtx::Handle});
  auto srv_b = std::make_unique<grpcxx::GrpcServerService>(
      *ring, routes,
      grpcxx::GrpcServerService::Options{.context = &tag_b, .handler = &TagServerCtx::Handle});
  ASSERT_TRUE(srv_a->Start().has_value());
  ASSERT_TRUE(srv_b->Start().has_value());
  const auto ep_a = srv_a->LocalAddress();
  const auto ep_b = srv_b->LocalAddress();
  ASSERT_TRUE(ep_a.has_value());
  ASSERT_TRUE(ep_b.has_value());

  const std::array<cares::Endpoint, 2> initial_eps{*dead_ep, *ep_a};
  grpcxx::ClientChannelService::Options cli_opts{.endpoints = initial_eps};
  auto cli = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, cli_opts);
  ASSERT_TRUE(cli->Start().has_value());

  auto step = [&](grpcxx::ClientChannelService* extra_cli = nullptr) {
    if (!ring->Tick(routes)) return false;
    if (srv_a && !srv_a->PollOnce({}, now)) return false;
    if (srv_b && !srv_b->PollOnce({}, now)) return false;
    if (cli && !cli->PollOnce({}, now)) return false;
    if (extra_cli && !extra_cli->PollOnce({}, now)) return false;
    return true;
  };

  for (unsigned tick = 0;
       tick < 20000 && cli->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(step());
  }
  ASSERT_EQ(cli->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  // Reconcile [dead_ep, ep_a, ep_b, ep_a]: active socket on ep_a stays kReady and dead_ep keeps
  // quarantine.
  const std::array<cares::Endpoint, 4> reconciled{*dead_ep, *ep_a, *ep_b, *ep_a};
  ASSERT_TRUE(cli->ReconcileEndpoints(reconciled).has_value());
  EXPECT_EQ(cli->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::array<std::byte, 1> req{std::byte{0x42}};
  ReplyCapture rep{};
  ASSERT_TRUE(cli->CallUnary("/echo/Unary", req, &rep, &ReplyCapture::OnDone).has_value());
  for (unsigned tick = 0; tick < 20000 && !rep.done; ++tick) ASSERT_TRUE(step());
  ASSERT_TRUE(rep.done);
  EXPECT_EQ(rep.body.back(), std::byte{0xA1});

  // Tear down srv_a inside dead_ep's 60s quarantine window; preserved health routes straight to
  // srv_b.
  now += 100'000'000ULL;
  ShutdownServer(*ring, routes, srv_a, cli.get(), now);
  for (unsigned tick = 0;
       tick < 20000 && cli->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(step());
  }
  ASSERT_EQ(cli->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  rep.Reset();
  ASSERT_TRUE(cli->CallUnary("/echo/Unary", req, &rep, &ReplyCapture::OnDone).has_value());
  for (unsigned tick = 0; tick < 20000 && !rep.done; ++tick) ASSERT_TRUE(step());
  ASSERT_TRUE(rep.done);
  EXPECT_EQ(rep.body.back(), std::byte{0xB2});

  // Also exercise live cares::Resolver + ClientChannelService::ReResolve() without resetting active
  // socket.
  auto resolver = cares::Resolver<>::FromSystem(*ring);
  ASSERT_TRUE(resolver.has_value());
  ASSERT_TRUE(resolver->InsertStaticHost("svc.internal", cares::IpAddress::FromV4({127, 0, 0, 1}))
                  .has_value());
  grpcxx::ClientChannelService::Options dns_opts{
      .target = "svc.internal",
      .port = ep_b->port,
  };
  auto dns_cli = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, *resolver, dns_opts);
  ASSERT_TRUE(dns_cli->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && dns_cli->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(step(dns_cli.get()));
  }
  ASSERT_EQ(dns_cli->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::size_t handled_before = tag_b.handled;
  resolver->InvalidateCache("svc.internal");
  ASSERT_TRUE(dns_cli->ReResolve().has_value());
  EXPECT_EQ(dns_cli->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  rep.Reset();
  ASSERT_TRUE(dns_cli->CallUnary("/echo/Unary", req, &rep, &ReplyCapture::OnDone).has_value());
  for (unsigned tick = 0; tick < 20000 && !rep.done; ++tick) ASSERT_TRUE(step(dns_cli.get()));
  ASSERT_TRUE(rep.done);
  EXPECT_EQ(rep.status, 0);
  EXPECT_EQ(rep.body.back(), std::byte{0xB2});
  EXPECT_EQ(tag_b.handled, handled_before + 1uz);

  ShutdownClient(*ring, routes, dns_cli, srv_b.get(), now);
  ShutdownClient(*ring, routes, cli, srv_b.get(), now);
  ShutdownServer(*ring, routes, srv_b, nullptr, now);
}

TEST(Phase2NexoraIntegration, SteadyStateHttp2MoveOnlyEventsFlowControlAndPascalCaseUri) {
  const auto https_uri =
      nexora::Uri::Parse("https://API.Example.Internal:8443/v1/echo?mode=fast#frag");
  ASSERT_TRUE(https_uri.has_value());
  EXPECT_EQ(https_uri->Scheme(), nexora::Scheme::kHttps);
  EXPECT_TRUE(https_uri->IsTls());
  EXPECT_EQ(https_uri->Host(), "api.example.internal");
  EXPECT_EQ(https_uri->Port(), 8443U);
  EXPECT_EQ(https_uri->EffectivePort(), 8443U);
  EXPECT_EQ(https_uri->ExplicitPort(), std::optional<std::uint16_t>{8443U});
  EXPECT_EQ(https_uri->Authority(), "api.example.internal:8443");
  EXPECT_EQ(https_uri->Path(), "/v1/echo");
  EXPECT_EQ(https_uri->Query(), std::optional<std::string_view>{"mode=fast"});
  EXPECT_EQ(https_uri->PathAndQuery(), "/v1/echo?mode=fast");

  const auto v6_uri = nexora::Uri::Parse("http://[::1]/grpc.health.v1.Health/Check");
  ASSERT_TRUE(v6_uri.has_value());
  EXPECT_FALSE(v6_uri->IsTls());
  EXPECT_EQ(v6_uri->Host(), "::1");
  EXPECT_EQ(v6_uri->EffectivePort(), 80U);
  EXPECT_FALSE(v6_uri->ExplicitPort().has_value());
  EXPECT_EQ(v6_uri->Authority(), "[::1]");
  EXPECT_EQ(v6_uri->Path(), "/grpc.health.v1.Health/Check");

  // Exercise 300 zero-payload PADDED DATA frames (300 * 256 = 76,800 flow-control bytes > 65,535)
  // followed by a 3-byte gRPC request message through SynchronousServer::FromMemoryPipe.
  auto pipes = nexora::MemoryPipe::CreatePair(512uz * 1024uz);
  ASSERT_TRUE(pipes.has_value());

  std::vector<std::byte> captured_req{};
  grpcxx::SynchronousServerOptions srv_opts{};
  srv_opts.flow_control_window_bytes = 65535U;
  auto server = grpcxx::SynchronousServer::FromMemoryPipe(
      std::move(pipes->server),
      [&captured_req](grpcxx::SynchronousServer& srv, const grpcxx::ServerRequest& req) noexcept {
        if (req.event == grpcxx::ServerRequest::Event::kMessage) {
          captured_req.assign(req.payload.begin(), req.payload.end());
          static_cast<void>(srv.Send(req.stream_id, req.payload));
        } else if (req.event == grpcxx::ServerRequest::Event::kHalfClose) {
          static_cast<void>(srv.Finish(req.stream_id, grpcxx::Status::OkStatus()));
        }
      },
      srv_opts);
  ASSERT_TRUE(server.has_value());

  auto cli_h2 = std::make_unique<nexora::Http2Connection>(nexora::Http2Role::kClient);
  ASSERT_TRUE(cli_h2->InitiateHandshake().has_value());
  const auto sid = cli_h2->OpenStream();
  ASSERT_TRUE(sid.has_value());
  nexora::Headers<> hdrs{};
  ASSERT_TRUE(hdrs.Set("te", "trailers").has_value());
  ASSERT_TRUE(hdrs.Set("content-type", "application/grpc").has_value());
  ASSERT_TRUE(cli_h2
                  ->SubmitRequestHeaders(*sid, nexora::Method::kPost, "http", "localhost",
                                         "/pad.Echo/Unary", hdrs, false)
                  .has_value());

  std::array<std::uint8_t, 16384> scratch{};
  auto pump_wire = [&]() {
    const auto out = cli_h2->PendingOutput();
    if (!out.empty()) {
      ASSERT_TRUE(pipes->client.WriteAll(out).has_value());
      cli_h2->ConsumeOutput(out.size());
    }
    ASSERT_TRUE((*server)->PollOnce().has_value());
    while (pipes->client.ReadableBytes() > 0) {
      const auto n = pipes->client.Read(scratch);
      ASSERT_TRUE(n.has_value());
      std::span<const std::uint8_t> rem{scratch.data(), *n};
      while (!rem.empty()) {
        const auto fed = cli_h2->FeedBytes(rem);
        ASSERT_TRUE(fed.Ok());
        rem = rem.subspan(fed.Consumed());
        if (fed.Paused()) break;
      }
    }
  };
  pump_wire();

  // Send 300 padded empty DATA frames (length=256, flags=0x08 PADDED, pad_length=255) in bursts of
  // 100
  // (> kEventCap=80) so both Http2Read pause-drain and flow_control_bytes replenishment are
  // exercised.
  std::array<std::uint8_t, 9 + 256> padded_frame{};
  padded_frame[0] = 0x00;
  padded_frame[1] = 0x01;
  padded_frame[2] = 0x00;  // length = 256
  padded_frame[3] = 0x00;  // DATA
  padded_frame[4] = 0x08;  // PADDED
  padded_frame[8] = static_cast<std::uint8_t>(*sid);
  padded_frame[9] = 255U;  // pad_length = 255, data payload = 0 bytes

  for (std::size_t burst = 0; burst < 3; ++burst) {
    for (std::size_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(pipes->client.WriteAll(padded_frame).has_value());
    }
    pump_wire();
    while (cli_h2->PollEvent().has_value()) {
    }
  }
  EXPECT_GT(cli_h2->WindowUpdateCount(), 0U);

  // Send final 5+3 byte gRPC message with END_STREAM.
  const std::array<std::uint8_t, 8> grpc_msg{0, 0, 0, 0, 3, 'o', 'k', '!'};
  const std::array<std::uint8_t, 9> final_hdr{
      0, 0, 8, 0x00, 0x01, 0, 0, 0, static_cast<std::uint8_t>(*sid)};
  ASSERT_TRUE(pipes->client.WriteAll(final_hdr).has_value());
  ASSERT_TRUE(pipes->client.WriteAll(grpc_msg).has_value());
  pump_wire();

  bool got_status_200 = false;
  bool got_end_stream = false;
  std::string grpc_status{};
  std::vector<std::uint8_t> resp_bytes{};
  while (auto ev = cli_h2->PollEvent()) {
    nexora::Http2Event moved_ev = std::move(*ev);
    if (moved_ev.kind == nexora::Http2Event::Kind::kHeaders) {
      got_status_200 = (moved_ev.status == nexora::StatusCode::kOk);
    } else if (moved_ev.kind == nexora::Http2Event::Kind::kData) {
      nexora::Http2Bytes moved_data = std::move(moved_ev.data);
      const auto span = moved_data.Span();
      resp_bytes.insert(resp_bytes.end(), span.begin(), span.end());
      if (moved_ev.end_stream) got_end_stream = true;
    } else if (moved_ev.kind == nexora::Http2Event::Kind::kTrailers) {
      if (const auto s = moved_ev.headers.Get("grpc-status")) grpc_status.assign(*s);
      if (moved_ev.end_stream) got_end_stream = true;
    }
  }

  EXPECT_TRUE(got_status_200);
  EXPECT_TRUE(got_end_stream);
  EXPECT_EQ(grpc_status, "0");
  ASSERT_EQ(captured_req.size(), 3uz);
  EXPECT_EQ(resp_bytes, (std::vector<std::uint8_t>(grpc_msg.begin(), grpc_msg.end())));
}

}  // namespace
