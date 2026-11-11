#include <grpcxx/h2/h2_client_session.h>

#include <bit>
#include <limits>
#include <nexora/socket/tcp_transport.hpp>
#include <string>

#include "transport_fixture.h"

namespace {

struct Rig {
  uring::Result<uring::Proactor> ring{uring::Proactor::Create(256)};
  nexora::CompletionRoutes<> routes;
  std::unique_ptr<grpcxx::GrpcServerService> server;
  std::unique_ptr<grpcxx::ClientChannelService> client;
  grpcxx::MonotonicTimestamp now{1'000'000'000ULL};

  std::string last_scheme;
  std::string last_authority;
  std::uint64_t last_timeout_ns{};
  int reply_status{0};
  std::string reply_message{};

  bool completed{};
  int status{-1};
  std::vector<std::byte> received;

  static void OnRequest(void* ctx, grpcxx::GrpcServerService& srv, std::size_t conn,
                        std::int32_t stream, std::uint64_t gen, std::string_view,
                        std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Rig*>(ctx);
    self.last_scheme = std::string(srv.StreamScheme(conn, stream, gen));
    self.last_authority = std::string(srv.StreamAuthority(conn, stream, gen));
    self.last_timeout_ns = srv.StreamTimeoutNs(conn, stream, gen);
    const auto body = self.reply_status == 0 ? payload : std::span<const std::byte>{};
    static_cast<void>(srv.Complete(conn, stream, gen, body, self.reply_status, self.reply_message));
  }

  static void OnResponse(void* ctx, std::int32_t, int code,
                         std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Rig*>(ctx);
    self.status = code;
    self.completed = true;
    self.received.assign(payload.begin(), payload.end());
  }

  bool EnsureRoute() const noexcept { return true; }

  bool Step() {
    if (!ring->Tick(routes)) return false;
    if (server && !server->PollOnce({}, now)) return false;
    if (client && !client->PollOnce({}, now)) return false;
    return true;
  }

  void StopClient() {
    if (!client) return;
    static_cast<void>(client->BeginShutdown(0));
    for (unsigned tick = 0; tick < 20000 && !client->Stopped(); ++tick) {
      static_cast<void>(ring->Tick(routes));
      static_cast<void>(client->PollShutdown({}, now));
      if (server) static_cast<void>(server->PollOnce({}, now));
    }
    static_cast<void>(client->Shutdown());
    client.reset();
  }

  void StopServer() {
    if (!server) return;
    static_cast<void>(server->BeginShutdown(0));
    for (unsigned tick = 0; tick < 20000 && !server->Stopped(); ++tick) {
      static_cast<void>(ring->Tick(routes));
      static_cast<void>(server->PollShutdown({}, now));
      if (client) static_cast<void>(client->PollOnce({}, now));
    }
    static_cast<void>(server->Shutdown());
    server.reset();
  }

  ~Rig() {
    if (!ring) return;
    StopClient();
    StopServer();
  }
};

std::optional<cares::Endpoint> ReserveDeadLoopbackEndpoint() {
  auto bound = nexora::TcpListener::Bind({cares::IpAddress::FromV4({127, 0, 0, 1}), 0});
  if (!bound) return std::nullopt;
  return bound->LocalEndpoint();
}

TEST(ProactorResilience, AuthoritySchemeTimeoutAndStatusMessage) {
  Rig rig;
  ASSERT_TRUE(rig.ring.has_value());
  ASSERT_TRUE(rig.EnsureRoute());

  grpcxx::GrpcServerService::Options srv_opts;
  srv_opts.context = &rig;
  srv_opts.handler = &Rig::OnRequest;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  ASSERT_TRUE(rig.server->Start().has_value());
  auto ep = rig.server->LocalAddress();
  ASSERT_TRUE(ep.has_value());

  // Plaintext with default server_name={} derives 127.0.0.1:<port> and propagates
  // default_timeout_ns.
  grpcxx::ClientChannelService::Options cli_opts;
  cli_opts.endpoint = *ep;
  cli_opts.default_timeout_ns = 1'500'000'000ULL;
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, cli_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::array<std::byte, 3> req{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", req, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
  EXPECT_EQ(rig.last_scheme, "http");
  EXPECT_EQ(rig.last_authority, "127.0.0.1:" + std::to_string(ep->port));
  EXPECT_EQ(rig.last_timeout_ns, 1'500'000'000ULL);
  rig.StopClient();

  // Explicit server_name overrides :authority and percent-encoded grpc-message round-trips.
  rig.reply_status = 8;
  rig.reply_message = "quota: 100% exhausted";
  grpcxx::ClientChannelService::Options override_opts;
  override_opts.endpoint = *ep;
  override_opts.server_name = "rpc.internal:8443";
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, override_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", req, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 8);
  EXPECT_EQ(rig.last_scheme, "http");
  EXPECT_EQ(rig.last_authority, "rpc.internal:8443");
  EXPECT_EQ(rig.client->LastStatusMessage(), "quota: 100% exhausted");
  rig.StopClient();
  rig.StopServer();

  // TLS derives :scheme == "https" and omits default port on bare server_name.
  auto srv_tls = nexora::TlsServerConfig::SelfSignedForLocalhost();
  auto cli_tls = nexora::TlsClientConfig::CreateInsecureForTest();
  ASSERT_TRUE(srv_tls.has_value());
  ASSERT_TRUE(cli_tls.has_value());
  ASSERT_TRUE(srv_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());
  ASSERT_TRUE(cli_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());

  rig.reply_status = 0;
  rig.reply_message.clear();
  srv_opts.tls = &*srv_tls;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  ASSERT_TRUE(rig.server->Start().has_value());
  auto tls_ep = rig.server->LocalAddress();
  ASSERT_TRUE(tls_ep.has_value());

  grpcxx::ClientChannelService::Options tls_cli_opts;
  tls_cli_opts.endpoint = *tls_ep;
  tls_cli_opts.tls = &*cli_tls;
  tls_cli_opts.server_name = "localhost";
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, tls_cli_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", req, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
  EXPECT_EQ(rig.last_scheme, "https");
  EXPECT_EQ(rig.last_authority, "localhost");
}

TEST(ProactorResilience, SilentPeersPermitValidClient) {
  Rig rig;
  ASSERT_TRUE(rig.ring.has_value());
  ASSERT_TRUE(rig.EnsureRoute());

  grpcxx::GrpcServerService::Options srv_opts;
  srv_opts.context = &rig;
  srv_opts.handler = &Rig::OnRequest;
  srv_opts.handshake_timeout_ns = 2'000'000'000ULL;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  ASSERT_TRUE(rig.server->Start().has_value());
  auto ep = rig.server->LocalAddress();
  ASSERT_TRUE(ep.has_value());

  // Open 64 silent TCP connections (exceeding the legacy 4-slot cap and filling all 64 slots).
  std::vector<nexora::TcpTransport> silent;
  silent.reserve(grpcxx::GrpcServerService::kMaxConnections);
  for (std::size_t i = 0; i < grpcxx::GrpcServerService::kMaxConnections; ++i) {
    auto conn = nexora::TcpTransport::ConnectTo(*ep, 1000);
    ASSERT_TRUE(conn.has_value());
    silent.push_back(std::move(*conn));
    for (unsigned step = 0; step < 8; ++step) ASSERT_TRUE(rig.Step());
  }

  // Even before handshake_timeout_ns elapses, slot eviction reclaims an unhandshaked slot.
  grpcxx::ClientChannelService::Options cli_opts;
  cli_opts.endpoint = *ep;
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, cli_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::array<std::byte, 4> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", payload, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
  EXPECT_TRUE(std::ranges::equal(rig.received, payload));

  // Advancing monotonic clock past handshake_timeout_ns evicts remaining silent connections
  // cleanly.
  rig.now += 3'000'000'000ULL;
  for (unsigned tick = 0; tick < 256; ++tick) ASSERT_TRUE(rig.Step());
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", payload, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
}

TEST(ProactorResilience, HalfStreamsReleaseAllSlots) {
  Rig rig;
  ASSERT_TRUE(rig.ring);
  ASSERT_TRUE(rig.EnsureRoute());
  grpcxx::GrpcServerService::Options options;
  options.context = &rig;
  options.handler = &Rig::OnRequest;
  options.handshake_timeout_ns = 0;
  options.idle_timeout_ns = 0;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, options);
  ASSERT_TRUE(rig.server->Start());
  auto endpoint = rig.server->LocalAddress();
  ASSERT_TRUE(endpoint);
  std::vector<nexora::TcpTransport> peers;
  for (std::size_t slot = 0; slot < grpcxx::GrpcServerService::kMaxConnections; ++slot) {
    auto peer = nexora::TcpTransport::ConnectTo(*endpoint, 1000);
    ASSERT_TRUE(peer);
    grpcxx::H2ClientSession session;
    ASSERT_TRUE(session.Open(
        "/echo/Unary", nullptr, nullptr,
        +[](void*, std::int32_t, int, std::span<const std::byte>) noexcept {}));
    for (unsigned step = 0; step < 8; ++step) {
      auto wire = session.Send();
      ASSERT_TRUE(wire);
      if (wire->empty()) break;
      ASSERT_TRUE(peer->WriteAll({std::bit_cast<const std::uint8_t*>(wire->data()), wire->size()}));
    }
    peers.push_back(std::move(*peer));
    for (unsigned step = 0; step < 32; ++step) ASSERT_TRUE(rig.Step());
  }
  EXPECT_EQ(rig.server->ActiveConnections(), 64U);
  for (auto& peer : peers) peer.ShutdownWrite();
  for (unsigned tick = 0; tick < 20000 && rig.server->ActiveConnections(); ++tick)
    ASSERT_TRUE(rig.Step());
  EXPECT_EQ(rig.server->ActiveConnections(), 0U);
  grpcxx::ClientChannelService::Options client_options;
  client_options.endpoint = *endpoint;
  rig.client =
      std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, client_options);
  ASSERT_TRUE(rig.client->Start());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick)
    ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", {}, &rig, &Rig::OnResponse));
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  EXPECT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
}

TEST(ProactorResilience, MultiEndpointFailoverAndQuarantineRecovery) {
  auto dead_ep = ReserveDeadLoopbackEndpoint();
  ASSERT_TRUE(dead_ep.has_value());

  Rig rig;
  ASSERT_TRUE(rig.ring.has_value());
  ASSERT_TRUE(rig.EnsureRoute());

  grpcxx::GrpcServerService::Options srv_opts;
  srv_opts.context = &rig;
  srv_opts.handler = &Rig::OnRequest;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  ASSERT_TRUE(rig.server->Start().has_value());
  auto live_ep = rig.server->LocalAddress();
  ASSERT_TRUE(live_ep.has_value());

  // 1) Dead endpoint first + live endpoint second -> automatic failover to live_ep.
  const std::array<cares::Endpoint, 2> failover_list{*dead_ep, *live_ep};
  grpcxx::ClientChannelService::Options cli_opts;
  cli_opts.endpoints = failover_list;
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, cli_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::array<std::byte, 2> msg{std::byte{0xca}, std::byte{0xfe}};
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", msg, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
  rig.StopClient();
  rig.StopServer();

  // 2) Single dead endpoint enters kTransientFailure (kUnavailable), then recovers after cooldown.
  auto recover_ep = ReserveDeadLoopbackEndpoint();
  ASSERT_TRUE(recover_ep.has_value());

  grpcxx::ClientChannelService::Options single_opts;
  single_opts.endpoint = *recover_ep;
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, single_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 &&
       rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kTransientFailure;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kTransientFailure);

  auto unavailable = rig.client->CallUnary("/echo/Unary", msg, &rig, &Rig::OnResponse);
  ASSERT_FALSE(unavailable.has_value());
  EXPECT_EQ(unavailable.error().Code(), grpcxx::StatusCode::kUnavailable);

  srv_opts.endpoint = *recover_ep;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  if (rig.server->Start().has_value()) {
    rig.now += 2'000'000'000ULL;
    for (unsigned tick = 0; tick < 20000 && rig.client->GetState() !=
                                                grpcxx::ClientChannelService::ChannelState::kReady;
         ++tick) {
      ASSERT_TRUE(rig.Step());
    }
    ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);
    rig.completed = false;
    ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", msg, &rig, &Rig::OnResponse).has_value());
    for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
    ASSERT_TRUE(rig.completed);
    EXPECT_EQ(rig.status, 0);
  } else {
    rig.server.reset();
  }
}

TEST(ProactorResilience, TlsTrustAndAlpnRejection) {
  Rig rig;
  ASSERT_TRUE(rig.ring.has_value());
  ASSERT_TRUE(rig.EnsureRoute());

  auto srv_tls = nexora::TlsServerConfig::SelfSignedForLocalhost();
  ASSERT_TRUE(srv_tls.has_value());
  ASSERT_TRUE(srv_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());

  grpcxx::GrpcServerService::Options srv_opts;
  srv_opts.context = &rig;
  srv_opts.handler = &Rig::OnRequest;
  srv_opts.tls = &*srv_tls;
  rig.server = std::make_unique<grpcxx::GrpcServerService>(*rig.ring, rig.routes, srv_opts);
  ASSERT_TRUE(rig.server->Start().has_value());
  auto ep = rig.server->LocalAddress();
  ASSERT_TRUE(ep.has_value());

  // A) System-trust client rejects self-signed server certificate cleanly.
  auto strict_tls = nexora::TlsClientConfig::CreateDefault();
  ASSERT_TRUE(strict_tls.has_value());
  ASSERT_TRUE(strict_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());

  grpcxx::ClientChannelService::Options bad_cert_opts;
  bad_cert_opts.endpoint = *ep;
  bad_cert_opts.tls = &*strict_tls;
  bad_cert_opts.server_name = "localhost";
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, bad_cert_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 &&
       rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kTransientFailure;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  EXPECT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kTransientFailure);
  rig.StopClient();

  // B) Client offering only http/1.1 against h2-requiring server fails ALPN cleanly.
  auto alpn_mismatch_tls = nexora::TlsClientConfig::CreateInsecureForTest();
  ASSERT_TRUE(alpn_mismatch_tls.has_value());
  ASSERT_TRUE(alpn_mismatch_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp11).has_value());

  grpcxx::ClientChannelService::Options bad_alpn_opts;
  bad_alpn_opts.endpoint = *ep;
  bad_alpn_opts.tls = &*alpn_mismatch_tls;
  bad_alpn_opts.server_name = "localhost";
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, bad_alpn_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 &&
       rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kTransientFailure;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  EXPECT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kTransientFailure);
  rig.StopClient();

  // C) Server remains healthy for a valid TLS + h2 client afterward.
  auto good_tls = nexora::TlsClientConfig::CreateInsecureForTest();
  ASSERT_TRUE(good_tls.has_value());
  ASSERT_TRUE(good_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());

  grpcxx::ClientChannelService::Options good_opts;
  good_opts.endpoint = *ep;
  good_opts.tls = &*good_tls;
  good_opts.server_name = "localhost";
  rig.client = std::make_unique<grpcxx::ClientChannelService>(*rig.ring, rig.routes, good_opts);
  ASSERT_TRUE(rig.client->Start().has_value());
  for (unsigned tick = 0;
       tick < 20000 && rig.client->GetState() != grpcxx::ClientChannelService::ChannelState::kReady;
       ++tick) {
    ASSERT_TRUE(rig.Step());
  }
  ASSERT_EQ(rig.client->GetState(), grpcxx::ClientChannelService::ChannelState::kReady);

  const std::array<std::byte, 2> body{std::byte{0xaa}, std::byte{0x55}};
  rig.completed = false;
  ASSERT_TRUE(rig.client->CallUnary("/echo/Unary", body, &rig, &Rig::OnResponse).has_value());
  for (unsigned tick = 0; tick < 20000 && !rig.completed; ++tick) ASSERT_TRUE(rig.Step());
  ASSERT_TRUE(rig.completed);
  EXPECT_EQ(rig.status, 0);
  EXPECT_TRUE(std::ranges::equal(rig.received, body));
}

}  // namespace
