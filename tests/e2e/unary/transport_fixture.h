#pragma once
#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/server/grpc_server_service.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <memory>
#include <vector>
namespace grpcxx::test {
struct Harness {
  uring::Result<uring::Proactor> ring{uring::Proactor::Create(256)};
  nexora::CompletionRoutes<> routes;
  std::optional<cares::Resolver<>> resolver;
  std::unique_ptr<grpcxx::GrpcServerService> server;
  std::unique_ptr<grpcxx::ClientChannelService> client;
  std::optional<nexora::TlsClientConfig> client_tls;
  std::optional<nexora::TlsServerConfig> server_tls;
  std::uint64_t generation{};
  std::size_t connection{};
  std::int32_t stream{};
  bool deferred{}, completed{}, valid{true}, reject_reentry{true};
  int status{-1};
  bool streaming{}, aggregate{}, finish_pending{}, dns{};
  std::vector<std::vector<std::byte>> replies, messages;
  std::size_t next_reply{};
  static void StreamRequest(void* context, grpcxx::GrpcServerService&, std::size_t connection,
                            std::int32_t stream, std::uint64_t generation, std::string_view,
                            std::span<const std::byte> payload, bool end) noexcept {
    auto& self = *static_cast<Harness*>(context);
    self.connection = connection;
    self.stream = stream;
    self.generation = generation;
    if (end) {
      if (self.aggregate)
        self.replies.push_back({std::byte{static_cast<unsigned char>(self.messages.size())}});
      self.finish_pending = true;
    } else {
      self.messages.emplace_back(payload.begin(), payload.end());
      if (!self.aggregate) self.replies.emplace_back(payload.begin(), payload.end());
    }
  }
  std::vector<std::byte> received;
  static void Request(void* context, grpcxx::GrpcServerService& service, std::size_t connection,
                      std::int32_t stream, std::uint64_t generation, std::string_view path,
                      std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Harness*>(context);
    self.valid &= path == "/echo/Unary";
    self.connection = connection;
    self.stream = stream;
    self.generation = generation;
    auto nested = service.PollOnce({}, 1);
    self.reject_reentry &=
        !nested && nested.error().Code() == grpcxx::StatusCode::kFailedPrecondition;
    if (!self.deferred)
      self.valid &= service.Complete(connection, stream, generation, payload).has_value();
  }
  static void Response(void* context, std::int32_t, int status,
                       std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Harness*>(context);
    self.status = status;
    self.completed = true;
    self.received.assign(payload.begin(), payload.end());
    auto nested = self.client->PollOnce({}, 1);
    self.reject_reentry &=
        !nested && nested.error().Code() == grpcxx::StatusCode::kFailedPrecondition;
  }
  bool Start(bool secure) {
    if (!ring) return false;
    if (secure) {
      auto server_config = nexora::TlsServerConfig::SelfSignedForLocalhost();
      auto client_config = nexora::TlsClientConfig::CreateInsecureForTest();
      if (!server_config || !client_config ||
          !server_config->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2) ||
          !client_config->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2))
        return false;
      server_tls.emplace(std::move(*server_config));
      client_tls.emplace(std::move(*client_config));
    }
    grpcxx::GrpcServerService::Options options;
    options.context = this;
    options.handler = &Request;
    if (streaming) options.stream_handler = &StreamRequest;
    options.tls = server_tls ? &*server_tls : nullptr;
    server = std::make_unique<grpcxx::GrpcServerService>(*ring, routes, options);
    if (!server->Start()) return false;
    auto endpoint = server->LocalAddress();
    if (!endpoint) return false;
    grpcxx::ClientChannelService::Options client_options;
    client_options.endpoint = *endpoint;
    client_options.tls = client_tls ? &*client_tls : nullptr;
    if (dns) {
      auto created = cares::Resolver<>::FromSystem(*ring);
      if (!created) return false;
      resolver.emplace(std::move(*created));
      client_options.target = "localhost";
      client_options.port = endpoint->port;
      client =
          std::make_unique<grpcxx::ClientChannelService>(*ring, routes, *resolver, client_options);
    } else
      client = std::make_unique<grpcxx::ClientChannelService>(*ring, routes, client_options);
    if (!client->Start()) return false;
    for (unsigned tick = 0; tick < 20000; ++tick) {
      if (!Step()) return false;
      if (client->GetState() == grpcxx::ClientChannelService::ChannelState::kReady) return true;
    }
    return false;
  }
  bool Step() {
    auto drive = ring->Tick(routes);
    if (!drive || !server->PollOnce({}, 1) || !client->PollOnce({}, 1)) return false;
    if (streaming && generation) {
      if (next_reply < replies.size()) {
        auto sent = server->SendMessage(connection, stream, generation, replies[next_reply]);
        if (sent)
          ++next_reply;
        else if (sent.error().Code() != grpcxx::StatusCode::kResourceExhausted)
          return false;
      } else if (finish_pending) {
        auto finished = server->Finish(connection, stream, generation);
        if (!finished) return false;
        finish_pending = false;
      }
    }
    return true;
  }
  bool Wait() {
    for (unsigned tick = 0; tick < 100000 && !completed; ++tick)
      if (!Step()) return false;
    return completed;
  }
  ~Harness() {
    if (!ring) return;
    if (client) static_cast<void>(client->BeginShutdown(0));
    if (server) static_cast<void>(server->BeginShutdown(0));
    for (unsigned tick = 0; tick < 100000; ++tick) {
      static_cast<void>(ring->Tick(routes));
      if (client) static_cast<void>(client->PollShutdown({}, 1));
      if (server) static_cast<void>(server->PollShutdown({}, 1));
      if ((!client || client->Stopped()) && (!server || server->Stopped())) break;
    }
  }
};
}  // namespace grpcxx::test
