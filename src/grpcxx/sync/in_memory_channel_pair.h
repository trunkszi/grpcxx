#pragma once

#include <cstddef>
#include <expected>
#include <memory>
#include <nexora/socket/memory_pipe.hpp>
#include <nexora/tls/tls_config.hpp>
#include <string_view>
#include <utility>

#include "grpcxx/core/status.h"
#include "grpcxx/sync/synchronous_channel.h"
#include "grpcxx/sync/synchronous_server.h"

namespace grpcxx {

/**
 * Connected caller-thread `SynchronousChannel` + `SynchronousServer` pair over
 * `nexora::MemoryPipe`.
 *
 * `server_` outlives `channel_` so any active `RpcStream` teardown can flush `RST_STREAM` before
 * the in-memory server closes.
 */
class InMemoryChannelPair final {
 public:
  InMemoryChannelPair(const InMemoryChannelPair&) =
      delete("InMemoryChannelPair owns live endpoints; copying is disabled");
  InMemoryChannelPair& operator=(const InMemoryChannelPair&) =
      delete("InMemoryChannelPair owns live endpoints; copying is disabled");
  InMemoryChannelPair(InMemoryChannelPair&&) =
      delete("InMemoryChannelPair wires progress hooks to server pointer; moving is disabled");
  InMemoryChannelPair& operator=(InMemoryChannelPair&&) =
      delete("InMemoryChannelPair wires progress hooks to server pointer; moving is disabled");
  ~InMemoryChannelPair() noexcept = default;

  [[nodiscard]] static std::expected<std::unique_ptr<InMemoryChannelPair>, Status> Create(
      SynchronousServer::HandlerDelegate handler = {}, SynchronousServerOptions server_options = {},
      SynchronousChannelOptions channel_options = {},
      std::size_t pipe_capacity = 262144uz) noexcept {
    auto pipes = nexora::MemoryPipe::CreatePair(pipe_capacity);
    if (!pipes) {
      return std::unexpected(
          Status{StatusCode::kResourceExhausted, std::string{pipes.error().Message()}});
    }
    auto server = SynchronousServer::FromMemoryPipe(std::move(pipes->server), std::move(handler),
                                                    server_options);
    if (!server) return std::unexpected(std::move(server.error()));
    auto channel =
        SynchronousChannel::FromMemoryPipe(std::move(pipes->client), "localhost", channel_options);
    if (!channel) return std::unexpected(std::move(channel.error()));
    SynchronousServer* srv_ptr = server->get();
    (*channel)->SetProgressHook([srv_ptr]() noexcept { return srv_ptr->PollOnce(); });
    return std::make_unique<InMemoryChannelPair>(std::move(*server), std::move(*channel));
  }

  [[nodiscard]] static std::expected<std::unique_ptr<InMemoryChannelPair>, Status> Create(
      SynchronousServer::RawHandler handler, void* context,
      SynchronousServerOptions server_options = {}, SynchronousChannelOptions channel_options = {},
      std::size_t pipe_capacity = 262144uz) noexcept {
    auto pipes = nexora::MemoryPipe::CreatePair(pipe_capacity);
    if (!pipes) {
      return std::unexpected(
          Status{StatusCode::kResourceExhausted, std::string{pipes.error().Message()}});
    }
    auto server = SynchronousServer::FromMemoryPipe(std::move(pipes->server), handler, context,
                                                    server_options);
    if (!server) return std::unexpected(std::move(server.error()));
    auto channel =
        SynchronousChannel::FromMemoryPipe(std::move(pipes->client), "localhost", channel_options);
    if (!channel) return std::unexpected(std::move(channel.error()));
    SynchronousServer* srv_ptr = server->get();
    (*channel)->SetProgressHook([srv_ptr]() noexcept { return srv_ptr->PollOnce(); });
    return std::make_unique<InMemoryChannelPair>(std::move(*server), std::move(*channel));
  }

  [[nodiscard]] static std::expected<std::unique_ptr<InMemoryChannelPair>, Status> CreateTls(
      const nexora::TlsServerConfig& server_tls, const nexora::TlsClientConfig& client_tls,
      SynchronousServer::HandlerDelegate handler = {}, std::string_view server_name = "localhost",
      SynchronousServerOptions server_options = {}, SynchronousChannelOptions channel_options = {},
      std::size_t pipe_capacity = 262144uz) noexcept {
    auto pipes = nexora::MemoryPipe::CreatePair(pipe_capacity);
    if (!pipes) {
      return std::unexpected(
          Status{StatusCode::kResourceExhausted, std::string{pipes.error().Message()}});
    }
    auto server = SynchronousServer::FromMemoryPipeTls(std::move(pipes->server), server_tls,
                                                       std::move(handler), server_options);
    if (!server) return std::unexpected(std::move(server.error()));
    auto channel = SynchronousChannel::FromMemoryPipeTls(std::move(pipes->client), client_tls,
                                                         server_name, channel_options);
    if (!channel) return std::unexpected(std::move(channel.error()));
    SynchronousServer* srv_ptr = server->get();
    (*channel)->SetProgressHook([srv_ptr]() noexcept { return srv_ptr->PollOnce(); });
    return std::make_unique<InMemoryChannelPair>(std::move(*server), std::move(*channel));
  }

  InMemoryChannelPair(std::unique_ptr<SynchronousServer> server,
                      std::unique_ptr<SynchronousChannel> channel) noexcept
      : server_(std::move(server)), channel_(std::move(channel)) {}

  [[nodiscard]] SynchronousChannel& Channel() noexcept { return *channel_; }
  [[nodiscard]] const SynchronousChannel& Channel() const noexcept { return *channel_; }
  [[nodiscard]] SynchronousServer& Server() noexcept { return *server_; }
  [[nodiscard]] const SynchronousServer& Server() const noexcept { return *server_; }

 private:
  std::unique_ptr<SynchronousServer> server_{};
  std::unique_ptr<SynchronousChannel> channel_{};
};

}  // namespace grpcxx
