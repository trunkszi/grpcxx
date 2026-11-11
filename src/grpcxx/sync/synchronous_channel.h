#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <nexora/socket/memory_pipe.hpp>
#include <nexora/socket/tcp_transport.hpp>
#include <nexora/socket/unix_socket.hpp>
#include <nexora/tls/tls_config.hpp>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/core/status.h"
#include "grpcxx/reflection/annotations.h"
#include "grpcxx/sync/call_options.h"
#include "grpcxx/sync/rpc_stream.h"

namespace grpcxx {

template <typename M>
concept SyncWireMessageLike = requires(const M& cm, M& m, void* out, const void* in, int sz) {
  { cm.ByteSizeLong() } -> std::convertible_to<std::size_t>;
  { cm.SerializeToArray(out, sz) } -> std::convertible_to<bool>;
  { m.ParseFromArray(in, sz) } -> std::convertible_to<bool>;
};

/**
 * Caller-thread synchronous and streaming gRPC HTTP/2 client channel over
 * `nexora::Http2Connection`.
 */
class SynchronousChannel final {
 public:
  struct Impl;
  using ProgressDelegate = InplaceDelegate<std::expected<void, Status>() noexcept>;
  using ProgressHook = std::expected<void, Status> (*)(void*) noexcept;

  SynchronousChannel(const SynchronousChannel&) =
      delete("SynchronousChannel is pinned to active stream handles; copying is disabled");
  SynchronousChannel& operator=(const SynchronousChannel&) =
      delete("SynchronousChannel is pinned to active stream handles; copying is disabled");
  SynchronousChannel(SynchronousChannel&&) =
      delete("SynchronousChannel is referenced by active RpcStream handles; moving is disabled");
  SynchronousChannel& operator=(SynchronousChannel&&) =
      delete("SynchronousChannel is referenced by active RpcStream handles; moving is disabled");
  ~SynchronousChannel() noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> Connect(
      std::string_view endpoint_uri, const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> Connect(
      std::string_view endpoint_uri, const SyncTlsClientOptions& tls,
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> Connect(
      const SyncEndpoint& endpoint, const SynchronousChannelOptions& options = {},
      const SyncTlsClientOptions& tls = {}) noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> ConnectTcp(
      std::string_view host, std::uint16_t port,
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> ConnectTls(
      std::string_view host, std::uint16_t port, const nexora::TlsClientConfig& tls,
      std::string_view server_name = {}, const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> ConnectTls(
      std::string_view host, std::uint16_t port, const SyncTlsClientOptions& tls,
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> ConnectUnix(
      std::string_view path, const SynchronousChannelOptions& options = {}) noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> AdoptTcp(
      int fd, std::string_view authority = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> AdoptUnix(
      int fd, std::string_view authority = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> FromTcp(
      nexora::TcpTransport tcp, std::string_view authority = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> FromUnix(
      nexora::UnixSocket sock, std::string_view authority = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> FromMemoryPipe(
      nexora::MemoryPipe pipe, std::string_view authority = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousChannel>, Status> FromMemoryPipeTls(
      nexora::MemoryPipe pipe, const nexora::TlsClientConfig& tls,
      std::string_view server_name = "localhost",
      const SynchronousChannelOptions& options = {}) noexcept;

  [[nodiscard]] std::expected<std::unique_ptr<RpcStream>, Status> Open(
      std::string_view path, RpcKind kind, CallOptions options = {}) noexcept;
  [[nodiscard]] std::expected<RpcResponse, Status> Unary(std::string_view path,
                                                         std::span<const std::byte> payload,
                                                         CallOptions options = {}) noexcept;
  [[nodiscard]] std::expected<std::unique_ptr<RpcStream>, Status> ClientStreamCall(
      std::string_view path, CallOptions options = {}) noexcept;
  [[nodiscard]] std::expected<std::unique_ptr<RpcStream>, Status> ServerStreamCall(
      std::string_view path, std::span<const std::byte> payload, CallOptions options = {}) noexcept;
  [[nodiscard]] std::expected<std::unique_ptr<RpcStream>, Status> BidiStreamCall(
      std::string_view path, CallOptions options = {}) noexcept;

  template <SyncWireMessageLike Req, SyncWireMessageLike Resp>
  [[nodiscard]] std::expected<Response<Resp>, Status> UnaryProto(
      std::string_view path, const Req& request, CallOptions options = {}) noexcept {
    const std::size_t byte_len = request.ByteSizeLong();
    if (byte_len > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
      return std::unexpected(Status{StatusCode::kResourceExhausted, "protobuf size exceeds limit"});
    }
    std::vector<std::byte> encoded(byte_len);
    if (byte_len > 0uz && !request.SerializeToArray(encoded.data(), static_cast<int>(byte_len))) {
      return std::unexpected(Status{StatusCode::kInternal, "protobuf serialization failed"});
    }
    auto rpc = Unary(path, encoded, std::move(options));
    if (!rpc) return std::unexpected(std::move(rpc.error()));
    if (!rpc->status.Ok()) return std::unexpected(std::move(rpc->status));
    if (rpc->payload.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
      return std::unexpected(Status{StatusCode::kInternal, "response exceeds protobuf limit"});
    }
    Response<Resp> typed{};
    const void* src = rpc->payload.empty() ? static_cast<const void*>("") : rpc->payload.data();
    if (!typed.message.ParseFromArray(src, static_cast<int>(rpc->payload.size()))) {
      return std::unexpected(Status{StatusCode::kInternal, "invalid protobuf response"});
    }
    typed.initial_metadata = std::move(rpc->initial_metadata);
    typed.trailing_metadata = std::move(rpc->trailing_metadata);
    return typed;
  }

  void Close() noexcept;
  [[nodiscard]] bool IsOpen() const noexcept;
  [[nodiscard]] std::expected<void, Status> PollOnce() noexcept;
  void SetProgressHook(ProgressDelegate hook) noexcept;
  void SetProgressHook(void* context, ProgressHook hook) noexcept;
  [[nodiscard]] std::size_t ActiveStreams() const noexcept;
  [[nodiscard]] std::size_t WindowUpdatesReceived() const noexcept;
  [[nodiscard]] int NativeHandle() const noexcept;

  explicit SynchronousChannel(std::unique_ptr<Impl> impl) noexcept;

 private:
  friend class RpcStream;

  std::unique_ptr<Impl> impl_{};
};

}  // namespace grpcxx
