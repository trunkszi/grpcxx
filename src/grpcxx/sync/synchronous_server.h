#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <nexora/error/error_codes.hpp>
#include <nexora/socket/memory_pipe.hpp>
#include <nexora/socket/tcp_transport.hpp>
#include <nexora/socket/unix_socket.hpp>
#include <nexora/tls/tls_config.hpp>
#include <optional>
#include <span>
#include <string_view>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"

namespace grpcxx {

struct ServerRequest final {
  enum class Event : std::uint8_t { kMessage = 0, kHalfClose = 1, kCancel = 2 };

  std::int32_t stream_id{0};
  std::string_view path{};
  std::span<const std::byte> payload{};
  const MetadataMap& metadata;
  Event event{Event::kMessage};
  std::optional<std::uint64_t> timeout_ns{};
};

struct SynchronousServerOptions final {
  std::size_t max_message_bytes{16uz * 1024uz * 1024uz};
  std::size_t max_queued_bytes{32uz * 1024uz * 1024uz};
  std::size_t max_metadata_bytes{65536uz};
  std::uint32_t max_concurrent_streams{32U};
  std::uint32_t flow_control_window_bytes{16U * 1024U * 1024U};
  CompressionAlgorithm response_compression{CompressionAlgorithm::kIdentity};

  [[nodiscard]] std::expected<void, Status> Validate() const noexcept;
};

/**
 * Single-connection caller-thread HTTP/2 gRPC server session over `nexora::Http2Connection`.
 */
class SynchronousServer final {
 public:
  struct Impl;
  using HandlerDelegate = InplaceDelegate<void(SynchronousServer&, const ServerRequest&) noexcept>;
  using RawHandler = void (*)(void*, SynchronousServer&, const ServerRequest&) noexcept;
  using HeadersDelegate = InplaceDelegate<void(SynchronousServer&, std::int32_t, std::string_view,
                                               const MetadataMap&) noexcept>;

  SynchronousServer(const SynchronousServer&) =
      delete("SynchronousServer owns single-owner HTTP/2 session state; copying is disabled");
  SynchronousServer& operator=(const SynchronousServer&) =
      delete("SynchronousServer owns single-owner HTTP/2 session state; copying is disabled");
  SynchronousServer(SynchronousServer&&) =
      delete("SynchronousServer is pinned to inline callbacks; moving is disabled");
  SynchronousServer& operator=(SynchronousServer&&) =
      delete("SynchronousServer is pinned to inline callbacks; moving is disabled");
  ~SynchronousServer() noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromMemoryPipe(
      nexora::MemoryPipe pipe, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromMemoryPipe(
      nexora::MemoryPipe pipe, RawHandler handler, void* context,
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromMemoryPipeTls(
      nexora::MemoryPipe pipe, const nexora::TlsServerConfig& tls, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromTcp(
      nexora::TcpTransport tcp, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromTcp(
      nexora::TcpTransport tcp, RawHandler handler, void* context,
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> AcceptTcp(
      nexora::TcpListener& listener, std::chrono::milliseconds accept_timeout,
      HandlerDelegate handler = {}, SynchronousServerOptions options = {}) noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromTls(
      nexora::TcpTransport tcp, const nexora::TlsServerConfig& tls,
      std::chrono::milliseconds handshake_timeout, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> AcceptTls(
      nexora::TcpListener& listener, const nexora::TlsServerConfig& tls,
      std::chrono::milliseconds timeout, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;

  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> FromUnix(
      nexora::UnixSocket sock, HandlerDelegate handler = {},
      SynchronousServerOptions options = {}) noexcept;
  [[nodiscard]] static std::expected<std::unique_ptr<SynchronousServer>, Status> AcceptUnix(
      nexora::UnixListener& listener, std::chrono::milliseconds accept_timeout,
      HandlerDelegate handler = {}, SynchronousServerOptions options = {}) noexcept;

  void RegisterHandler(HandlerDelegate handler) noexcept;
  void RegisterHandler(RawHandler handler, void* context) noexcept;
  void SetHeadersHandler(HeadersDelegate handler) noexcept;

  [[nodiscard]] std::expected<void, Status> SetInitialMetadata(
      std::int32_t stream_id, const MetadataMap& metadata) noexcept;
  [[nodiscard]] std::expected<void, Status> SendInitialMetadata(
      std::int32_t stream_id, const MetadataMap& metadata = {}) noexcept;
  [[nodiscard]] std::expected<void, Status> Send(std::int32_t stream_id,
                                                 std::span<const std::byte> payload) noexcept;
  [[nodiscard]] std::expected<void, Status> Finish(std::int32_t stream_id, Status status,
                                                   const MetadataMap& trailers = {}) noexcept;
  [[nodiscard]] std::expected<void, Status> FinishTrailersOnly(
      std::int32_t stream_id, Status status, const MetadataMap& trailers = {}) noexcept;
  [[nodiscard]] std::expected<void, Status> ResetStream(
      std::int32_t stream_id,
      nexora::Http2ErrorCode error = nexora::Http2ErrorCode::kCancel) noexcept;

  [[nodiscard]] std::expected<void, Status> PollOnce() noexcept;
  void Close() noexcept;
  [[nodiscard]] bool IsOpen() const noexcept;
  [[nodiscard]] std::size_t CancelledStreams() const noexcept;
  [[nodiscard]] std::size_t ActiveStreams() const noexcept;
  [[nodiscard]] std::size_t WindowUpdatesReceived() const noexcept;
  [[nodiscard]] int NativeHandle() const noexcept;

  explicit SynchronousServer(std::unique_ptr<Impl> impl) noexcept;

 private:
  std::unique_ptr<Impl> impl_{};
};

}  // namespace grpcxx
