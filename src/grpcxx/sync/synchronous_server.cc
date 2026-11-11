#include <array>
#include <limits>
#include <memory>
#include <utility>

#include "grpcxx/h2/grpc_http2_headers.h"
#include "grpcxx/sync/synchronous_server_internal.h"

namespace grpcxx {
namespace {

[[nodiscard]] std::uint32_t ClampAcceptTimeoutMs(std::chrono::milliseconds d) noexcept {
  if (d.count() <= 0) return 1U;
  if (d.count() > static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())) {
    return std::numeric_limits<std::uint32_t>::max();
  }
  return static_cast<std::uint32_t>(d.count());
}

[[nodiscard]] std::expected<std::unique_ptr<SynchronousServer>, Status> BuildServerFromWire(
    std::expected<sync_detail::SyncWireTransport, Status> wire,
    SynchronousServer::HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  if (!wire) return std::unexpected(std::move(wire.error()));
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  auto impl = std::make_unique<SynchronousServer::Impl>();
  impl->transport = std::move(*wire);
  impl->options = options;
  impl->handler = std::move(handler);
  if (auto ready = impl->Initialize(); !ready) return std::unexpected(std::move(ready.error()));
  return std::make_unique<SynchronousServer>(std::move(impl));
}

[[nodiscard]] SynchronousServer::HandlerDelegate WrapRawHandler(SynchronousServer::RawHandler fn,
                                                                void* ctx) noexcept {
  if (fn == nullptr) return SynchronousServer::HandlerDelegate{};
  return SynchronousServer::HandlerDelegate{
      [fn, ctx](SynchronousServer& srv, const ServerRequest& req) noexcept { fn(ctx, srv, req); }};
}

}  // namespace

std::expected<void, Status> SynchronousServerOptions::Validate() const noexcept {
  if (max_message_bytes == 0uz || max_message_bytes > std::numeric_limits<std::uint32_t>::max() ||
      max_queued_bytes < max_message_bytes + 5uz || max_metadata_bytes == 0uz ||
      max_concurrent_streams == 0U || max_concurrent_streams > 64U ||
      flow_control_window_bytes < 65535U || flow_control_window_bytes > 0x7fffffffU) {
    return std::unexpected(
        MakeSrvError(StatusCode::kInvalidArgument, "invalid server capacity or window options"));
  }
  return {};
}

SynchronousServer::SynchronousServer(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {
  impl_->owner_server = this;
}

SynchronousServer::~SynchronousServer() noexcept { Close(); }

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromMemoryPipe(
    nexora::MemoryPipe pipe, HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  return BuildServerFromWire(sync_detail::SyncWireTransport::FromMemoryPipe(std::move(pipe)),
                             std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromMemoryPipe(
    nexora::MemoryPipe pipe, RawHandler handler, void* context,
    SynchronousServerOptions options) noexcept {
  return FromMemoryPipe(std::move(pipe), WrapRawHandler(handler, context), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromMemoryPipeTls(
    nexora::MemoryPipe pipe, const nexora::TlsServerConfig& tls, HandlerDelegate handler,
    SynchronousServerOptions options) noexcept {
  return BuildServerFromWire(
      sync_detail::SyncWireTransport::FromMemoryPipeTlsServer(std::move(pipe), tls),
      std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromTcp(
    nexora::TcpTransport tcp, HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  return BuildServerFromWire(sync_detail::SyncWireTransport::FromTcp(std::move(tcp)),
                             std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromTcp(
    nexora::TcpTransport tcp, RawHandler handler, void* context,
    SynchronousServerOptions options) noexcept {
  return FromTcp(std::move(tcp), WrapRawHandler(handler, context), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::AcceptTcp(
    nexora::TcpListener& listener, std::chrono::milliseconds accept_timeout,
    HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  auto accepted = listener.Accept(ClampAcceptTimeoutMs(accept_timeout));
  if (!accepted) {
    return std::unexpected(MakeSrvError(StatusCode::kUnavailable, accepted.error().Message()));
  }
  return FromTcp(std::move(*accepted), std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromTls(
    nexora::TcpTransport tcp, const nexora::TlsServerConfig& tls,
    std::chrono::milliseconds handshake_timeout, HandlerDelegate handler,
    SynchronousServerOptions options) noexcept {
  return BuildServerFromWire(
      sync_detail::SyncWireTransport::FromTlsServer(std::move(tcp), tls, handshake_timeout),
      std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::AcceptTls(
    nexora::TcpListener& listener, const nexora::TlsServerConfig& tls,
    std::chrono::milliseconds timeout, HandlerDelegate handler,
    SynchronousServerOptions options) noexcept {
  auto accepted = listener.Accept(ClampAcceptTimeoutMs(timeout));
  if (!accepted) {
    return std::unexpected(MakeSrvError(StatusCode::kUnavailable, accepted.error().Message()));
  }
  return FromTls(std::move(*accepted), tls, timeout, std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::FromUnix(
    nexora::UnixSocket sock, HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  return BuildServerFromWire(sync_detail::SyncWireTransport::FromUnix(std::move(sock)),
                             std::move(handler), options);
}

std::expected<std::unique_ptr<SynchronousServer>, Status> SynchronousServer::AcceptUnix(
    nexora::UnixListener& listener, std::chrono::milliseconds accept_timeout,
    HandlerDelegate handler, SynchronousServerOptions options) noexcept {
  auto accepted = listener.Accept(ClampAcceptTimeoutMs(accept_timeout));
  if (!accepted) {
    return std::unexpected(MakeSrvError(StatusCode::kUnavailable, accepted.error().Message()));
  }
  return FromUnix(std::move(*accepted), std::move(handler), options);
}

void SynchronousServer::RegisterHandler(HandlerDelegate handler) noexcept {
  impl_->handler = std::move(handler);
}

void SynchronousServer::RegisterHandler(RawHandler handler, void* context) noexcept {
  impl_->handler = WrapRawHandler(handler, context);
}

void SynchronousServer::SetHeadersHandler(HeadersDelegate handler) noexcept {
  impl_->headers_handler = std::move(handler);
}

std::expected<void, Status> SynchronousServer::SetInitialMetadata(
    std::int32_t stream_id, const MetadataMap& metadata) noexcept {
  auto* slot = impl_->Find(stream_id);
  if (uarch::unlikely(slot == nullptr || slot->IsStarted())) {
    return std::unexpected(
        MakeSrvError(StatusCode::kFailedPrecondition, "response headers already committed"));
  }
  auto check = h2::BuildGrpcResponseHeaders(slot->response_encoding, &metadata, "application/grpc",
                                            impl_->options.max_metadata_bytes);
  if (!check) return std::unexpected(std::move(check.error()));
  slot->initial = metadata;
  return {};
}

std::expected<void, Status> SynchronousServer::SendInitialMetadata(
    std::int32_t stream_id, const MetadataMap& metadata) noexcept {
  if (!metadata.Entries().empty()) {
    if (auto set_ok = SetInitialMetadata(stream_id, metadata); !set_ok) return set_ok;
  }
  auto* slot = impl_->Find(stream_id);
  if (uarch::unlikely(slot == nullptr)) {
    return std::unexpected(MakeSrvError(StatusCode::kFailedPrecondition, "unknown stream id"));
  }
  return impl_->EnsureStarted(*slot);
}

std::expected<void, Status> SynchronousServer::Send(std::int32_t stream_id,
                                                    std::span<const std::byte> payload) noexcept {
  auto* slot = impl_->Find(stream_id);
  if (uarch::unlikely(slot == nullptr || slot->IsFinished())) {
    return std::unexpected(MakeSrvError(StatusCode::kFailedPrecondition, "response stream closed"));
  }
  if (uarch::unlikely(payload.size() > impl_->options.max_message_bytes)) {
    return std::unexpected(
        MakeSrvError(StatusCode::kResourceExhausted, "response message exceeds limit"));
  }
  auto encoded = impl_->compression.Compress(slot->response_encoding, payload);
  if (uarch::unlikely(!encoded.has_value())) return std::unexpected(std::move(encoded.error()));
  if (uarch::unlikely(encoded->WireSize() > impl_->options.max_queued_bytes ||
                      slot->queued_bytes > impl_->options.max_queued_bytes ||
                      encoded->WireSize() > impl_->options.max_queued_bytes - slot->queued_bytes)) {
    return std::unexpected(MakeSrvError(StatusCode::kResourceExhausted, "response queue is full"));
  }
  if (auto started = impl_->EnsureStarted(*slot); !started) return started;
  slot->queued_bytes += encoded->WireSize();
  slot->output.push_back(std::move(*encoded));
  return impl_->PumpOutgoing();
}

std::expected<void, Status> SynchronousServer::Finish(std::int32_t stream_id, Status status,
                                                      const MetadataMap& trailers) noexcept {
  auto* slot = impl_->Find(stream_id);
  if (uarch::unlikely(slot == nullptr || slot->IsFinished())) {
    return std::unexpected(MakeSrvError(StatusCode::kFailedPrecondition, "response stream closed"));
  }
  auto check = h2::BuildGrpcTrailers(status, &trailers, impl_->options.max_metadata_bytes);
  if (uarch::unlikely(!check.has_value())) return std::unexpected(std::move(check.error()));
  if (auto started = impl_->EnsureStarted(*slot); !started) return started;
  slot->status = std::move(status);
  slot->trailers = trailers;
  slot->meta = slot->meta.with<^^ServerStreamMetaSpec::finished>(true);
  return impl_->PumpOutgoing();
}

std::expected<void, Status> SynchronousServer::FinishTrailersOnly(
    std::int32_t stream_id, Status status, const MetadataMap& trailers) noexcept {
  auto* slot = impl_->Find(stream_id);
  if (uarch::unlikely(slot == nullptr || slot->IsStarted() || slot->IsFinished())) {
    return std::unexpected(MakeSrvError(StatusCode::kFailedPrecondition,
                                        "trailers-only requires uncommitted response stream"));
  }
  auto built_trailers = h2::BuildGrpcTrailers(status, &trailers, impl_->options.max_metadata_bytes);
  if (!built_trailers) return std::unexpected(std::move(built_trailers.error()));
  nexora::Headers<> combined{};
  if (auto ok = h2::AppendHeaderChecked(combined, "content-type", "application/grpc"); !ok) {
    return ok;
  }
  for (const auto& field : built_trailers->Fields()) {
    if (auto ok = h2::AppendHeaderChecked(combined, field.name.View(), field.value.View()); !ok) {
      return ok;
    }
  }
  if (impl_->OutputFreeBytes() < 65536uz) {
    if (auto flushed = impl_->FlushWire(); !flushed) return flushed;
  }
  auto submitted = impl_->h2->SubmitResponseHeaders(static_cast<std::uint32_t>(slot->id),
                                                    nexora::StatusCode{200U}, combined, true);
  if (!submitted) return std::unexpected(MapSrvH2Error(submitted.error()));
  slot->meta = slot->meta.with<^^ServerStreamMetaSpec::started>(true)
                   .with<^^ServerStreamMetaSpec::finished>(true)
                   .with<^^ServerStreamMetaSpec::trailers_submitted>(true);
  impl_->MaybeReclaimCompleted(*slot);
  return {};
}

std::expected<void, Status> SynchronousServer::ResetStream(std::int32_t stream_id,
                                                           nexora::Http2ErrorCode error) noexcept {
  auto* slot = impl_->Find(stream_id);
  if (slot == nullptr) return {};
  impl_->RejectStream(*slot, error);
  return {};
}

std::expected<void, Status> SynchronousServer::PollOnce() noexcept {
  if (uarch::unlikely(impl_->polling)) {
    return std::unexpected(MakeSrvError(StatusCode::kFailedPrecondition, "reentrant server poll"));
  }
  if (uarch::unlikely(impl_->closed)) return {};
  impl_->polling = true;
  const auto res = [this]() noexcept -> std::expected<void, Status> {
    const auto peer_or = [this](Status err) noexcept -> std::expected<void, Status> {
      if (Impl::IsPeerTransportClosed(err)) return impl_->FinishPeerClose();
      return std::unexpected(std::move(err));
    };
    if (auto pumped = impl_->PumpOutgoing(); !pumped) {
      return peer_or(std::move(pumped.error()));
    }
    if (auto flushed = impl_->FlushWire(); !flushed) return peer_or(std::move(flushed.error()));

    for (std::size_t iter = 0uz; iter < 64uz; ++iter) {
      auto read_res = impl_->transport.ReadSome(impl_->rx_scratch);
      if (!read_res) return peer_or(std::move(read_res.error()));
      if (read_res->eof != 0U) {
        impl_->CloseOnPeerEof();
        return {};
      }
      if (read_res->would_block != 0U || read_res->bytes == 0uz) break;
      const auto bytes_u8 =
          sync_detail::AsUint8Span(std::span<std::byte>{impl_->rx_scratch.data(), read_res->bytes});
      if (auto fed = impl_->FeedInboundBytes(bytes_u8); uarch::unlikely(!fed.has_value())) {
        return fed;
      }
      if (impl_->closed) return {};
      if (auto pumped_mid = impl_->PumpOutgoing(); !pumped_mid) {
        return peer_or(std::move(pumped_mid.error()));
      }
      if (auto flushed_mid = impl_->FlushWire(); !flushed_mid) {
        return peer_or(std::move(flushed_mid.error()));
      }
    }

    if (impl_->closed) return {};
    impl_->DrainEvents();
    if (uarch::unlikely(!impl_->fatal_error.Ok())) return std::unexpected(impl_->fatal_error);
    if (auto pumped = impl_->PumpOutgoing(); !pumped) return peer_or(std::move(pumped.error()));
    if (auto flushed = impl_->FlushWire(); !flushed) return peer_or(std::move(flushed.error()));
    return {};
  }();
  impl_->polling = false;
  return res;
}

void SynchronousServer::Close() noexcept {
  if (!impl_ || impl_->closed) return;
  if (impl_->h2 && !impl_->peer_eof) {
    auto goaway =
        impl_->h2->SubmitGoaway(impl_->last_peer_stream_id, nexora::Http2ErrorCode::kNoError);
    if (!goaway) {
      impl_->fatal_error = MapSrvH2Error(goaway.error());
    } else if (auto flushed = impl_->FlushWire(); !flushed) {
      impl_->fatal_error = std::move(flushed.error());
    }
  }
  impl_->transport.Close();
  impl_->active_streams_mask = uarch::Bitmap64{};
  impl_->closed = true;
}

bool SynchronousServer::IsOpen() const noexcept {
  return impl_ != nullptr && !impl_->closed && !impl_->peer_eof && impl_->transport.IsOpen();
}

std::size_t SynchronousServer::CancelledStreams() const noexcept {
  return impl_ ? impl_->cancelled_streams : 0uz;
}

std::size_t SynchronousServer::ActiveStreams() const noexcept {
  return impl_ ? impl_->active_streams_mask.count() : 0uz;
}

std::size_t SynchronousServer::WindowUpdatesReceived() const noexcept {
  return impl_ ? impl_->window_updates_received : 0uz;
}

int SynchronousServer::NativeHandle() const noexcept {
  return impl_ ? impl_->transport.NativeHandle() : -1;
}

}  // namespace grpcxx
