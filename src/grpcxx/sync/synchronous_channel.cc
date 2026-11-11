#include <chrono>
#include <memory>
#include <string>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/h2/grpc_http2_headers.h"
#include "grpcxx/sync/synchronous_channel_internal.h"

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeChanError(StatusCode code,
                                                             std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard]] std::expected<std::unique_ptr<SynchronousChannel>, Status> BuildChannelFromWire(
    std::expected<sync_detail::SyncWireTransport, Status> wire, std::string_view authority,
    const SynchronousChannelOptions& options) noexcept {
  if (!wire) return std::unexpected(std::move(wire.error()));
  if (auto valid = options.Validate(); !valid) return std::unexpected(std::move(valid.error()));
  const std::string_view effective_authority =
      options.authority_override.empty() ? authority : std::string_view{options.authority_override};
  if (effective_authority.empty() || !h2::IsAsciiVisibleToken(effective_authority)) {
    return std::unexpected(
        MakeChanError(StatusCode::kInvalidArgument, "channel requires printable ASCII authority"));
  }
  auto impl =
      std::make_unique<SynchronousChannel::Impl>(std::move(*wire), effective_authority, options);
  if (auto ready = impl->Initialize(); !ready) return std::unexpected(std::move(ready.error()));
  return std::make_unique<SynchronousChannel>(std::move(impl));
}

}  // namespace

SynchronousChannel::SynchronousChannel(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SynchronousChannel::~SynchronousChannel() noexcept {
  if (impl_) {
    for (const auto idx : impl_->active_streams_mask | uarch::views::ones) {
      if (auto* handle = impl_->handles[idx]) {
        impl_->handles[idx] = nullptr;
        handle->DetachChannel();
      }
    }
  }
  Close();
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::Connect(
    std::string_view endpoint_uri, const SynchronousChannelOptions& options) noexcept {
  return Connect(endpoint_uri, SyncTlsClientOptions{}, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::Connect(
    std::string_view endpoint_uri, const SyncTlsClientOptions& tls,
    const SynchronousChannelOptions& options) noexcept {
  auto parsed = SyncEndpoint::Parse(endpoint_uri);
  if (!parsed) return std::unexpected(std::move(parsed.error()));
  return Connect(*parsed, options, tls);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::Connect(
    const SyncEndpoint& endpoint, const SynchronousChannelOptions& options,
    const SyncTlsClientOptions& tls) noexcept {
  switch (endpoint.Scheme()) {
    case EndpointScheme::kGrpc:
      return ConnectTcp(endpoint.Host(), endpoint.Port(), options);
    case EndpointScheme::kGrpcs:
      return ConnectTls(endpoint.Host(), endpoint.Port(), tls, options);
    case EndpointScheme::kUnix:
      return ConnectUnix(endpoint.UnixPath(), options);
  }
  return std::unexpected(
      MakeChanError(StatusCode::kInvalidArgument, "unsupported endpoint scheme"));
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::ConnectTcp(
    std::string_view host, std::uint16_t port, const SynchronousChannelOptions& options) noexcept {
  const std::string authority = host.contains(':')
                                    ? ("[" + std::string{host} + "]:" + std::to_string(port))
                                    : (std::string{host} + ":" + std::to_string(port));
  return BuildChannelFromWire(sync_detail::SyncWireTransport::ConnectTcp(host, port, options),
                              authority, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::ConnectTls(
    std::string_view host, std::uint16_t port, const nexora::TlsClientConfig& tls,
    std::string_view server_name, const SynchronousChannelOptions& options) noexcept {
  const std::string authority =
      !server_name.empty()
          ? std::string{server_name}
          : (host.contains(':') ? ("[" + std::string{host} + "]:" + std::to_string(port))
                                : (std::string{host} + ":" + std::to_string(port)));
  return BuildChannelFromWire(
      sync_detail::SyncWireTransport::ConnectTls(host, port, tls, server_name, options), authority,
      options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::ConnectTls(
    std::string_view host, std::uint16_t port, const SyncTlsClientOptions& tls,
    const SynchronousChannelOptions& options) noexcept {
  const std::string_view sni =
      tls.server_name_override.has_value() ? std::string_view{*tls.server_name_override} : host;
  if (tls.config != nullptr) return ConnectTls(host, port, *tls.config, sni, options);
  auto built = tls.BuildConfig();
  if (!built) return std::unexpected(std::move(built.error()));
  return ConnectTls(host, port, *built, sni, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::ConnectUnix(
    std::string_view path, const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::ConnectUnix(path, options),
                              "localhost", options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::AdoptTcp(
    int fd, std::string_view authority, const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::AdoptTcp(fd, options), authority,
                              options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::AdoptUnix(
    int fd, std::string_view authority, const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::AdoptUnix(fd, options), authority,
                              options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::FromTcp(
    nexora::TcpTransport tcp, std::string_view authority,
    const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::FromTcp(std::move(tcp), options),
                              authority, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::FromUnix(
    nexora::UnixSocket sock, std::string_view authority,
    const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::FromUnix(std::move(sock), options),
                              authority, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::FromMemoryPipe(
    nexora::MemoryPipe pipe, std::string_view authority,
    const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(sync_detail::SyncWireTransport::FromMemoryPipe(std::move(pipe)),
                              authority, options);
}

std::expected<std::unique_ptr<SynchronousChannel>, Status> SynchronousChannel::FromMemoryPipeTls(
    nexora::MemoryPipe pipe, const nexora::TlsClientConfig& tls, std::string_view server_name,
    const SynchronousChannelOptions& options) noexcept {
  return BuildChannelFromWire(
      sync_detail::SyncWireTransport::FromMemoryPipeTlsClient(std::move(pipe), tls, server_name),
      server_name.empty() ? "localhost" : server_name, options);
}

void SynchronousChannel::Close() noexcept {
  if (!impl_ || impl_->IsClosed()) return;
  impl_->FailAll(MakeChanError(StatusCode::kCancelled, "channel closed"));
  if (impl_->h2) {
    if (auto goaway = impl_->h2->SubmitGoaway(0U, nexora::Http2ErrorCode::kNoError); !goaway) {
      if (impl_->failure.Ok()) {
        impl_->failure = MakeChanError(StatusCode::kUnavailable, goaway.error().Message());
      }
    } else if (auto flushed = impl_->FlushWire(); !flushed && impl_->failure.Ok()) {
      auto err = std::move(flushed.error());
      if (err.Code() == StatusCode::kDeadlineExceeded) {
        err = MakeChanError(StatusCode::kUnavailable, err.Message());
      }
      impl_->failure = std::move(err);
    }
  }
  impl_->transport.Close();
  impl_->runtime_meta = impl_->runtime_meta.with<^^SyncChannelRuntimeSpec::closed>(true);
}

bool SynchronousChannel::IsOpen() const noexcept {
  return impl_ != nullptr && !impl_->IsClosed() && impl_->failure.Ok() && impl_->transport.IsOpen();
}

std::expected<void, Status> SynchronousChannel::PollOnce() noexcept {
  auto owned = impl_->CheckOwner();
  if (!owned) return owned;
  auto stepped = impl_->PollStep();
  if (!stepped) return std::unexpected(std::move(stepped.error()));
  return {};
}

void SynchronousChannel::SetProgressHook(ProgressDelegate hook) noexcept {
  if (!impl_->CheckOwner() || impl_->IsPolling()) return;
  impl_->progress_hook = std::move(hook);
}

void SynchronousChannel::SetProgressHook(void* context, ProgressHook hook) noexcept {
  if (!impl_->CheckOwner() || impl_->IsPolling()) return;
  if (hook == nullptr) {
    impl_->progress_hook = ProgressDelegate{};
    return;
  }
  impl_->progress_hook = ProgressDelegate{[context, hook]() noexcept { return hook(context); }};
}

std::size_t SynchronousChannel::ActiveStreams() const noexcept {
  return impl_ ? impl_->active_streams_mask.count() : 0uz;
}

std::size_t SynchronousChannel::WindowUpdatesReceived() const noexcept {
  return impl_ ? impl_->window_updates_received : 0uz;
}

int SynchronousChannel::NativeHandle() const noexcept {
  return (impl_ && impl_->CheckOwner().has_value()) ? impl_->transport.NativeHandle() : -1;
}

std::expected<std::unique_ptr<RpcStream>, Status> SynchronousChannel::Open(
    std::string_view path, RpcKind kind, CallOptions options) noexcept {
  auto owned = impl_->CheckOwner();
  if (uarch::unlikely(!owned.has_value())) return std::unexpected(std::move(owned.error()));
  if (uarch::unlikely(impl_->IsPolling())) {
    return std::unexpected(
        MakeChanError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (uarch::unlikely(impl_->IsClosed())) {
    return std::unexpected(MakeChanError(StatusCode::kFailedPrecondition, "channel is closed"));
  }
  if (uarch::unlikely(!impl_->failure.Ok())) return std::unexpected(impl_->failure);
  if (uarch::unlikely(impl_->IsDraining())) {
    return std::unexpected(
        MakeChanError(StatusCode::kUnavailable, "HTTP/2 channel is draining after GOAWAY"));
  }
  if (uarch::unlikely(options.timeout.count() <= 0)) {
    return std::unexpected(
        MakeChanError(StatusCode::kInvalidArgument, "RPC timeout must be positive"));
  }
  const bool valid_kind = [kind]() noexcept {
    switch (kind) {
      case RpcKind::kUnary:
      case RpcKind::kClientStreaming:
      case RpcKind::kServerStreaming:
      case RpcKind::kBidiStreaming:
        return true;
    }
    return false;
  }();
  if (uarch::unlikely(!valid_kind)) {
    return std::unexpected(MakeChanError(StatusCode::kInvalidArgument, "unsupported RPC kind"));
  }

  const auto timeout_ns = static_cast<std::uint64_t>(options.timeout.count());
  auto built_headers = h2::BuildGrpcRequestHeaders(h2::GrpcRequestHeaderSpec{
      .scheme = impl_->IsSecure() ? "https" : "http",
      .authority = impl_->authority,
      .path = path,
      .content_type = options.content_type,
      .encoding = options.compression,
      .accept_encoding = CompressionRegistry::AcceptEncoding(),
      .timeout_ns = timeout_ns,
      .metadata = &options.metadata,
      .max_metadata_bytes = impl_->options.max_metadata_bytes,
  });
  if (uarch::unlikely(!built_headers.has_value())) {
    return std::unexpected(std::move(built_headers.error()));
  }

  const auto free_slot = impl_->active_streams_mask.lowest_zero();
  if (uarch::unlikely(!free_slot.has_value() || impl_->active_streams_mask.count() >=
                                                    impl_->options.max_concurrent_streams)) {
    return std::unexpected(
        MakeChanError(StatusCode::kResourceExhausted, "concurrent stream limit reached"));
  }

  if (auto primed = impl_->FlushWire(); uarch::unlikely(!primed.has_value())) {
    impl_->FailAll(primed.error());
    return std::unexpected(impl_->failure);
  }
  auto opened_id = impl_->h2->OpenStream();
  if (uarch::unlikely(!opened_id.has_value())) {
    return std::unexpected(
        MakeChanError(StatusCode::kResourceExhausted, "cannot open HTTP/2 client stream"));
  }
  auto submitted = impl_->h2->SubmitRequestHeaders(*opened_id, nexora::Method::kPost,
                                                   impl_->IsSecure() ? "https" : "http",
                                                   impl_->authority, path, *built_headers, false);
  if (uarch::unlikely(!submitted.has_value())) {
    if (auto rst = impl_->h2->SubmitRstStream(*opened_id, nexora::Http2ErrorCode::kCancel);
        !rst &&
        rst.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
      impl_->FailAll(MakeChanError(StatusCode::kUnavailable, rst.error().Message()));
    }
    return std::unexpected(
        MakeChanError(StatusCode::kInternal, "cannot submit HTTP/2 request headers"));
  }

  auto state = std::make_unique<RpcStream::State>(impl_->options.max_message_bytes);
  state->slot_index = static_cast<std::uint32_t>(*free_slot);
  state->id = *opened_id;
  state->kind = kind;
  state->path.assign(path);
  state->options = std::move(options);
  const auto now = std::chrono::steady_clock::now();
  const auto max_rem = std::chrono::steady_clock::time_point::max() - now;
  state->deadline = state->options.timeout >= max_rem ? std::chrono::steady_clock::time_point::max()
                                                      : now + state->options.timeout;

  impl_->streams[*free_slot] = state.get();
  impl_->active_streams_mask = impl_->active_streams_mask.set(*free_slot);
  auto stream = std::make_unique<RpcStream>(*this, std::move(state));
  if (auto flushed = impl_->FlushWire(); uarch::unlikely(!flushed.has_value())) {
    impl_->FailAll(flushed.error());
    return std::unexpected(impl_->failure);
  }
  return stream;
}

std::expected<RpcResponse, Status> SynchronousChannel::Unary(std::string_view path,
                                                             std::span<const std::byte> payload,
                                                             CallOptions options) noexcept {
  auto stream = Open(path, RpcKind::kUnary, std::move(options));
  if (uarch::unlikely(!stream.has_value())) return std::unexpected(std::move(stream.error()));
  if (auto sent = (*stream)->Send(payload); uarch::unlikely(!sent.has_value())) {
    return std::unexpected(std::move(sent.error()));
  }
  return (*stream)->Finish();
}

std::expected<std::unique_ptr<RpcStream>, Status> SynchronousChannel::ClientStreamCall(
    std::string_view path, CallOptions options) noexcept {
  return Open(path, RpcKind::kClientStreaming, std::move(options));
}

std::expected<std::unique_ptr<RpcStream>, Status> SynchronousChannel::ServerStreamCall(
    std::string_view path, std::span<const std::byte> payload, CallOptions options) noexcept {
  auto stream = Open(path, RpcKind::kServerStreaming, std::move(options));
  if (uarch::unlikely(!stream.has_value())) return std::unexpected(std::move(stream.error()));
  if (auto sent = (*stream)->Send(payload); uarch::unlikely(!sent.has_value())) {
    return std::unexpected(std::move(sent.error()));
  }
  if (auto closed = (*stream)->HalfClose(); uarch::unlikely(!closed.has_value())) {
    return std::unexpected(std::move(closed.error()));
  }
  return stream;
}

std::expected<std::unique_ptr<RpcStream>, Status> SynchronousChannel::BidiStreamCall(
    std::string_view path, CallOptions options) noexcept {
  return Open(path, RpcKind::kBidiStreaming, std::move(options));
}

}  // namespace grpcxx
