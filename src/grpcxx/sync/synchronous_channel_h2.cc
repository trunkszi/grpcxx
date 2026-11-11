#include <array>
#include <chrono>
#include <string>
#include <thread>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/h2/grpc_http2_headers.h"
#include "grpcxx/sync/synchronous_channel_internal.h"

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeH2ChanError(StatusCode code,
                                                               std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MapClientH2Error(const nexora::Error& err) noexcept {
  uarch::cold_path();
  std::string msg = "HTTP/2 client protocol error: ";
  msg.append(err.Message());
  return Status{StatusCode::kInternal, std::move(msg)};
}

}  // namespace

SynchronousChannel::Impl::Impl(sync_detail::SyncWireTransport wire, std::string_view authority_text,
                               SynchronousChannelOptions opts) noexcept
    : transport(std::move(wire)),
      authority(authority_text),
      options(std::move(opts)),
      runtime_meta(uarch::BitWord<SyncChannelRuntimeSpec>{}.with<^^SyncChannelRuntimeSpec::secure>(
          transport.IsSecure())) {}

SynchronousChannel::Impl::~Impl() noexcept = default;

std::expected<void, Status> SynchronousChannel::Impl::Initialize() noexcept {
  const nexora::Http2Settings local_settings{
      .header_table_size = 4096U,
      .enable_push = 0U,
      .max_concurrent_streams = options.max_concurrent_streams,
      .initial_window_size = options.flow_control_window_bytes,
      .max_frame_size = 65536U,
      .max_header_list_size = 131072U,
  };
  const nexora::Http2Limits limits{
      .io_buffer_bytes = kIoBufferBytes,
      .header_block_bytes = 65536uz,
      .max_coalesced_body_bytes = 0uz,
      .coalesce_message_body = false,
  };
  h2 =
      std::make_unique<nexora::Http2Connection>(nexora::Http2Role::kClient, local_settings, limits);
  if (uarch::unlikely(!h2)) {
    return std::unexpected(
        MakeH2ChanError(StatusCode::kResourceExhausted, "cannot allocate HTTP/2 client session"));
  }
  if (auto init = h2->InitiateHandshake(); uarch::unlikely(!init.has_value())) {
    return std::unexpected(MapClientH2Error(init.error()));
  }
  if (options.flow_control_window_bytes > nexora::kHttp2DefaultWindowSize) {
    const auto delta = options.flow_control_window_bytes - nexora::kHttp2DefaultWindowSize;
    if (auto wu = h2->SubmitWindowUpdate(0U, delta); uarch::unlikely(!wu.has_value())) {
      return std::unexpected(MapClientH2Error(wu.error()));
    }
  }
  return {};
}

std::expected<void, Status> SynchronousChannel::Impl::CheckOwner() const noexcept {
  if (uarch::unlikely(std::this_thread::get_id() != owner)) {
    return std::unexpected(MakeH2ChanError(StatusCode::kFailedPrecondition,
                                           "channel belongs to another caller thread"));
  }
  return {};
}

RpcStream::State* SynchronousChannel::Impl::Find(std::uint32_t stream_id) noexcept {
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    auto* stream = streams[idx];
    if (stream != nullptr) {
      uarch::mem::prefetch_read<uarch::mem::L1>(*stream);
      if (stream->id == stream_id) return stream;
    }
  }
  return nullptr;
}

std::expected<bool, Status> SynchronousChannel::Impl::PumpOutgoing() noexcept {
  bool progressed = false;
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    auto* stream = streams[idx];
    if (stream == nullptr || stream->IsTerminal() || stream->EndStreamSubmitted()) continue;

    while (!stream->outgoing.empty()) {
      const auto wire_bytes = stream->outgoing.front().WireBytes().subspan(stream->outgoing_offset);
      const bool finish = (stream->outgoing.size() == 1uz) && stream->RequestEnded();
      const auto chunk = sync_detail::AsUint8Span(wire_bytes);
      auto sent = h2->SubmitData(stream->id, chunk, finish);
      if (uarch::unlikely(!sent.has_value())) {
        AbortStream(*stream, MapClientH2Error(sent.error()), false);
        break;
      }
      if (*sent == 0uz) break;
      progressed = true;
      stream->outgoing_offset += *sent;
      stream->outgoing_bytes -= *sent;
      if (*sent == wire_bytes.size()) {
        stream->outgoing.pop_front();
        stream->outgoing_offset = 0uz;
        if (finish) {
          stream->meta = stream->meta.with<^^SyncStreamMetaSpec::end_stream_submitted>(true);
        }
      }
    }

    if (stream->outgoing.empty() && stream->RequestEnded() && !stream->EndStreamSubmitted() &&
        !stream->IsTerminal()) {
      if (OutputFreeBytes() >= nexora::kHttp2FrameHeaderSize) {
        auto eof_res = h2->SubmitData(stream->id, {}, true);
        if (uarch::unlikely(!eof_res.has_value())) {
          AbortStream(*stream, MapClientH2Error(eof_res.error()), false);
        } else {
          stream->meta = stream->meta.with<^^SyncStreamMetaSpec::end_stream_submitted>(true);
          progressed = true;
        }
      }
    }
  }
  return progressed;
}

std::expected<bool, Status> SynchronousChannel::Impl::FlushWire() noexcept {
  bool moved = false;
  if (transport.IsSecure()) {
    if (auto stepped = transport.StepTls(); !stepped) {
      return std::unexpected(std::move(stepped.error()));
    }
  }
  for (std::size_t frame = 0uz; frame < 64uz; ++frame) {
    const auto pending = h2->PendingOutput();
    if (pending.empty()) break;
    auto wrote = transport.WriteSome(std::as_bytes(pending));
    if (uarch::unlikely(!wrote.has_value())) {
      return std::unexpected(std::move(wrote.error()));
    }
    if (wrote->bytes > 0uz) {
      h2->ConsumeOutput(wrote->bytes);
      moved = true;
    }
    if (wrote->would_block != 0U || wrote->bytes == 0uz) break;
  }
  return moved;
}

void SynchronousChannel::Impl::HandleHeadersEvent(const nexora::Http2EventView& ev) noexcept {
  auto* stream = Find(ev.stream_id);
  if (stream == nullptr || stream->IsTerminal()) return;
  if (uarch::unlikely(stream->ReceivedHeaders())) {
    AbortStream(*stream, MakeH2ChanError(StatusCode::kInternal, "unexpected second HEADERS block"),
                true);
    return;
  }
  const auto fields =
      ev.headers != nullptr ? ev.headers->Fields() : std::span<const nexora::HeaderField>{};
  auto parsed = h2::ParseGrpcResponseHeaders(static_cast<int>(ev.status.AsU16()), fields, false,
                                             ev.end_stream, options.max_metadata_bytes);
  if (uarch::unlikely(!parsed.has_value())) {
    AbortStream(*stream, std::move(parsed.error()), true);
    return;
  }
  stream->meta = stream->meta.with<^^SyncStreamMetaSpec::received_headers>(true);
  stream->initial = std::move(parsed->metadata);
  stream->incoming_encoding = parsed->encoding;
  if (parsed->has_grpc_status) {
    stream->trailers = stream->initial;
    stream->status = std::move(parsed->grpc_status);
    stream->meta = stream->meta.with<^^SyncStreamMetaSpec::received_status>(true);
  }
  if (ev.end_stream) FinalizeRemoteEndStream(*stream);
}

void SynchronousChannel::Impl::HandleTrailersEvent(const nexora::Http2EventView& ev) noexcept {
  auto* stream = Find(ev.stream_id);
  if (stream == nullptr || stream->IsTerminal()) return;
  if (uarch::unlikely(!stream->ReceivedHeaders())) {
    AbortStream(*stream, MakeH2ChanError(StatusCode::kInternal, "trailers before initial headers"),
                true);
    return;
  }
  const auto fields =
      ev.headers != nullptr ? ev.headers->Fields() : std::span<const nexora::HeaderField>{};
  auto parsed =
      h2::ParseGrpcResponseHeaders(200, fields, true, ev.end_stream, options.max_metadata_bytes);
  if (uarch::unlikely(!parsed.has_value())) {
    AbortStream(*stream, std::move(parsed.error()), true);
    return;
  }
  if (uarch::unlikely(!parsed->has_grpc_status)) {
    AbortStream(*stream,
                MakeH2ChanError(StatusCode::kInternal, "response ended without grpc-status"), true);
    return;
  }
  stream->trailers = std::move(parsed->metadata);
  stream->status = std::move(parsed->grpc_status);
  stream->meta = stream->meta.with<^^SyncStreamMetaSpec::received_status>(true);
  FinalizeRemoteEndStream(*stream);
}

void SynchronousChannel::Impl::HandleDataEvent(const nexora::Http2EventView& ev) noexcept {
  const auto data_len = ev.data.size();
  const auto fc_bytes =
      ev.flow_control_bytes != 0U ? ev.flow_control_bytes : static_cast<std::uint32_t>(data_len);
  if (fc_bytes > 0U) {
    conn_unreplenished_recv_bytes += fc_bytes;
    if (conn_unreplenished_recv_bytes >= kWindowReplenishThreshold) {
      if (auto updated = h2->SubmitWindowUpdate(0U, conn_unreplenished_recv_bytes); !updated) {
        FailAll(MapClientH2Error(updated.error()));
        return;
      }
      conn_unreplenished_recv_bytes = 0U;
    }
  }

  auto* stream = Find(ev.stream_id);
  if (stream == nullptr || stream->IsTerminal()) return;
  if (uarch::unlikely(!stream->ReceivedHeaders())) {
    AbortStream(*stream, MakeH2ChanError(StatusCode::kInternal, "DATA before response headers"),
                true);
    return;
  }

  if (data_len > 0uz) {
    auto frames = stream->parser.Feed(std::as_bytes(ev.data));
    if (uarch::unlikely(!frames.has_value())) {
      AbortStream(*stream, std::move(frames.error()), true);
      return;
    }
    for (auto& msg : *frames) {
      if (uarch::unlikely(msg.Compressed() &&
                          stream->incoming_encoding == CompressionAlgorithm::kIdentity)) {
        AbortStream(*stream,
                    MakeH2ChanError(StatusCode::kInternal,
                                    "compressed message without negotiated encoding"),
                    true);
        return;
      }
      auto decoded = compression.Decompress(
          msg.Compressed() ? stream->incoming_encoding : CompressionAlgorithm::kIdentity,
          msg.Payload(), options.max_message_bytes);
      if (uarch::unlikely(!decoded.has_value())) {
        AbortStream(*stream, std::move(decoded.error()), true);
        return;
      }
      if (uarch::unlikely(
              (stream->kind == RpcKind::kUnary || stream->kind == RpcKind::kClientStreaming) &&
              stream->received_messages >= 1uz)) {
        AbortStream(
            *stream,
            MakeH2ChanError(StatusCode::kInternal, "RPC allows at most one response message"),
            true);
        return;
      }
      if (uarch::unlikely(stream->incoming_bytes + decoded->PayloadSize() >
                          options.max_queued_bytes)) {
        AbortStream(*stream,
                    MakeH2ChanError(StatusCode::kResourceExhausted, "response queue exceeds limit"),
                    true);
        return;
      }
      stream->incoming_bytes += decoded->PayloadSize();
      stream->incoming.emplace_back(decoded->Payload().begin(), decoded->Payload().end());
      ++stream->received_messages;
    }
  }

  if (fc_bytes > 0U && !ev.end_stream && !stream->IsTerminal()) {
    stream->unreplenished_recv_bytes += fc_bytes;
    if (stream->unreplenished_recv_bytes >= kWindowReplenishThreshold) {
      if (auto updated = h2->SubmitWindowUpdate(stream->id, stream->unreplenished_recv_bytes);
          !updated) {
        // A later frame in this read can close the stream before its WINDOW_UPDATE is submitted.
        if (updated.error() !=
            nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
          AbortStream(*stream, MapClientH2Error(updated.error()), false);
          if (failure.Ok()) FailAll(MapClientH2Error(updated.error()));
          return;
        }
      }
      stream->unreplenished_recv_bytes = 0U;
    }
  }

  if (ev.end_stream) FinalizeRemoteEndStream(*stream);
}

void SynchronousChannel::Impl::FinalizeRemoteEndStream(RpcStream::State& stream) noexcept {
  stream.meta = stream.meta.with<^^SyncStreamMetaSpec::peer_closed>(true);
  if (stream.IsTerminal()) return;
  if (uarch::unlikely(!stream.ReceivedStatus())) {
    AbortStream(stream,
                MakeH2ChanError(StatusCode::kInternal, "response ended without grpc-status"),
                false);
    return;
  }
  if (uarch::unlikely(stream.parser.State() != LengthPrefixedFrame::ParseState::kReadingHeader ||
                      stream.parser.BufferedPayloadBytes() != 0uz)) {
    AbortStream(stream, MakeH2ChanError(StatusCode::kDataLoss, "truncated gRPC message"), false);
    return;
  }
  stream.meta = kSyncStreamTerminalClosedPatch.apply(stream.meta);
}

void SynchronousChannel::Impl::AbortStream(RpcStream::State& stream, Status reason,
                                           bool send_rst) noexcept {
  if (stream.IsTerminal()) return;
  stream.status = std::move(reason);
  stream.meta = kSyncStreamLocalAbortPatch.apply(stream.meta);
  if (send_rst && !stream.IsPeerClosed() && stream.id != 0U && !stream.RstSent() && h2) {
    if (auto rst = h2->SubmitRstStream(stream.id, nexora::Http2ErrorCode::kCancel);
        !rst &&
        rst.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
      if (failure.Ok()) {
        failure = MakeH2ChanError(StatusCode::kUnavailable, rst.error().Message());
      }
    } else {
      stream.meta = stream.meta.with<^^SyncStreamMetaSpec::rst_sent>(true);
    }
  }
}

void SynchronousChannel::Impl::FailAll(const Status& reason) noexcept {
  Status poison = reason;
  if (poison.Code() == StatusCode::kDeadlineExceeded) {
    poison = MakeH2ChanError(StatusCode::kUnavailable, reason.Message());
  }
  if (failure.Ok()) failure = poison;
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    auto* stream = streams[idx];
    if (stream != nullptr && !stream->IsTerminal()) {
      AbortStream(*stream, poison, false);
    }
  }
}

std::expected<bool, Status> SynchronousChannel::Impl::DrainEvents() noexcept {
  bool drained_any = false;
  while (const auto ev = h2->PeekEventView()) {
    drained_any = true;
    switch (ev->kind) {
      case nexora::Http2Event::Kind::kNone:
        break;
      case nexora::Http2Event::Kind::kHeaders:
        HandleHeadersEvent(*ev);
        break;
      case nexora::Http2Event::Kind::kTrailers:
        HandleTrailersEvent(*ev);
        break;
      case nexora::Http2Event::Kind::kData:
        HandleDataEvent(*ev);
        break;
      case nexora::Http2Event::Kind::kMessageComplete:
        if (auto* stream = Find(ev->stream_id); stream != nullptr) {
          FinalizeRemoteEndStream(*stream);
        }
        break;
      case nexora::Http2Event::Kind::kStreamReset:
        if (auto* stream = Find(ev->stream_id); stream != nullptr) {
          stream->meta = stream->meta.with<^^SyncStreamMetaSpec::peer_closed>(true);
          if (!stream->IsTerminal()) {
            stream->status = MakeH2ChanError(ev->error_code == nexora::Http2ErrorCode::kCancel
                                                 ? StatusCode::kCancelled
                                                 : StatusCode::kUnavailable,
                                             "HTTP/2 stream reset before trailers");
            stream->meta = kSyncStreamPeerResetPatch.apply(stream->meta);
          }
        }
        break;
      case nexora::Http2Event::Kind::kGoaway:
        runtime_meta = runtime_meta.with<^^SyncChannelRuntimeSpec::draining>(true);
        for (const auto idx : active_streams_mask | uarch::views::ones) {
          auto* stream = streams[idx];
          if (stream != nullptr && !stream->IsTerminal() &&
              (stream->id > ev->last_stream_id ||
               ev->error_code != nexora::Http2ErrorCode::kNoError)) {
            AbortStream(*stream,
                        MakeH2ChanError(StatusCode::kUnavailable, "peer sent HTTP/2 GOAWAY"),
                        false);
          }
        }
        break;
      case nexora::Http2Event::Kind::kWindowUpdate:
        ++window_updates_received;
        break;
      case nexora::Http2Event::Kind::kSettingsAck:
      case nexora::Http2Event::Kind::kPingAck:
        break;
    }
    h2->ConsumeEvent();
  }
  return drained_any;
}

std::expected<bool, Status> SynchronousChannel::Impl::PollStep() noexcept {
  if (uarch::unlikely(IsPolling())) {
    return std::unexpected(
        MakeH2ChanError(StatusCode::kFailedPrecondition, "recursive channel polling"));
  }
  if (uarch::unlikely(!failure.Ok())) return std::unexpected(failure);
  if (uarch::unlikely(IsClosed())) {
    return std::unexpected(MakeH2ChanError(StatusCode::kFailedPrecondition, "channel is closed"));
  }

  runtime_meta = runtime_meta.with<^^SyncChannelRuntimeSpec::polling>(true);
  const auto result = [this]() noexcept -> std::expected<bool, Status> {
    bool progressed = false;
    const auto now = std::chrono::steady_clock::now();
    for (const auto idx : active_streams_mask | uarch::views::ones) {
      auto* stream = streams[idx];
      if (stream != nullptr && !stream->IsTerminal() && now >= stream->deadline) {
        AbortStream(*stream,
                    MakeH2ChanError(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"), true);
        progressed = true;
      }
    }

    auto pumped_pre = PumpOutgoing();
    if (!pumped_pre) return std::unexpected(std::move(pumped_pre.error()));
    progressed = progressed || *pumped_pre;

    auto flushed_pre = FlushWire();
    if (!flushed_pre) return std::unexpected(std::move(flushed_pre.error()));
    progressed = progressed || *flushed_pre;

    if (progress_hook) {
      if (auto hooked = progress_hook(); !hooked) {
        return std::unexpected(std::move(hooked.error()));
      }
    }

    bool saw_eof = false;
    for (std::size_t iter = 0uz; iter < 64uz; ++iter) {
      auto read_res = transport.ReadSome(rx_scratch);
      if (!read_res) return std::unexpected(std::move(read_res.error()));
      if (read_res->eof != 0U) {
        saw_eof = true;
        break;
      }
      if (read_res->bytes == 0uz || read_res->would_block != 0U) break;
      progressed = true;
      const auto bytes_u8 =
          sync_detail::AsUint8Span(std::span<std::byte>{rx_scratch.data(), read_res->bytes});
      std::size_t offset = 0uz;
      while (offset < bytes_u8.size() || h2->IsIngressPaused()) {
        const auto fed = h2->FeedBytes(bytes_u8.subspan(offset));
        if (uarch::unlikely(!fed.Ok())) {
          return std::unexpected(MapClientH2Error(fed.ErrorValue()));
        }
        offset += fed.Consumed();
        auto drained = DrainEvents();
        if (!drained) return std::unexpected(std::move(drained.error()));
        if (uarch::unlikely(!failure.Ok())) return std::unexpected(failure);
        progressed = progressed || *drained;
        if (h2->IsIngressPaused()) {
          auto flushed_mid = FlushWire();
          if (!flushed_mid) return std::unexpected(std::move(flushed_mid.error()));
          progressed = progressed || *flushed_mid;
          if (fed.Consumed() == 0uz && !*drained && !*flushed_mid) {
            if (offset < bytes_u8.size()) {
              return std::unexpected(MapClientH2Error(nexora::Error::BufferOverflow()));
            }
            break;
          }
        }
      }
    }

    auto drained_tail = DrainEvents();
    if (!drained_tail) return std::unexpected(std::move(drained_tail.error()));
    if (uarch::unlikely(!failure.Ok())) return std::unexpected(failure);
    progressed = progressed || *drained_tail;

    auto pumped_post = PumpOutgoing();
    if (!pumped_post) return std::unexpected(std::move(pumped_post.error()));
    progressed = progressed || *pumped_post;

    auto flushed_post = FlushWire();
    if (!flushed_post) return std::unexpected(std::move(flushed_post.error()));
    progressed = progressed || *flushed_post;

    if (saw_eof) {
      auto closed_status =
          MakeH2ChanError(StatusCode::kUnavailable, "peer closed HTTP/2 connection");
      if (failure.Ok()) failure = closed_status;
      const bool any_unfinished = [this]() noexcept {
        for (const auto idx : active_streams_mask | uarch::views::ones) {
          if (streams[idx] != nullptr && !streams[idx]->IsTerminal()) return true;
        }
        return false;
      }();
      if (any_unfinished) return std::unexpected(std::move(closed_status));
    }
    return progressed;
  }();

  runtime_meta = runtime_meta.with<^^SyncChannelRuntimeSpec::polling>(false);
  if (uarch::unlikely(!result.has_value())) {
    FailAll(result.error());
    return std::unexpected(failure);
  }
  return *result;
}

bool SynchronousChannel::Impl::InboundFrameOpen() const noexcept {
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    const auto* stream = streams[idx];
    if (stream != nullptr &&
        stream->parser.State() == LengthPrefixedFrame::ParseState::kReadingPayload) {
      return true;
    }
  }
  return false;
}

bool SynchronousChannel::Impl::OutboundCanProgress(bool caller_wants_write) const noexcept {
  if (h2 == nullptr) return false;
  if (!h2->PendingOutput().empty() || transport.HasPendingTlsWrite()) return true;
  if (!caller_wants_write || h2->ConnectionSendWindow() <= 0) return false;
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    const auto* stream = streams[idx];
    if (stream == nullptr || stream->IsTerminal()) continue;
    const bool payload = !stream->outgoing.empty();
    const bool end_needed = stream->RequestEnded() && !stream->EndStreamSubmitted();
    if (!payload && !end_needed) continue;
    if (payload && h2->StreamSendWindow(stream->id) <= 0) continue;
    return true;
  }
  return false;
}

std::expected<void, Status> SynchronousChannel::Impl::PollUntilProgress(
    bool want_write, std::chrono::steady_clock::time_point deadline) noexcept {
  if (std::chrono::steady_clock::now() >= deadline) {
    return std::unexpected(MakeH2ChanError(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"));
  }
  auto stepped = PollStep();
  if (!stepped) return std::unexpected(std::move(stepped.error()));
  if (*stepped) return {};

  const bool h2_wants_write = OutboundCanProgress(want_write);
  if (transport.NativeHandle() >= 0 && !progress_hook) {
    const bool honor_read = options.read_timeout.count() > 0 || InboundFrameOpen();
    auto waited = transport.WaitSocket(h2_wants_write, deadline, honor_read);
    if (!waited) {
      const bool rpc_expired = std::chrono::steady_clock::now() >= deadline;
      if (h2_wants_write && !rpc_expired) {
        FailAll(MakeH2ChanError(StatusCode::kUnavailable, waited.error().Message()));
        return std::unexpected(failure);
      }
      return std::unexpected(std::move(waited.error()));
    }
    return {};
  }
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) {
    return std::unexpected(MakeH2ChanError(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"));
  }
  if (deadline - now > std::chrono::milliseconds{1}) {
    std::this_thread::sleep_for(std::chrono::microseconds{100});
  } else {
    std::this_thread::yield();
  }
  return {};
}

}  // namespace grpcxx
