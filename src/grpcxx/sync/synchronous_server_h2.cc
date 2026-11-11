#include <array>
#include <utility>

#include "grpcxx/h2/grpc_http2_headers.h"
#include "grpcxx/sync/synchronous_server_internal.h"

namespace grpcxx {

std::expected<void, Status> SynchronousServer::Impl::Initialize() noexcept {
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
      std::make_unique<nexora::Http2Connection>(nexora::Http2Role::kServer, local_settings, limits);
  if (uarch::unlikely(!h2)) {
    return std::unexpected(
        MakeSrvError(StatusCode::kResourceExhausted, "cannot allocate HTTP/2 server session"));
  }
  if (auto init = h2->InitiateHandshake(); uarch::unlikely(!init.has_value())) {
    return std::unexpected(MapSrvH2Error(init.error()));
  }
  if (options.flow_control_window_bytes > nexora::kHttp2DefaultWindowSize) {
    const auto delta = options.flow_control_window_bytes - nexora::kHttp2DefaultWindowSize;
    if (auto wu = h2->SubmitWindowUpdate(0U, delta); uarch::unlikely(!wu.has_value())) {
      return std::unexpected(MapSrvH2Error(wu.error()));
    }
  }
  return {};
}

SynchronousServer::Impl::StreamSlot* SynchronousServer::Impl::Find(std::int32_t id) noexcept {
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    uarch::mem::prefetch_read<uarch::mem::L1>(streams[idx]);
    if (streams[idx].id == id) return &streams[idx];
  }
  return nullptr;
}

void SynchronousServer::Impl::ReleaseSlot(StreamSlot& slot) noexcept {
  const auto idx = slot.slot_index;
  slot = StreamSlot{};
  active_streams_mask = active_streams_mask.clear(idx);
}

void SynchronousServer::Impl::MaybeReclaimCompleted(StreamSlot& slot) noexcept {
  if (!slot.IsInDispatch() && slot.IsActive() &&
      (slot.IsRejected() || (slot.IsHalfClosed() && slot.IsTrailersSubmitted()))) {
    ReleaseSlot(slot);
  }
}

void SynchronousServer::Impl::RejectStream(StreamSlot& slot, nexora::Http2ErrorCode code) noexcept {
  slot.meta = slot.meta.with<^^ServerStreamMetaSpec::rejected>(true);
  if (auto rst = h2->SubmitRstStream(static_cast<std::uint32_t>(slot.id), code);
      !rst && rst.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed) &&
      fatal_error.Ok()) {
    fatal_error = MapSrvH2Error(rst.error());
  }
  // ServerRequest aliases slot.path and slot.request_metadata for the duration of Dispatch.
  if (!slot.IsInDispatch()) ReleaseSlot(slot);
}

bool SynchronousServer::Impl::IsPeerTransportClosed(const Status& status) noexcept {
  if (status.Code() != StatusCode::kUnavailable) return false;
  const auto msg = status.Message();
  return msg.contains("connection closed") || msg.contains("connection reset");
}

std::expected<void, Status> SynchronousServer::Impl::FeedInboundBytes(
    std::span<const std::uint8_t> wire) noexcept {
  std::size_t offset = 0uz;
  while (offset < wire.size() || h2->IsIngressPaused()) {
    const auto fed = h2->FeedBytes(wire.subspan(offset));
    if (uarch::unlikely(!fed.Ok())) {
      return std::unexpected(MapSrvH2Error(fed.ErrorValue()));
    }
    offset += fed.Consumed();
    const bool had_events = h2->PeekEventView().has_value();
    DrainEvents();
    if (uarch::unlikely(!fatal_error.Ok())) return std::unexpected(fatal_error);
    if (closed) return {};
    if (h2->IsIngressPaused()) {
      const auto before_out = h2->PendingOutput().size();
      if (auto flushed = FlushWire(); !flushed) {
        if (IsPeerTransportClosed(flushed.error())) return {};
        return flushed;
      }
      const bool flushed_any = h2->PendingOutput().size() < before_out;
      if (fed.Consumed() == 0uz && !had_events && !flushed_any) {
        if (offset < wire.size()) {
          return std::unexpected(MapSrvH2Error(nexora::Error::BufferOverflow()));
        }
        break;
      }
    }
  }
  return {};
}

std::expected<void, Status> SynchronousServer::Impl::DrainInbound() noexcept {
  for (std::size_t iter = 0uz; iter < 64uz; ++iter) {
    if (closed) return {};
    auto read_res = transport.ReadSome(rx_scratch);
    if (!read_res) {
      if (IsPeerTransportClosed(read_res.error())) return {};
      return std::unexpected(std::move(read_res.error()));
    }
    if (read_res->eof != 0U || read_res->would_block != 0U || read_res->bytes == 0uz) return {};
    const auto bytes_u8 =
        sync_detail::AsUint8Span(std::span<std::byte>{rx_scratch.data(), read_res->bytes});
    if (auto fed = FeedInboundBytes(bytes_u8); uarch::unlikely(!fed.has_value())) {
      return fed;
    }
  }
  return {};
}

std::expected<void, Status> SynchronousServer::Impl::FinishPeerClose() noexcept {
  // Buffered requests can still sit in the pipe after the peer has closed its read side.
  if (auto drained = DrainInbound(); !drained) return drained;
  CloseOnPeerEof();
  if (uarch::unlikely(!fatal_error.Ok())) return std::unexpected(fatal_error);
  return {};
}

void SynchronousServer::Impl::CloseOnPeerEof() noexcept {
  if (peer_eof) return;
  peer_eof = true;
  std::array<std::uint32_t, kMaxSlots> idxs{};
  std::size_t count = 0uz;
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    idxs[count++] = static_cast<std::uint32_t>(idx);
  }
  for (std::size_t i = 0uz; i < count; ++i) {
    auto& slot = streams[idxs[i]];
    if (!slot.IsActive()) continue;
    ++cancelled_streams;
    Dispatch(slot, ServerRequest::Event::kCancel);
    if (slot.IsActive()) ReleaseSlot(slot);
  }
  closed = true;
}

std::expected<void, Status> SynchronousServer::Impl::EnsureStarted(StreamSlot& slot) noexcept {
  if (slot.IsStarted()) return {};
  auto headers = h2::BuildGrpcResponseHeaders(slot.response_encoding, &slot.initial,
                                              "application/grpc", options.max_metadata_bytes);
  if (uarch::unlikely(!headers.has_value())) return std::unexpected(std::move(headers.error()));
  if (OutputFreeBytes() < 65536uz) {
    if (auto flushed = FlushWire(); !flushed) return flushed;
  }
  auto submitted = h2->SubmitResponseHeaders(static_cast<std::uint32_t>(slot.id),
                                             nexora::StatusCode{200U}, *headers, false);
  if (uarch::unlikely(!submitted.has_value())) {
    return std::unexpected(MapSrvH2Error(submitted.error()));
  }
  slot.meta = slot.meta.with<^^ServerStreamMetaSpec::started>(true);
  return {};
}

std::expected<void, Status> SynchronousServer::Impl::PumpOutgoing() noexcept {
  for (const auto idx : active_streams_mask | uarch::views::ones) {
    auto& slot = streams[idx];
    if (!slot.IsActive() || slot.IsRejected() || slot.IsTrailersSubmitted()) continue;

    while (!slot.output.empty()) {
      const auto rem = slot.output.front().WireBytes().subspan(slot.output_offset);
      const auto chunk = sync_detail::AsUint8Span(rem);
      auto sent = h2->SubmitData(static_cast<std::uint32_t>(slot.id), chunk, false);
      if (uarch::unlikely(!sent.has_value())) {
        return std::unexpected(MapSrvH2Error(sent.error()));
      }
      if (*sent == 0uz) break;
      slot.output_offset += *sent;
      slot.queued_bytes -= *sent;
      if (*sent == rem.size()) {
        slot.output.pop_front();
        slot.output_offset = 0uz;
      }
    }

    if (slot.output.empty() && slot.IsFinished() && !slot.IsTrailersSubmitted()) {
      if (OutputFreeBytes() < 65536uz) {
        if (auto flushed = FlushWire(); !flushed) return flushed;
      }
      auto trailers =
          h2::BuildGrpcTrailers(slot.status, &slot.trailers, options.max_metadata_bytes);
      if (uarch::unlikely(!trailers.has_value())) {
        return std::unexpected(std::move(trailers.error()));
      }
      auto submitted = h2->SubmitTrailers(static_cast<std::uint32_t>(slot.id), *trailers);
      if (uarch::unlikely(!submitted.has_value())) {
        if (submitted.error() ==
            nexora::Error::FromProto(nexora::ProtoErrorCode::kBufferOverflow)) {
          continue;
        }
        return std::unexpected(MapSrvH2Error(submitted.error()));
      }
      slot.meta = slot.meta.with<^^ServerStreamMetaSpec::trailers_submitted>(true);
      MaybeReclaimCompleted(slot);
    }
  }
  return {};
}

std::expected<void, Status> SynchronousServer::Impl::FlushWire() noexcept {
  if (transport.IsSecure()) {
    if (auto stepped = transport.StepTls(); !stepped) return stepped;
  }
  for (std::size_t iter = 0uz; iter < 64uz; ++iter) {
    const auto pending = h2->PendingOutput();
    if (pending.empty()) break;
    auto wrote = transport.WriteSome(std::as_bytes(pending));
    if (uarch::unlikely(!wrote.has_value())) return std::unexpected(std::move(wrote.error()));
    if (wrote->bytes > 0uz) h2->ConsumeOutput(wrote->bytes);
    if (wrote->would_block != 0U || wrote->bytes == 0uz) break;
  }
  return {};
}

void SynchronousServer::Impl::Dispatch(StreamSlot& slot, ServerRequest::Event event,
                                       std::span<const std::byte> payload) noexcept {
  if (handler) {
    slot.meta = slot.meta.with<^^ServerStreamMetaSpec::in_dispatch>(true);
    const ServerRequest req{
        .stream_id = slot.id,
        .path = slot.path,
        .payload = payload,
        .metadata = slot.request_metadata,
        .event = event,
        .timeout_ns = slot.timeout_ns,
    };
    handler(*owner_server, req);
    const bool rejected = slot.IsRejected();
    slot.meta = slot.meta.with<^^ServerStreamMetaSpec::in_dispatch>(false);
    if (rejected && slot.IsActive()) ReleaseSlot(slot);
  } else if (event == ServerRequest::Event::kHalfClose && slot.IsActive() && !slot.IsFinished()) {
    if (auto fin = owner_server->Finish(slot.id, Status{StatusCode::kUnimplemented});
        uarch::unlikely(!fin.has_value())) {
      fatal_error = std::move(fin.error());
    }
  }
}

void SynchronousServer::Impl::HandleRequestHeaders(const nexora::Http2EventView& ev) noexcept {
  if (ev.stream_id > last_peer_stream_id) last_peer_stream_id = ev.stream_id;
  if (uarch::unlikely(ev.kind == nexora::Http2Event::Kind::kTrailers)) {
    if (auto* existing = Find(static_cast<std::int32_t>(ev.stream_id)); existing != nullptr) {
      RejectStream(*existing, nexora::Http2ErrorCode::kProtocolError);
    } else if (auto rst = h2->SubmitRstStream(ev.stream_id, nexora::Http2ErrorCode::kProtocolError);
               !rst &&
               rst.error() !=
                   nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed) &&
               fatal_error.Ok()) {
      fatal_error = MapSrvH2Error(rst.error());
    }
    return;
  }
  const auto free_idx = active_streams_mask.lowest_zero();
  if (uarch::unlikely(!free_idx.has_value() ||
                      active_streams_mask.count() >= options.max_concurrent_streams)) {
    if (auto rst = h2->SubmitRstStream(ev.stream_id, nexora::Http2ErrorCode::kRefusedStream);
        !rst &&
        rst.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed) &&
        fatal_error.Ok()) {
      fatal_error = MapSrvH2Error(rst.error());
    }
    return;
  }
  const auto fields =
      ev.headers != nullptr ? ev.headers->Fields() : std::span<const nexora::HeaderField>{};
  auto parsed =
      h2::ParseGrpcRequestHeaders(ev.method, ev.scheme, ev.authority, ev.path, fields,
                                  options.response_compression, options.max_metadata_bytes);
  if (uarch::unlikely(!parsed.has_value())) {
    if (auto rst = h2->SubmitRstStream(ev.stream_id, nexora::Http2ErrorCode::kProtocolError);
        !rst &&
        rst.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed) &&
        fatal_error.Ok()) {
      fatal_error = MapSrvH2Error(rst.error());
    }
    return;
  }

  auto& slot = streams[*free_idx];
  slot = StreamSlot{};
  slot.slot_index = static_cast<std::uint32_t>(*free_idx);
  slot.id = static_cast<std::int32_t>(ev.stream_id);
  slot.path.assign(parsed->path);
  slot.request_metadata = std::move(parsed->metadata);
  slot.timeout_ns = parsed->timeout_ns;
  slot.request_encoding = parsed->request_encoding;
  slot.response_encoding = parsed->negotiated_response_encoding;
  slot.parser = LengthPrefixedFrame{options.max_message_bytes};
  slot.meta = slot.meta.with<^^ServerStreamMetaSpec::active>(true);
  active_streams_mask = active_streams_mask.set(*free_idx);

  if (headers_handler) {
    slot.meta = slot.meta.with<^^ServerStreamMetaSpec::in_dispatch>(true);
    headers_handler(*owner_server, slot.id, slot.path, slot.request_metadata);
    slot.meta = slot.meta.with<^^ServerStreamMetaSpec::in_dispatch>(false);
    MaybeReclaimCompleted(slot);
  }
  if (ev.end_stream && slot.IsActive() && !slot.IsHalfClosed()) {
    slot.meta = slot.meta.with<^^ServerStreamMetaSpec::half_closed>(true);
    Dispatch(slot, ServerRequest::Event::kHalfClose);
    MaybeReclaimCompleted(slot);
  }
}

void SynchronousServer::Impl::HandleRequestData(const nexora::Http2EventView& ev) noexcept {
  const auto data_len = ev.data.size();
  const auto fc_bytes =
      ev.flow_control_bytes != 0U ? ev.flow_control_bytes : static_cast<std::uint32_t>(data_len);
  if (fc_bytes > 0U) {
    conn_unreplenished_recv_bytes += fc_bytes;
    if (conn_unreplenished_recv_bytes >= kWindowReplenishThreshold) {
      if (auto updated = h2->SubmitWindowUpdate(0U, conn_unreplenished_recv_bytes); !updated) {
        if (fatal_error.Ok()) fatal_error = MapSrvH2Error(updated.error());
        return;
      }
      conn_unreplenished_recv_bytes = 0U;
    }
  }

  auto* slot = Find(static_cast<std::int32_t>(ev.stream_id));
  if (slot == nullptr || !slot->IsActive() || slot->IsRejected()) return;
  if (uarch::unlikely(slot->IsHalfClosed())) {
    RejectStream(*slot, nexora::Http2ErrorCode::kProtocolError);
    return;
  }

  if (data_len > 0uz) {
    auto frames = slot->parser.Feed(std::as_bytes(ev.data));
    if (uarch::unlikely(!frames.has_value())) {
      RejectStream(*slot, nexora::Http2ErrorCode::kProtocolError);
      return;
    }
    for (auto& frame : *frames) {
      if (!slot->IsActive() || slot->IsRejected()) return;
      if (frame.Compressed()) {
        if (uarch::unlikely(slot->request_encoding == CompressionAlgorithm::kIdentity)) {
          RejectStream(*slot, nexora::Http2ErrorCode::kProtocolError);
          return;
        }
        auto decoded = compression.Decompress(slot->request_encoding, frame.Payload(),
                                              options.max_message_bytes);
        if (uarch::unlikely(!decoded.has_value())) {
          RejectStream(*slot, nexora::Http2ErrorCode::kInternalError);
          return;
        }
        Dispatch(*slot, ServerRequest::Event::kMessage, decoded->Payload());
      } else {
        Dispatch(*slot, ServerRequest::Event::kMessage, frame.Payload());
      }
    }
  }

  if (fc_bytes > 0U && !ev.end_stream && slot->IsActive() && !slot->IsFinished()) {
    slot->unreplenished_recv_bytes += fc_bytes;
    if (slot->unreplenished_recv_bytes >= kWindowReplenishThreshold) {
      if (auto updated = h2->SubmitWindowUpdate(ev.stream_id, slot->unreplenished_recv_bytes);
          !updated) {
        // A later frame in this read can close the stream before its WINDOW_UPDATE is submitted.
        if (updated.error() !=
            nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
          if (fatal_error.Ok()) fatal_error = MapSrvH2Error(updated.error());
          return;
        }
      }
      slot->unreplenished_recv_bytes = 0U;
    }
  }

  if (ev.end_stream && slot->IsActive() && !slot->IsHalfClosed()) {
    slot->meta = slot->meta.with<^^ServerStreamMetaSpec::half_closed>(true);
    if (uarch::unlikely(slot->parser.State() != LengthPrefixedFrame::ParseState::kReadingHeader ||
                        slot->parser.BufferedPayloadBytes() != 0uz)) {
      RejectStream(*slot, nexora::Http2ErrorCode::kProtocolError);
      return;
    }
    Dispatch(*slot, ServerRequest::Event::kHalfClose);
    MaybeReclaimCompleted(*slot);
  }
}

void SynchronousServer::Impl::DrainEvents() noexcept {
  while (const auto ev = h2->PeekEventView()) {
    switch (ev->kind) {
      case nexora::Http2Event::Kind::kNone:
        break;
      case nexora::Http2Event::Kind::kHeaders:
      case nexora::Http2Event::Kind::kTrailers:
        HandleRequestHeaders(*ev);
        break;
      case nexora::Http2Event::Kind::kData:
        HandleRequestData(*ev);
        break;
      case nexora::Http2Event::Kind::kMessageComplete:
        if (auto* slot = Find(static_cast<std::int32_t>(ev->stream_id));
            slot != nullptr && slot->IsActive()) {
          if (!slot->IsHalfClosed()) {
            slot->meta = slot->meta.with<^^ServerStreamMetaSpec::half_closed>(true);
            if (slot->parser.State() != LengthPrefixedFrame::ParseState::kReadingHeader ||
                slot->parser.BufferedPayloadBytes() != 0uz) {
              RejectStream(*slot, nexora::Http2ErrorCode::kProtocolError);
            } else {
              Dispatch(*slot, ServerRequest::Event::kHalfClose);
              MaybeReclaimCompleted(*slot);
            }
          } else {
            MaybeReclaimCompleted(*slot);
          }
        }
        break;
      case nexora::Http2Event::Kind::kStreamReset:
        ++cancelled_streams;
        if (auto* slot = Find(static_cast<std::int32_t>(ev->stream_id));
            slot != nullptr && slot->IsActive()) {
          Dispatch(*slot, ServerRequest::Event::kCancel);
          if (slot->IsActive()) ReleaseSlot(*slot);
        }
        break;
      case nexora::Http2Event::Kind::kWindowUpdate:
        ++window_updates_received;
        break;
      case nexora::Http2Event::Kind::kGoaway:
      case nexora::Http2Event::Kind::kSettingsAck:
      case nexora::Http2Event::Kind::kPingAck:
        break;
    }
    h2->ConsumeEvent();
  }
}

}  // namespace grpcxx
