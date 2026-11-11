#include <grpcxx/h2/h2_server_session.h>
#include <grpcxx/h2/session_detail.h>

namespace grpcxx {
struct H2ServerSession::Impl {
  static_assert(std::has_single_bit(kMaxStreams));
  struct Call {
    std::uint32_t id{};
    std::uint64_t generation{};
    h2_detail::MessageBuffer input, output;
    std::size_t sent{};
    int status{};
    std::uint64_t timeout_ns{};
    h2_detail::FlagState flags;
    nexora::HeaderValueText path, response_type, scheme, authority, grpc_message;
  };
  h2_detail::SessionCore core{nexora::Http2Role::kServer};
  std::array<Call, kMaxStreams> calls;
  uarch::Bitmap64 active{};
  uarch::Bitmap64 pinned{};
  std::uint32_t flush_cursor{};
  std::uint32_t last_stream_id{};
  nexora::HeaderValueText type;
  void* context{};
  RequestHandler handler{};
  StreamHandler stream_handler{};

  explicit Impl(std::string_view media) noexcept {
    core.flags.Set<^^h2_detail::RuntimeFlags::failed>(
        core.flags.Test<^^h2_detail::RuntimeFlags::failed>() ||
        !h2_detail::ValidContentType(media) || !type.TryAssign(media));
  }

  Call* Find(std::uint32_t id) noexcept {
    for (const auto slot : active | uarch::views::ones)
      if (calls[slot].id == id) return &calls[slot];
    return nullptr;
  }

  const Call* Find(std::uint32_t id) const noexcept {
    for (const auto slot : active | uarch::views::ones)
      if (calls[slot].id == id) return &calls[slot];
    return nullptr;
  }

  void Retire(Call& call) noexcept {
    active = active.clear(static_cast<std::uint32_t>(&call - calls.data()));
    call.flags.Set<^^h2_detail::RuntimeFlags::dispatched>(false);
  }

  void RejectCall(Call& call, int code, bool peer_ended) noexcept {
    if (!call.flags.Test<^^h2_detail::RuntimeFlags::rejected>()) {
      call.flags.Set<^^h2_detail::RuntimeFlags::rejected>(true);
      ++core.rejected;
    }
    call.status = code;
    call.flags.Set<^^h2_detail::RuntimeFlags::dispatched>(false);
    call.flags.Set<^^h2_detail::RuntimeFlags::sending>(true);
    call.flags.Set<^^h2_detail::RuntimeFlags::finish_requested>(true);
    call.flags.Set<^^h2_detail::RuntimeFlags::trailers_only>(
        !call.flags.Test<^^h2_detail::RuntimeFlags::headers_sent>());
    call.flags.Set<^^h2_detail::RuntimeFlags::rst_after_reply>(!peer_ended);
    call.output.Reset();
    call.sent = 0;
    static_cast<void>(call.response_type.TryAssign(type.View()));
  }

  [[nodiscard]] bool EmitTrailersOnly(Call& call) noexcept {
    if (!core.HeaderReady()) return true;
    nexora::Headers<> headers;
    std::array<char, 4> digits{};
    if (!headers.Add("content-type", call.response_type.View()) ||
        !headers.Add("grpc-status", h2_detail::FormatGrpcStatus(call.status, digits)))
      return false;
    if (!call.grpc_message.View().empty() && !headers.Add("grpc-message", call.grpc_message.View()))
      return false;
    auto r = core.h2.SubmitResponseHeaders(call.id, nexora::StatusCode{200}, headers, true);
    if (!r) {
      if (r.error() == nexora::Error::BufferOverflow()) return true;
      if (r.error() == nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
        Retire(call);
        return true;
      }
      return false;
    }
    if (call.flags.Test<^^h2_detail::RuntimeFlags::rst_after_reply>() &&
        !core.QueueOrSubmitRst(call.id, nexora::Http2ErrorCode::kNoError))
      return false;
    Retire(call);
    return true;
  }

  [[nodiscard]] bool EmitFinalTrailers(Call& call) noexcept {
    if (!core.HeaderReady()) return true;
    nexora::Headers<> trailers;
    std::array<char, 4> digits{};
    if (!trailers.Add("grpc-status", h2_detail::FormatGrpcStatus(call.status, digits)))
      return false;
    if (!call.grpc_message.View().empty() &&
        !trailers.Add("grpc-message", call.grpc_message.View()))
      return false;
    auto r = core.h2.SubmitTrailers(call.id, trailers);
    if (!r) {
      if (r.error() == nexora::Error::BufferOverflow()) return true;
      if (r.error() == nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
        Retire(call);
        return true;
      }
      return false;
    }
    if (call.flags.Test<^^h2_detail::RuntimeFlags::rst_after_reply>() &&
        !core.QueueOrSubmitRst(call.id, nexora::Http2ErrorCode::kNoError))
      return false;
    Retire(call);
    return true;
  }

  bool Flush() noexcept {
    if (!core.FlushCredits()) return false;
    const auto closed_err = nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed);
    for (const auto slot : active | uarch::views::ones) {
      auto& call = calls[slot];
      if (!call.flags.Test<^^h2_detail::RuntimeFlags::sending>()) continue;
      if (call.flags.Test<^^h2_detail::RuntimeFlags::trailers_only>()) {
        if (!EmitTrailersOnly(call)) return false;
        continue;
      }
      if (!call.flags.Test<^^h2_detail::RuntimeFlags::headers_sent>()) {
        if (!core.HeaderReady()) continue;
        nexora::Headers<> headers;
        if (!headers.Add("content-type", call.response_type.View())) return false;
        auto r = core.h2.SubmitResponseHeaders(call.id, nexora::StatusCode{200}, headers, false);
        if (!r) {
          if (r.error() == nexora::Error::BufferOverflow()) continue;
          if (r.error() == closed_err) {
            Retire(call);
            continue;
          }
          return false;
        }
        call.flags.Set<^^h2_detail::RuntimeFlags::headers_sent>(true);
      }
      if (call.sent == call.output.size) {
        if (!call.flags.Test<^^h2_detail::RuntimeFlags::finish_requested>())
          call.flags.Set<^^h2_detail::RuntimeFlags::sending>(false);
        else if (!EmitFinalTrailers(call))
          return false;
      }
    }
    bool progressed = true;
    while (progressed && !active.is_empty()) {
      progressed = false;
      for (std::size_t step = 0; step < kMaxStreams; ++step) {
        const auto slot = static_cast<std::uint32_t>((flush_cursor + step) & (kMaxStreams - 1));
        if (!active.test(slot)) continue;
        auto& call = calls[slot];
        if (!call.flags.Test<^^h2_detail::RuntimeFlags::sending>() ||
            !call.flags.Test<^^h2_detail::RuntimeFlags::headers_sent>() ||
            call.sent >= call.output.size)
          continue;
        const auto budget = core.DataBudget();
        if (budget == 0) return true;
        const auto rem = call.output.WireBytes().subspan(call.sent);
        const auto chunk = rem.first(std::min(rem.size(), budget));
        auto sent = core.h2.SubmitData(call.id, h2_detail::Wire(chunk), false);
        if (!sent) {
          if (sent.error() == closed_err) {
            Retire(call);
            continue;
          }
          if (sent.error() == nexora::Error::BufferOverflow()) return true;
          return false;
        }
        if (*sent > 0) {
          call.sent += *sent;
          flush_cursor = (slot + 1U) & (kMaxStreams - 1U);
          progressed = true;
          if (call.sent == call.output.size) {
            if (!call.flags.Test<^^h2_detail::RuntimeFlags::finish_requested>())
              call.flags.Set<^^h2_detail::RuntimeFlags::sending>(false);
            else if (!EmitFinalTrailers(call))
              return false;
          }
        }
      }
    }
    return true;
  }

  bool Events() noexcept {
    while (auto event = core.h2.PeekEventView()) {
      auto* call = Find(event->stream_id);
      if (event->kind == nexora::Http2Event::Kind::kGoaway)
        core.flags.Set<^^h2_detail::RuntimeFlags::draining>(true);
      if (event->kind == nexora::Http2Event::Kind::kHeaders && !call) {
        last_stream_id = std::max(last_stream_id, event->stream_id);
        const auto slot = (active | pinned).lowest_zero();
        if (core.flags.Test<^^h2_detail::RuntimeFlags::draining>() || !slot ||
            *slot >= kMaxStreams ||
            calls[*slot].generation == std::numeric_limits<std::uint64_t>::max()) {
          ++core.rejected;
          if (!core.QueueOrSubmitRst(event->stream_id, nexora::Http2ErrorCode::kRefusedStream))
            return false;
          core.h2.ConsumeEvent();
          continue;
        }
        call = &calls[*slot];
        ++call->generation;
        call->id = event->stream_id;
        call->sent = 0;
        call->status = 0;
        call->timeout_ns = 0;
        call->flags.Reset();
        call->input.Reset();
        call->output.Reset();
        call->path.Clear();
        call->scheme.Clear();
        call->authority.Clear();
        call->grpc_message.Clear();
        static_cast<void>(call->scheme.TryAssign(event->scheme));
        static_cast<void>(call->authority.TryAssign(event->authority));
        active = active.set(*slot);

        int reject_code = 0;
        const auto media = event->headers->Get("content-type");
        if (event->method != nexora::Method::kPost || !media ||
            !h2_detail::AcceptsContentType(type.View(), *media)) {
          reject_code = 13;
        } else if (event->path.empty() || !event->path.starts_with('/') ||
                   event->path.size() > h2_detail::kMaxPathBytes ||
                   !call->path.TryAssign(event->path)) {
          reject_code = 12;
        } else if (const auto timeout = event->headers->Get("grpc-timeout")) {
          auto parsed = GrpcTimeout::Parse(*timeout);
          if (!parsed)
            reject_code = 3;
          else
            call->timeout_ns = *parsed;
        }
        if (reject_code != 0) {
          RejectCall(*call, reject_code, event->end_stream);
        } else if (stream_handler) {
          call->flags.Set<^^h2_detail::RuntimeFlags::dispatched>(true);
        }
      } else if (event->kind == nexora::Http2Event::Kind::kData) {
        const auto fc_bytes = event->flow_control_bytes != 0U
                                  ? std::size_t{event->flow_control_bytes}
                                  : event->data.size();
        if (!call) {
          if (!core.Credit(event->stream_id, fc_bytes, true)) return false;
          core.h2.ConsumeEvent();
          continue;
        }
        if (call->flags.Test<^^h2_detail::RuntimeFlags::rejected>()) {
          if (event->end_stream) call->flags.Set<^^h2_detail::RuntimeFlags::rst_after_reply>(false);
          if (!core.Credit(call->id, fc_bytes, event->end_stream)) return false;
          core.h2.ConsumeEvent();
          continue;
        }
        const auto slot = static_cast<std::uint32_t>(call - calls.data());
        if (stream_handler) {
          pinned = pinned.set(slot);
          const bool ok = call->input.Decode(
              event->data,
              [&](std::span<const std::byte> payload) {
                stream_handler(context, static_cast<std::int32_t>(call->id), call->generation,
                               call->path.View(), payload, false);
              },
              [&] {
                return active.test(slot) &&
                       !call->flags.Test<^^h2_detail::RuntimeFlags::rejected>();
              });
          pinned = pinned.clear(slot);
          if (!active.test(slot)) {
            if (!core.Credit(event->stream_id, fc_bytes, true)) return false;
            core.h2.ConsumeEvent();
            continue;
          }
          if (!ok) RejectCall(*call, 13, event->end_stream);
        } else if (!call->input.Append(event->data)) {
          RejectCall(*call, 8, event->end_stream);
        }
        if (!core.Credit(call->id, fc_bytes, event->end_stream)) return false;
      } else if (call && event->kind == nexora::Http2Event::Kind::kStreamReset) {
        Retire(*call);
      } else if (call && event->kind == nexora::Http2Event::Kind::kMessageComplete) {
        call->flags.Set<^^h2_detail::RuntimeFlags::ended>(true);
        if (call->flags.Test<^^h2_detail::RuntimeFlags::rejected>()) {
          call->flags.Set<^^h2_detail::RuntimeFlags::rst_after_reply>(false);
          core.h2.ConsumeEvent();
          continue;
        }
        call->flags.Set<^^h2_detail::RuntimeFlags::dispatched>(true);
        if (stream_handler) {
          if (call->status == 0 && call->input.size == 0) {
            stream_handler(context, static_cast<std::int32_t>(call->id), call->generation,
                           call->path.View(), {}, true);
          } else {
            RejectCall(*call, 13, true);
          }
          core.h2.ConsumeEvent();
          continue;
        }
        auto payload = call->input.Payload();
        if (!payload || call->status != 0 || handler == nullptr) {
          if (!payload || call->status != 0) ++core.rejected;
          if (call->status == 0) call->status = payload ? 12 : 13;
          call->flags.Set<^^h2_detail::RuntimeFlags::sending>(
              call->flags.Set<^^h2_detail::RuntimeFlags::finish_requested>(true));
          if (!call->response_type.TryAssign(type.View())) return false;
        } else {
          handler(context, static_cast<std::int32_t>(call->id), call->generation, call->path.View(),
                  *payload);
        }
      }
      core.h2.ConsumeEvent();
    }
    return Flush();
  }
};

H2ServerSession::H2ServerSession(std::string_view type) noexcept
    : impl_(std::make_unique<Impl>(type)) {}
H2ServerSession::~H2ServerSession() = default;
bool H2ServerSession::HasPendingOutput() const noexcept {
  if (!impl_->core.h2.PendingOutput().empty() || impl_->core.HasPendingControl()) return true;
  for (const auto slot : impl_->active | uarch::views::ones)
    if (impl_->calls[slot].flags.Test<^^h2_detail::RuntimeFlags::sending>()) return true;
  return false;
}
bool H2ServerSession::Ready() const noexcept {
  return !impl_->core.flags.Test<^^h2_detail::RuntimeFlags::failed>() &&
         !impl_->core.flags.Test<^^h2_detail::RuntimeFlags::draining>();
}
H2ServerSession::State H2ServerSession::GetState() const noexcept {
  return impl_->core.flags.Test<^^h2_detail::RuntimeFlags::failed>()     ? State::kFailed
         : impl_->core.flags.Test<^^h2_detail::RuntimeFlags::draining>() ? State::kDraining
                                                                         : State::kOpen;
}
std::size_t H2ServerSession::ActiveStreams() const noexcept { return impl_->active.count(); }
std::uint64_t H2ServerSession::RejectedTransitions() const noexcept { return impl_->core.rejected; }

std::expected<std::size_t, Status> H2ServerSession::Receive(
    std::span<const std::byte> bytes) noexcept {
  auto& i = *impl_;
  if (i.core.flags.Test<^^h2_detail::RuntimeFlags::driving>()) {
    ++i.core.rejected;
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  if (i.core.flags.Test<^^h2_detail::RuntimeFlags::failed>())
    return std::unexpected(Status{StatusCode::kUnavailable});
  h2_detail::DriveScope scope(i.core.flags);
  if (!i.core.Feed(bytes, [&] { return i.Events(); })) {
    i.core.flags.Set<^^h2_detail::RuntimeFlags::failed>(true);
    i.active = uarch::Bitmap64::empty();
    return std::unexpected(Status{StatusCode::kInternal});
  }
  return bytes.size();
}

std::expected<std::span<const std::byte>, Status> H2ServerSession::Send() noexcept {
  if (impl_->core.flags.Test<^^h2_detail::RuntimeFlags::driving>()) {
    ++impl_->core.rejected;
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  if (!impl_->Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return impl_->core.Send();
}

std::expected<void, Status> H2ServerSession::Ping() noexcept {
  if (auto r = impl_->core.h2.SubmitPing(0); !r)
    return std::unexpected(h2_detail::ProtocolError(r.error()));
  return {};
}

std::expected<void, Status> H2ServerSession::GoAway() noexcept {
  impl_->core.flags.Set<^^h2_detail::RuntimeFlags::draining>(true);
  if (auto r = impl_->core.h2.SubmitGoaway(impl_->last_stream_id, nexora::Http2ErrorCode::kNoError);
      !r)
    return std::unexpected(h2_detail::ProtocolError(r.error()));
  return {};
}

std::expected<void, Status> H2ServerSession::OnPeerEof() noexcept {
  auto& i = *impl_;
  if (i.core.flags.Test<^^h2_detail::RuntimeFlags::driving>()) {
    ++i.core.rejected;
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  h2_detail::DriveScope scope(i.core.flags);
  i.core.flags.Set<^^h2_detail::RuntimeFlags::draining>(true);
  for (const auto slot : i.active | uarch::views::ones) {
    auto& call = i.calls[slot];
    if (call.flags.Test<^^h2_detail::RuntimeFlags::ended>()) continue;
    const bool notify =
        i.stream_handler && call.flags.Test<^^h2_detail::RuntimeFlags::dispatched>();
    i.Retire(call);
    if (notify)
      i.stream_handler(i.context, static_cast<std::int32_t>(call.id), call.generation,
                       call.path.View(), {}, true);
  }
  return {};
}

void H2ServerSession::SetRequestHandler(void* context, RequestHandler handler) noexcept {
  impl_->context = context;
  impl_->handler = handler;
}

bool H2ServerSession::IsCurrent(std::int32_t id, std::uint64_t generation) const noexcept {
  if (id <= 0 || impl_->core.flags.Test<^^h2_detail::RuntimeFlags::failed>()) return false;
  const auto* call = impl_->Find(static_cast<std::uint32_t>(id));
  return call && call->generation == generation &&
         call->flags.Test<^^h2_detail::RuntimeFlags::dispatched>() &&
         !call->flags.Test<^^h2_detail::RuntimeFlags::finish_requested>();
}

std::string_view H2ServerSession::StreamScheme(std::int32_t id) const noexcept {
  if (id <= 0) return {};
  const auto* call = impl_->Find(static_cast<std::uint32_t>(id));
  return call ? call->scheme.View() : std::string_view{};
}

std::string_view H2ServerSession::StreamAuthority(std::int32_t id) const noexcept {
  if (id <= 0) return {};
  const auto* call = impl_->Find(static_cast<std::uint32_t>(id));
  return call ? call->authority.View() : std::string_view{};
}

std::uint64_t H2ServerSession::StreamTimeoutNs(std::int32_t id) const noexcept {
  if (id <= 0) return 0;
  const auto* call = impl_->Find(static_cast<std::uint32_t>(id));
  return call ? call->timeout_ns : 0;
}

std::expected<void, Status> H2ServerSession::Complete(std::int32_t id, std::uint64_t generation,
                                                      std::span<const std::byte> payload,
                                                      int status, std::string_view type,
                                                      std::string_view grpc_message) noexcept {
  if (!IsCurrent(id, generation)) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (status < 0 || status > 16 || (!type.empty() && !h2_detail::ValidContentType(type)))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto& call = *impl_->Find(static_cast<std::uint32_t>(id));
  if (payload.size() > kMaxMessageBytes)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  if (!call.response_type.TryAssign(type.empty() ? impl_->type.View() : type))
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  call.grpc_message.Clear();
  if (!grpc_message.empty()) {
    std::array<char, 512> encoded_buf{};
    const auto encoded = h2_detail::PercentEncodeInto(grpc_message, encoded_buf);
    if (!call.grpc_message.TryAssign(encoded))
      return std::unexpected(Status{StatusCode::kResourceExhausted});
  }
  if (status == 0 || !payload.empty()) {
    if (!call.output.Assign(payload))
      return std::unexpected(Status{StatusCode::kResourceExhausted});
  } else {
    call.output.Reset();
  }
  call.sent = 0;
  call.status = status;
  call.flags.Set<^^h2_detail::RuntimeFlags::sending>(
      call.flags.Set<^^h2_detail::RuntimeFlags::finish_requested>(true));
  return {};
}

void H2ServerSession::SetStreamingHandler(void* context, StreamHandler handler) noexcept {
  impl_->context = context;
  impl_->stream_handler = handler;
}

std::expected<void, Status> H2ServerSession::SendMessage(
    std::int32_t id, std::uint64_t generation, std::span<const std::byte> payload) noexcept {
  if (!IsCurrent(id, generation)) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto& call = *impl_->Find(static_cast<std::uint32_t>(id));
  if (call.flags.Test<^^h2_detail::RuntimeFlags::sending>())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  if (!call.response_type.TryAssign(impl_->type.View()) || !call.output.Assign(payload))
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  call.sent = 0;
  call.flags.Set<^^h2_detail::RuntimeFlags::sending>(true);
  return {};
}

std::expected<void, Status> H2ServerSession::Finish(std::int32_t id, std::uint64_t generation,
                                                    int status,
                                                    std::string_view grpc_message) noexcept {
  if (!IsCurrent(id, generation)) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (status < 0 || status > 16) return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto& call = *impl_->Find(static_cast<std::uint32_t>(id));
  if (!call.response_type.TryAssign(impl_->type.View()))
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  call.grpc_message.Clear();
  if (!grpc_message.empty()) {
    std::array<char, 512> encoded_buf{};
    const auto encoded = h2_detail::PercentEncodeInto(grpc_message, encoded_buf);
    if (!call.grpc_message.TryAssign(encoded))
      return std::unexpected(Status{StatusCode::kResourceExhausted});
  }
  call.status = status;
  call.flags.Set<^^h2_detail::RuntimeFlags::finish_requested>(
      call.flags.Set<^^h2_detail::RuntimeFlags::sending>(true));
  return {};
}
}  // namespace grpcxx
