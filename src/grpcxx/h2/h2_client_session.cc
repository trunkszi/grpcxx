#include <grpcxx/h2/h2_client_session.h>
#include <grpcxx/h2/session_detail.h>

#include <utility>

namespace grpcxx {
struct H2ClientSession::Impl {
  static_assert(std::has_single_bit(kMaxStreams));
  struct Call {
    std::uint32_t id{};
    void* context{};
    Completion completion{};
    MessageCallback message{};
    h2_detail::MessageBuffer input, output;
    std::size_t sent{};
    int status{2};
    h2_detail::FlagState flags;
    int http_status{200};
    nexora::HeaderValueText type;
    nexora::HeaderValueText grpc_message;
  };
  h2_detail::SessionCore core{nexora::Http2Role::kClient};
  std::array<Call, kMaxStreams> calls;
  uarch::Bitmap64 active{};
  uarch::Bitmap64 pinned{};
  std::uint32_t flush_cursor{};
  nexora::HeaderValueText scheme;
  nexora::HeaderValueText authority;
  std::uint64_t default_timeout_ns{};
  std::array<char, 512> last_message{};
  std::size_t last_message_size{};

  Impl() noexcept {
    static_cast<void>(scheme.TryAssign("http"));
    static_cast<void>(authority.TryAssign("localhost"));
  }

  Call* Find(std::uint32_t id) noexcept {
    for (const auto slot : active | uarch::views::ones)
      if (calls[slot].id == id) return &calls[slot];
    return nullptr;
  }

  [[nodiscard]] bool HasUnsentCallData() const noexcept {
    for (const auto slot : active | uarch::views::ones) {
      const auto& call = calls[slot];
      if (call.sent < call.output.size) return true;
      if (call.flags.Test<^^h2_detail::RuntimeFlags::end_requested>() &&
          !call.flags.Test<^^h2_detail::RuntimeFlags::ended>())
        return true;
    }
    return false;
  }

  void Finish(Call& call, int status) noexcept {
    const auto index = static_cast<std::uint32_t>(&call - calls.data());
    auto completion = std::exchange(call.completion, nullptr);
    auto payload = call.input.Payload();
    const auto decoded = h2_detail::PercentDecodeInto(call.grpc_message.View(), last_message);
    last_message_size = decoded.size();
    active = active.clear(index);
    if (completion)
      completion(call.context, static_cast<std::int32_t>(call.id), status,
                 payload ? *payload : std::span<const std::byte>{});
  }

  bool Flush() noexcept {
    if (!core.FlushCredits()) return false;
    const auto closed_err = nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed);
    bool progressed = true;
    while (progressed && !active.is_empty()) {
      progressed = false;
      for (std::size_t step = 0; step < kMaxStreams; ++step) {
        const auto slot = static_cast<std::uint32_t>((flush_cursor + step) & (kMaxStreams - 1));
        if (!active.test(slot)) continue;
        auto& call = calls[slot];
        if (call.sent < call.output.size) {
          const auto budget = core.DataBudget();
          if (budget == 0) return true;
          const auto rem = call.output.WireBytes().subspan(call.sent);
          const auto chunk = rem.first(std::min(rem.size(), budget));
          const bool want_end = (chunk.size() == rem.size()) &&
                                call.flags.Test<^^h2_detail::RuntimeFlags::end_requested>();
          auto sent = core.h2.SubmitData(call.id, h2_detail::Wire(chunk), want_end);
          if (!sent) {
            if (sent.error() == closed_err) {
              call.sent = call.output.size;
              call.flags.Set<^^h2_detail::RuntimeFlags::ended>(true);
              continue;
            }
            if (sent.error() == nexora::Error::BufferOverflow()) return true;
            return false;
          }
          if (*sent > 0) {
            call.sent += *sent;
            if (call.sent == call.output.size && want_end)
              call.flags.Set<^^h2_detail::RuntimeFlags::ended>(true);
            flush_cursor = (slot + 1U) & (kMaxStreams - 1U);
            progressed = true;
          }
        } else if (call.flags.Test<^^h2_detail::RuntimeFlags::end_requested>() &&
                   !call.flags.Test<^^h2_detail::RuntimeFlags::ended>()) {
          if (core.DataBudget() == 0) return true;
          auto ended = core.h2.SubmitData(call.id, {}, true);
          if (ended || ended.error() == closed_err) {
            call.flags.Set<^^h2_detail::RuntimeFlags::ended>(true);
            flush_cursor = (slot + 1U) & (kMaxStreams - 1U);
            progressed = true;
          } else if (ended.error() != nexora::Error::BufferOverflow()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  static int MapResetStatus(nexora::Http2ErrorCode code, const Call& call) noexcept {
    switch (code) {
      case nexora::Http2ErrorCode::kNoError:
        return call.flags.Test<^^h2_detail::RuntimeFlags::status_seen>() ? call.status : 13;
      case nexora::Http2ErrorCode::kRefusedStream:
        return 14;
      case nexora::Http2ErrorCode::kEnhanceYourCalm:
        return 8;
      case nexora::Http2ErrorCode::kCancel:
        return 1;
      default:
        return 13;
    }
  }

  static int MapHttpFallback(int http_status) noexcept {
    switch (http_status) {
      case 400:
        return 13;
      case 401:
        return 16;
      case 403:
        return 7;
      case 404:
        return 12;
      case 429:
      case 502:
      case 503:
      case 504:
        return 14;
      default:
        return (http_status >= 200 && http_status < 300) ? 13 : 2;
    }
  }

  bool Events() noexcept {
    while (auto event = core.h2.PeekEventView()) {
      auto* call = Find(event->stream_id);
      if (event->kind == nexora::Http2Event::Kind::kGoaway)
        core.flags.Set<^^h2_detail::RuntimeFlags::draining>(true);
      if (event->kind == nexora::Http2Event::Kind::kData) {
        const auto fc_bytes = event->flow_control_bytes != 0U
                                  ? std::size_t{event->flow_control_bytes}
                                  : event->data.size();
        if (!call) {
          if (!core.Credit(event->stream_id, fc_bytes, true)) return false;
          core.h2.ConsumeEvent();
          continue;
        }
        const auto slot = static_cast<std::uint32_t>(call - calls.data());
        if (call->flags.Test<^^h2_detail::RuntimeFlags::streaming>()) {
          pinned = pinned.set(slot);
          const bool ok = call->input.Decode(
              event->data,
              [&](std::span<const std::byte> payload) {
                call->message(call->context, static_cast<std::int32_t>(call->id), payload);
              },
              [&] { return active.test(slot); });
          pinned = pinned.clear(slot);
          if (!active.test(slot)) {
            if (!core.Credit(event->stream_id, fc_bytes, true)) return false;
            core.h2.ConsumeEvent();
            continue;
          }
          if (!ok) {
            call->status = 13;
            call->flags.Set<^^h2_detail::RuntimeFlags::invalid>(true);
          }
        } else if (!call->input.Append(event->data)) {
          call->status = 8;
          call->flags.Set<^^h2_detail::RuntimeFlags::invalid>(true);
        }
        if (!core.Credit(call->id, fc_bytes, event->end_stream)) return false;
      } else if (call) {
        if (event->kind == nexora::Http2Event::Kind::kHeaders ||
            event->kind == nexora::Http2Event::Kind::kTrailers) {
          if (event->kind == nexora::Http2Event::Kind::kHeaders)
            call->http_status = event->status.AsU16();
          if (auto type = event->headers->Get("content-type"))
            call->flags.Set<^^h2_detail::RuntimeFlags::type_ok>(
                h2_detail::AcceptsContentType(call->type.View(), *type));
          if (auto msg = event->headers->Get("grpc-message"))
            static_cast<void>(call->grpc_message.TryAssign(*msg));
          if (auto status = event->headers->Get("grpc-status")) {
            int parsed{};
            auto result = std::from_chars(status->data(), status->data() + status->size(), parsed);
            call->status = result.ec == std::errc{} &&
                                   result.ptr == status->data() + status->size() && parsed >= 0 &&
                                   parsed <= 16
                               ? parsed
                               : 2;
            call->flags.Set<^^h2_detail::RuntimeFlags::status_seen>(true);
            if (!event->end_stream) {
              call->flags.Set<^^h2_detail::RuntimeFlags::invalid>(true);
              ++core.rejected;
            }
          }
        } else if (event->kind == nexora::Http2Event::Kind::kStreamReset) {
          Finish(*call, MapResetStatus(event->error_code, *call));
        } else if (event->kind == nexora::Http2Event::Kind::kMessageComplete) {
          auto status =
              call->http_status == 200 && call->flags.Test<^^h2_detail::RuntimeFlags::status_seen>()
                  ? call->status
                  : MapHttpFallback(call->http_status);
          if (call->http_status == 200 && call->flags.Test<^^h2_detail::RuntimeFlags::invalid>())
            status = call->status == 8 ? 8 : 13;
          if (status == 0 &&
              (!call->flags.Test<^^h2_detail::RuntimeFlags::type_ok>() ||
               (call->flags.Test<^^h2_detail::RuntimeFlags::streaming>() ? call->input.size != 0
                                                                         : !call->input.Payload())))
            status = 13;
          Finish(*call, status);
        }
      }
      core.h2.ConsumeEvent();
    }
    return Flush();
  }
};

H2ClientSession::H2ClientSession() noexcept : impl_(std::make_unique<Impl>()) {}
H2ClientSession::~H2ClientSession() = default;
bool H2ClientSession::Ready() const noexcept {
  return !impl_->core.flags.Test<^^h2_detail::RuntimeFlags::failed>() &&
         !impl_->core.flags.Test<^^h2_detail::RuntimeFlags::draining>();
}
bool H2ClientSession::HasPendingOutput() const noexcept {
  return !impl_->core.h2.PendingOutput().empty() || impl_->core.HasPendingControl() ||
         impl_->HasUnsentCallData();
}
H2ClientSession::State H2ClientSession::GetState() const noexcept {
  return impl_->core.flags.Test<^^h2_detail::RuntimeFlags::failed>()     ? State::kFailed
         : impl_->core.flags.Test<^^h2_detail::RuntimeFlags::draining>() ? State::kDraining
                                                                         : State::kOpen;
}
std::size_t H2ClientSession::ActiveStreams() const noexcept { return impl_->active.count(); }
std::uint64_t H2ClientSession::RejectedTransitions() const noexcept { return impl_->core.rejected; }

std::expected<void, Status> H2ClientSession::SetTarget(std::string_view scheme,
                                                       std::string_view authority) noexcept {
  if (scheme.empty() || authority.empty() || !nexora::IsValidHeaderValue(scheme) ||
      !nexora::IsValidHeaderValue(authority))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  nexora::HeaderValueText next_scheme, next_authority;
  if (!next_scheme.TryAssign(scheme) || !next_authority.TryAssign(authority))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  impl_->scheme = std::move(next_scheme);
  impl_->authority = std::move(next_authority);
  return {};
}

void H2ClientSession::SetDefaultTimeoutNs(std::uint64_t timeout_ns) noexcept {
  impl_->default_timeout_ns = timeout_ns;
}

std::string_view H2ClientSession::LastStatusMessage() const noexcept {
  return {impl_->last_message.data(), impl_->last_message_size};
}

std::expected<std::size_t, Status> H2ClientSession::Receive(
    std::span<const std::byte> bytes) noexcept {
  auto& i = *impl_;
  if (i.core.flags.Test<^^h2_detail::RuntimeFlags::driving>()) {
    ++i.core.rejected;
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  h2_detail::DriveScope scope(i.core.flags);
  if (i.core.flags.Test<^^h2_detail::RuntimeFlags::failed>())
    return std::unexpected(Status{StatusCode::kUnavailable});
  if (!i.core.Feed(bytes, [&] { return i.Events(); })) {
    i.core.flags.Set<^^h2_detail::RuntimeFlags::failed>(true);
    for (const auto slot : i.active | uarch::views::ones) i.Finish(i.calls[slot], 14);
    return std::unexpected(Status{StatusCode::kInternal});
  }
  return bytes.size();
}

std::expected<std::span<const std::byte>, Status> H2ClientSession::Send() noexcept {
  if (impl_->core.flags.Test<^^h2_detail::RuntimeFlags::driving>()) {
    ++impl_->core.rejected;
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  if (!impl_->Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return impl_->core.Send();
}

std::expected<void, Status> H2ClientSession::Ping() noexcept {
  if (auto r = impl_->core.h2.SubmitPing(0); !r)
    return std::unexpected(h2_detail::ProtocolError(r.error()));
  return {};
}

std::expected<void, Status> H2ClientSession::GoAway() noexcept {
  impl_->core.flags.Set<^^h2_detail::RuntimeFlags::draining>(true);
  if (auto r = impl_->core.h2.SubmitGoaway(0x7fffffffU, nexora::Http2ErrorCode::kNoError); !r)
    return std::unexpected(h2_detail::ProtocolError(r.error()));
  return {};
}

std::expected<void, Status> H2ClientSession::Cancel(std::int32_t stream) noexcept {
  if (stream <= 0) return std::unexpected(Status{StatusCode::kNotFound});
  if (auto* call = impl_->Find(static_cast<std::uint32_t>(stream))) {
    const auto id = call->id;
    call->completion = nullptr;
    call->message = nullptr;
    impl_->active = impl_->active.clear(static_cast<std::uint32_t>(call - impl_->calls.data()));
    if (!impl_->core.QueueOrSubmitRst(id, nexora::Http2ErrorCode::kCancel))
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    return {};
  }
  return std::unexpected(Status{StatusCode::kNotFound});
}

std::expected<std::int32_t, Status> H2ClientSession::Open(std::string_view path, void* context,
                                                          MessageCallback message,
                                                          Completion completion,
                                                          std::string_view content_type,
                                                          std::uint64_t timeout_ns) noexcept {
  auto& i = *impl_;
  if (!Ready()) return std::unexpected(Status{StatusCode::kUnavailable});
  if (!completion || path.empty() || !path.starts_with('/') ||
      path.size() > h2_detail::kMaxPathBytes || !nexora::IsValidHeaderValue(path) ||
      !h2_detail::ValidContentType(content_type))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  nexora::HeaderValueText call_type;
  if (!call_type.TryAssign(content_type))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  nexora::Headers<> headers;
  if (!headers.Add("content-type", content_type) || !headers.Add("te", "trailers"))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  const auto effective_timeout = timeout_ns != 0 ? timeout_ns : i.default_timeout_ns;
  if (effective_timeout != 0) {
    std::array<char, 16> timeout_buf{};
    const auto formatted = GrpcTimeout::FormatTo(effective_timeout, timeout_buf);
    if (formatted.empty() || !headers.Add("grpc-timeout", formatted))
      return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  const auto slot = (i.active | i.pinned).lowest_zero();
  if (!slot || *slot >= kMaxStreams || !i.core.HeaderReady())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto id = i.core.h2.OpenStream();
  if (!id) {
    const auto err = id.error();
    return std::unexpected(
        Status{err == nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2RefusedStream)
                   ? StatusCode::kResourceExhausted
                   : StatusCode::kUnavailable});
  }
  auto r = i.core.h2.SubmitRequestHeaders(*id, nexora::Method::kPost, i.scheme.View(),
                                          i.authority.View(), path, headers, false);
  if (!r) return std::unexpected(h2_detail::ProtocolError(r.error()));
  auto& call = i.calls[*slot];
  call.id = *id;
  call.context = context;
  call.completion = completion;
  call.message = message;
  call.sent = 0;
  call.status = 2;
  call.flags.Reset();
  call.flags.Set<^^h2_detail::RuntimeFlags::streaming>(message != nullptr);
  call.http_status = 200;
  call.input.Reset();
  call.output.Reset();
  call.type = std::move(call_type);
  call.grpc_message.Clear();
  i.active = i.active.set(*slot);
  if (!i.Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return static_cast<std::int32_t>(*id);
}

std::expected<void, Status> H2ClientSession::SendMessage(
    std::int32_t stream, std::span<const std::byte> payload) noexcept {
  if (stream <= 0) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto* call = impl_->Find(static_cast<std::uint32_t>(stream));
  if (!call || call->flags.Test<^^h2_detail::RuntimeFlags::end_requested>())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (call->sent != call->output.size)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  if (!call->output.Assign(payload)) return std::unexpected(Status{StatusCode::kResourceExhausted});
  call->sent = 0;
  if (!impl_->Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return {};
}

std::expected<void, Status> H2ClientSession::HalfClose(std::int32_t stream) noexcept {
  if (stream <= 0) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto* call = impl_->Find(static_cast<std::uint32_t>(stream));
  if (!call || call->flags.Test<^^h2_detail::RuntimeFlags::end_requested>())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  call->flags.Set<^^h2_detail::RuntimeFlags::end_requested>(true);
  if (!impl_->Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return {};
}

std::expected<std::int32_t, Status> H2ClientSession::Submit(std::string_view path,
                                                            std::span<const std::byte> payload,
                                                            void* context, Completion completion,
                                                            std::string_view type,
                                                            std::uint64_t timeout_ns) noexcept {
  if (payload.size() > kMaxMessageBytes)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto stream = Open(path, context, nullptr, completion, type, timeout_ns);
  if (!stream) return stream;
  auto* call = impl_->Find(static_cast<std::uint32_t>(*stream));
  if (!call->output.Assign(payload)) {
    static_cast<void>(Cancel(*stream));
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  }
  call->flags.Set<^^h2_detail::RuntimeFlags::end_requested>(true);
  call->sent = 0;
  if (!impl_->Flush()) return std::unexpected(Status{StatusCode::kInternal});
  return stream;
}
}  // namespace grpcxx
