#include "grpcxx/h2/nghttp2_server_session.h"

#include <fmt/format.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <ranges>

#include "grpcxx/h2/content_type.h"
namespace grpcxx {
namespace {
Status NativeError(int code) noexcept {
  if (code == NGHTTP2_ERR_NOMEM || code == NGHTTP2_ERR_BUFFER_ERROR ||
      code == NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE)
    return Status{StatusCode::kResourceExhausted};
  if (code == NGHTTP2_ERR_SESSION_CLOSING) return Status{StatusCode::kUnavailable};
  if (code == NGHTTP2_ERR_INVALID_ARGUMENT) return Status{StatusCode::kInvalidArgument};
  if (code == NGHTTP2_ERR_INVALID_STATE) return Status{StatusCode::kFailedPrecondition};
  return Status{StatusCode::kInternal};
}
nghttp2_nv Header(std::string_view name, std::string_view value) noexcept {
  return {reinterpret_cast<std::uint8_t*>(const_cast<char*>(name.data())),
          reinterpret_cast<std::uint8_t*>(const_cast<char*>(value.data())), name.size(),
          value.size(), NGHTTP2_NV_FLAG_NONE};
}
}  // namespace
/** Slots and buffers are allocated at construction. nghttp2 retains its own allocation policy. */
struct Nghttp2ServerSession::Impl {
  enum class StreamState : std::uint8_t { kFree, kReceiving, kDispatched, kSending, kClosed };
  struct Stream {
    std::int32_t id_{};
    std::uint64_t generation_{};
    StreamState state_{StreamState::kFree};
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> input_{};
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> output_{};
    std::size_t input_size_{}, output_size_{}, output_offset_{};
    bool input_rejected_{};
    std::array<char, 512> path_{};
    std::size_t path_size_{};
    std::array<char, 8> status_text_{};
    bool content_type_{}, method_{};
    bool content_type_seen_{};
  };
  nghttp2_session* native_{};
  std::array<Stream, kMaxStreams> streams_{};
  State state_{State::kFailed};
  bool pumping_{};
  std::uint64_t generation_{}, rejected_transitions_{};
  void* handler_context_{};
  RequestHandler handler_{};
  std::array<char, h2_detail::kMaxContentTypeBytes> content_type_{};
  std::size_t content_type_size_{};
  std::string_view ContentType() const noexcept {
    return {content_type_.data(), content_type_size_};
  }
  bool Transition(Stream& stream, StreamState next) noexcept {
    bool accepted{};
    switch (stream.state_) {
      case StreamState::kFree:
        accepted = next == StreamState::kReceiving;
        break;
      case StreamState::kReceiving:
        accepted = next == StreamState::kDispatched || next == StreamState::kClosed;
        break;
      case StreamState::kDispatched:
        accepted = next == StreamState::kSending || next == StreamState::kClosed;
        break;
      case StreamState::kSending:
        accepted = next == StreamState::kClosed;
        break;
      case StreamState::kClosed:
        accepted = next == StreamState::kReceiving;
        break;
    }
    if (!accepted) {
      ++rejected_transitions_;
      return false;
    }
    stream.state_ = next;
    return true;
  }
  Stream* Find(std::int32_t id) noexcept {
    auto found = std::ranges::find_if(streams_, [id](const auto& s) {
      return s.id_ == id && s.state_ != StreamState::kFree && s.state_ != StreamState::kClosed;
    });
    return found == streams_.end() ? nullptr : &*found;
  }
  Stream* Allocate(std::int32_t id) noexcept {
    auto free = std::ranges::find_if(streams_, [](const auto& s) {
      return s.state_ == StreamState::kFree || s.state_ == StreamState::kClosed;
    });
    if (free == streams_.end()) {
      ++rejected_transitions_;
      return nullptr;
    }
    free->id_ = id;
    free->generation_ = ++generation_;
    if (!Transition(*free, StreamState::kReceiving)) return nullptr;
    free->input_rejected_ = false;
    free->input_size_ = free->output_size_ = free->output_offset_ = free->path_size_ = 0;
    free->content_type_ = free->method_ = false;
    free->content_type_seen_ = false;
    return &*free;
  }
  static int InvalidFrame(nghttp2_session*, const nghttp2_frame*, int, void* context) noexcept {
    ++static_cast<Impl*>(context)->rejected_transitions_;
    return 0;
  }
  static int Begin(nghttp2_session*, const nghttp2_frame* frame, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
      if (self.state_ != State::kOpen || self.Allocate(frame->hd.stream_id) == nullptr) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
    }
    return 0;
  }
  static int HeaderReceived(nghttp2_session*, const nghttp2_frame* frame, const std::uint8_t* name,
                            std::size_t name_len, const std::uint8_t* value, std::size_t value_len,
                            std::uint8_t, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    auto* stream = self.Find(frame->hd.stream_id);
    if (stream == nullptr) return 0;
    const std::string_view key{reinterpret_cast<const char*>(name), name_len};
    const std::string_view text{reinterpret_cast<const char*>(value), value_len};
    if (key == ":path") {
      if (stream->path_size_ != 0 || text.size() > stream->path_.size() || text.empty()) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      std::ranges::copy(text, stream->path_.data());
      stream->path_size_ = text.size();
    } else if (key == ":method")
      stream->method_ = text == "POST";
    else if (key == "content-type") {
      if (stream->content_type_seen_) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      stream->content_type_seen_ = true;
      stream->content_type_ = h2_detail::AcceptsContentType(self.ContentType(), text);
    }
    return 0;
  }
  static int DataReceived(nghttp2_session* native, std::uint8_t, std::int32_t id,
                          const std::uint8_t* bytes, std::size_t size, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    auto* stream = self.Find(id);
    if (stream == nullptr || stream->input_rejected_) return 0;
    if (stream->state_ != StreamState::kReceiving ||
        size > stream->input_.size() - 3 - stream->input_size_) {
      ++self.rejected_transitions_;
      stream->input_rejected_ = true;
      if (nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE, id, NGHTTP2_PROTOCOL_ERROR) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      return 0;
    }
    if (size != 0) std::memcpy(stream->input_.data() + 3 + stream->input_size_, bytes, size);
    stream->input_size_ += size;
    return 0;
  }
  static std::expected<std::span<const std::byte>, int> Payload(Stream& stream) noexcept {
    if (stream.input_size_ < 5 || stream.input_[3] != std::byte{}) return std::unexpected(13);
    std::uint32_t size{};
    for (auto byte : std::span{stream.input_}.subspan(4, 4))
      size = (size << 8) | std::to_integer<std::uint32_t>(byte);
    if (size != stream.input_size_ - 5) return std::unexpected(13);
    return std::span<const std::byte>{stream.input_}.subspan(8, size);
  }
  static int FrameReceived(nghttp2_session*, const nghttp2_frame* frame, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    switch (frame->hd.type) {
      case NGHTTP2_GOAWAY:
        self.state_ = State::kDraining;

        break;
      case NGHTTP2_HEADERS:
      case NGHTTP2_DATA:
        if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
          if (auto* stream = self.Find(frame->hd.stream_id);
              stream != nullptr && !stream->input_rejected_)
            self.Deliver(*stream);
        }
        break;
      case NGHTTP2_RST_STREAM:
        break;
      case NGHTTP2_SETTINGS:
        break;
      case NGHTTP2_PING:
        break;
      case NGHTTP2_WINDOW_UPDATE:
        break;
      case NGHTTP2_PRIORITY:
        break;
      case NGHTTP2_PUSH_PROMISE:
        ++self.rejected_transitions_;
        break;
      case NGHTTP2_CONTINUATION:
        break;
      case NGHTTP2_ALTSVC:
        break;
      case NGHTTP2_ORIGIN:
        break;
      case NGHTTP2_PRIORITY_UPDATE:
        break;
    }
    return 0;
  }
  static int Closed(nghttp2_session*, std::int32_t id, std::uint32_t error,
                    void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    if (auto* stream = self.Find(id); stream != nullptr) {
      (void)error;
      if (!self.Transition(*stream, StreamState::kClosed)) return 0;
    }
    return 0;
  }
  static nghttp2_ssize Read(nghttp2_session* session, std::int32_t id, std::uint8_t* buffer,
                            std::size_t length, std::uint32_t* flags, nghttp2_data_source* source,
                            void*) noexcept {
    auto& stream = *static_cast<Stream*>(source->ptr);
    const auto count = std::min(length, stream.output_size_ - stream.output_offset_);
    std::memcpy(buffer, stream.output_.data() + 3 + stream.output_offset_, count);
    stream.output_offset_ += count;
    if (stream.output_offset_ == stream.output_size_) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      const auto trailer = Header("grpc-status", "0");
      if (nghttp2_submit_trailer(session, id, &trailer, 1) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return static_cast<nghttp2_ssize>(count);
  }
  static void SetOutput(Stream& stream, std::span<const std::byte> payload) noexcept {
    stream.output_[3] = std::byte{};
    const auto length = static_cast<std::uint32_t>(payload.size());
    for (std::size_t index = 0; index != 4; ++index)
      stream.output_[index + 4] = std::byte((length >> (24 - 8 * index)) & 255);
    std::ranges::copy(payload, stream.output_.data() + 8);
    stream.output_size_ = payload.size() + 5;
    stream.output_offset_ = 0;
  }
  void Deliver(Stream& stream) noexcept {
    if (stream.state_ != StreamState::kReceiving) {
      ++rejected_transitions_;
      return;
    }
    if (!Transition(stream, StreamState::kDispatched)) return;
    auto payload = Payload(stream);
    if (!payload || !stream.method_ || !stream.content_type_ || stream.path_size_ == 0) {
      ++rejected_transitions_;
      const auto result = Complete(stream, {}, 13);
      if (!result) state_ = State::kFailed;
      return;
    }
    if (handler_ != nullptr)
      handler_(handler_context_, stream.id_, stream.generation_,
               {stream.path_.data(), stream.path_size_}, *payload);
    else if (auto result = Complete(stream, {}, 12); !result)
      state_ = State::kFailed;
  }
  std::expected<void, int> Complete(Stream& stream, std::span<const std::byte> payload,
                                    int grpc_status, std::string_view content_type = {}) noexcept {
    if (stream.state_ != StreamState::kDispatched) {
      ++rejected_transitions_;
      return std::unexpected(NGHTTP2_ERR_INVALID_STATE);
    }
    if (grpc_status < 0 || grpc_status > 16) return std::unexpected(NGHTTP2_ERR_INVALID_ARGUMENT);
    if (content_type.empty()) content_type = ContentType();
    if (!h2_detail::ValidContentType(content_type))
      return std::unexpected(NGHTTP2_ERR_INVALID_ARGUMENT);
    if (payload.size() > kMaxMessageBytes) return std::unexpected(NGHTTP2_ERR_BUFFER_ERROR);
    const auto formatted =
        fmt::format_to_n(stream.status_text_.data(), stream.status_text_.size(), "{}", grpc_status);
    const std::string_view status{stream.status_text_.data(), formatted.size};
    std::array headers{Header(":status", "200"), Header("content-type", content_type),
                       Header("grpc-status", status)};
    int result{};
    if (grpc_status != 0)
      result = nghttp2_submit_headers(native_, NGHTTP2_FLAG_END_STREAM, stream.id_, nullptr,
                                      headers.data(), headers.size(), nullptr);
    else {
      SetOutput(stream, payload);
      nghttp2_data_provider2 provider{{.ptr = &stream}, &Read};
      result = nghttp2_submit_response2(native_, stream.id_, headers.data(), 2, &provider);
    }
    if (result < 0) return std::unexpected(result);
    if (!Transition(stream, StreamState::kSending))
      return std::unexpected(NGHTTP2_ERR_INVALID_STATE);
    return {};
  }
  void FailPending() noexcept {
    for (auto& stream : streams_) {
      if (stream.state_ == StreamState::kFree || stream.state_ == StreamState::kClosed) continue;
      if (!Transition(stream, StreamState::kClosed)) state_ = State::kFailed;
    }
  }
  explicit Impl(std::string_view content_type) noexcept {
    if (!h2_detail::ValidContentType(content_type)) return;
    std::ranges::copy(content_type, content_type_.data());
    content_type_size_ = content_type.size();
    nghttp2_session_callbacks* callbacks{};
    if (nghttp2_session_callbacks_new(&callbacks) != 0) return;
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, &InvalidFrame);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, &Begin);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, &HeaderReceived);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, &DataReceived);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, &FrameReceived);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, &Closed);
    const auto result = nghttp2_session_server_new(&native_, callbacks, this);
    nghttp2_session_callbacks_del(callbacks);
    if (result != 0) return;
    nghttp2_settings_entry settings{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, kMaxStreams};
    if (nghttp2_submit_settings(native_, NGHTTP2_FLAG_NONE, &settings, 1) != 0) return;
    state_ = State::kOpen;
  }
  ~Impl() {
    if (native_ != nullptr) nghttp2_session_del(native_);
  }
};
Nghttp2ServerSession::Nghttp2ServerSession(std::string_view content_type) noexcept
    : impl_(new(std::nothrow) Impl(content_type)) {}
Nghttp2ServerSession::~Nghttp2ServerSession() = default;
bool Nghttp2ServerSession::Ready() const noexcept { return impl_ && impl_->state_ == State::kOpen; }
Nghttp2ServerSession::State Nghttp2ServerSession::GetState() const noexcept {
  return impl_ ? impl_->state_ : State::kFailed;
}
std::size_t Nghttp2ServerSession::ActiveStreams() const noexcept {
  if (!impl_) return 0;
  return static_cast<std::size_t>(std::ranges::count_if(impl_->streams_, [](const auto& stream) {
    return stream.state_ != Impl::StreamState::kFree && stream.state_ != Impl::StreamState::kClosed;
  }));
}
std::uint64_t Nghttp2ServerSession::RejectedTransitions() const noexcept {
  return impl_ ? impl_->rejected_transitions_ : 0;
}
std::expected<std::size_t, Status> Nghttp2ServerSession::Receive(
    std::span<const std::byte> bytes) noexcept {
  if (!impl_ || !impl_->native_ || impl_->state_ == State::kFailed ||
      impl_->state_ == State::kClosed)
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  if (impl_->pumping_) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  impl_->pumping_ = true;
  const auto result = nghttp2_session_mem_recv2(
      impl_->native_, reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
  if (result < 0) {
    impl_->state_ = State::kFailed;
    impl_->FailPending();
    impl_->pumping_ = false;
    return std::unexpected(NativeError(static_cast<int>(result)));
  }
  impl_->pumping_ = false;
  return static_cast<std::size_t>(result);
}
std::expected<std::span<const std::byte>, Status> Nghttp2ServerSession::Send() noexcept {
  if (impl_ && impl_->state_ == State::kClosed) return std::span<const std::byte>{};
  if (!impl_ || !impl_->native_ || impl_->state_ == State::kFailed ||
      impl_->state_ == State::kClosed)
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  if (impl_->pumping_) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  impl_->pumping_ = true;
  const std::uint8_t* bytes{};
  const auto count = nghttp2_session_mem_send2(impl_->native_, &bytes);
  if (count < 0) {
    impl_->state_ = State::kFailed;
    impl_->FailPending();
    impl_->pumping_ = false;
    return std::unexpected(NativeError(static_cast<int>(count)));
  }
  impl_->pumping_ = false;
  if (count == 0 && nghttp2_session_want_read(impl_->native_) == 0 &&
      nghttp2_session_want_write(impl_->native_) == 0)
    impl_->state_ = State::kClosed;
  return std::span<const std::byte>{reinterpret_cast<const std::byte*>(bytes),
                                    static_cast<std::size_t>(count)};
}
std::expected<void, Status> Nghttp2ServerSession::Ping() noexcept {
  if (!Ready()) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_ping(impl_->native_, NGHTTP2_FLAG_NONE, nullptr);
  if (result != 0) return std::unexpected(NativeError(result));
  return {};
}
std::expected<void, Status> Nghttp2ServerSession::GoAway() noexcept {
  if (!Ready()) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_goaway(impl_->native_, NGHTTP2_FLAG_NONE,
                                            nghttp2_session_get_last_proc_stream_id(impl_->native_),
                                            NGHTTP2_NO_ERROR, nullptr, 0);
  if (result != 0) return std::unexpected(NativeError(result));
  impl_->state_ = State::kDraining;
  return {};
}
void Nghttp2ServerSession::SetRequestHandler(void* context, RequestHandler handler) noexcept {
  if (impl_) {
    impl_->handler_context_ = context;
    impl_->handler_ = handler;
  }
}
bool Nghttp2ServerSession::IsCurrent(std::int32_t id, std::uint64_t generation) const noexcept {
  if (!impl_ || impl_->state_ == State::kFailed || impl_->state_ == State::kClosed) return false;
  auto* stream = impl_->Find(id);
  return stream && stream->generation_ == generation &&
         stream->state_ == Impl::StreamState::kDispatched;
}
std::expected<void, Status> Nghttp2ServerSession::Complete(std::int32_t id,
                                                           std::uint64_t generation,
                                                           std::span<const std::byte> payload,
                                                           int grpc_status,
                                                           std::string_view content_type) noexcept {
  if (!impl_ || impl_->state_ == State::kFailed || impl_->state_ == State::kClosed)
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  auto* stream = impl_->Find(id);
  if (!stream || stream->generation_ != generation) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  auto result = impl_->Complete(*stream, payload, grpc_status, content_type);
  if (!result) return std::unexpected(NativeError(result.error()));
  return {};
}
}  // namespace grpcxx
