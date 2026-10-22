#include "grpcxx/h2/nghttp2_client_session.h"

#include <fmt/format.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <charconv>
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
struct Nghttp2ClientSession::Impl {
  enum class StreamState : std::uint8_t { kFree, kReceiving, kClosed };
  struct Stream {
    std::int32_t id_{};
    StreamState state_{StreamState::kFree};
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> input_{};
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> output_{};
    std::size_t input_size_{}, output_size_{}, output_offset_{};
    bool input_rejected_{};
    int grpc_status_{13}, http_status_{};
    bool status_seen_{}, content_type_{}, completed_{};
    bool content_type_seen_{};
    std::array<char, h2_detail::kMaxContentTypeBytes> expected_type_{};
    std::size_t expected_type_size_{};
    void* context_{};
    Completion completion_{};
  };
  nghttp2_session* native_{};
  std::array<Stream, kMaxStreams> streams_{};
  State state_{State::kFailed};
  bool pumping_{};
  std::uint64_t rejected_transitions_{};

  bool Transition(Stream& stream, StreamState next) noexcept {
    bool accepted{};
    switch (stream.state_) {
      case StreamState::kFree:
        accepted = next == StreamState::kReceiving;
        break;
      case StreamState::kReceiving:
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
    if (!Transition(*free, StreamState::kReceiving)) return nullptr;
    free->input_rejected_ = false;
    free->input_size_ = free->output_size_ = free->output_offset_ = 0;
    free->grpc_status_ = 13;
    free->http_status_ = 0;
    free->status_seen_ = free->content_type_ = free->completed_ = false;
    free->content_type_seen_ = false;
    return &*free;
  }
  static int InvalidFrame(nghttp2_session*, const nghttp2_frame*, int, void* context) noexcept {
    ++static_cast<Impl*>(context)->rejected_transitions_;
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
    if (key == ":status") {
      int status{};
      const auto parsed = std::from_chars(text.data(), text.data() + text.size(), status);
      if (text.size() != 3 || parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
          status < 100 || status > 599) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      stream->http_status_ = status;
    } else if (key == "content-type") {
      if (stream->content_type_seen_) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      stream->content_type_seen_ = true;
      stream->content_type_ = h2_detail::AcceptsContentType(
          {stream->expected_type_.data(), stream->expected_type_size_}, text);
    } else if (key == "grpc-status") {
      int status{};
      auto result = std::from_chars(text.data(), text.data() + text.size(), status);
      if (stream->status_seen_ || result.ec != std::errc{} ||
          result.ptr != text.data() + text.size() || status < 0 || status > 16 ||
          (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      stream->status_seen_ = true;
      stream->grpc_status_ = status;
    }
    return 0;
  }
  static int DataReceived(nghttp2_session* native, std::uint8_t, std::int32_t id,
                          const std::uint8_t* bytes, std::size_t size, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    auto* stream = self.Find(id);
    if (stream == nullptr || stream->input_rejected_) return 0;
    if (stream->completed_ || size > stream->input_.size() - 3 - stream->input_size_) {
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
        for (auto& stream : self.streams_) {
          if (stream.id_ > frame->goaway.last_stream_id && !stream.completed_ &&
              stream.state_ != StreamState::kClosed && stream.state_ != StreamState::kFree) {
            stream.completed_ = true;
            if (stream.completion_) stream.completion_(stream.context_, stream.id_, 14, {});
          }
        }
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
      if (!stream->completed_ && stream->completion_ != nullptr) {
        stream->completed_ = true;
        stream->completion_(stream->context_, id, error == NGHTTP2_CANCEL ? 1 : 14, {});
      }
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
      (void)session;
      (void)id;
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
    if (stream.completed_) {
      ++rejected_transitions_;
      return;
    }
    stream.completed_ = true;
    auto payload = Payload(stream);
    int status = 13;
    if (stream.status_seen_)
      status = stream.grpc_status_;
    else if (stream.http_status_ != 0 && stream.http_status_ != 200) {
      const auto mapped = Status::FromHttpStatus(stream.http_status_).Code();
      status = static_cast<int>(mapped == StatusCode::kOk ? StatusCode::kInternal : mapped);
    }
    if (status == 0 && (!payload || !stream.content_type_ || stream.http_status_ != 200)) {
      ++rejected_transitions_;
      status = 13;
    }
    if (stream.completion_ != nullptr)
      stream.completion_(stream.context_, stream.id_, status,
                         payload ? *payload : std::span<const std::byte>{});
  }

  void FailPending() noexcept {
    for (auto& stream : streams_) {
      if (stream.state_ == StreamState::kFree || stream.state_ == StreamState::kClosed) continue;
      auto completion = stream.completion_;
      auto* context = stream.context_;
      const auto id = stream.id_;
      const bool notify = !stream.completed_ && completion != nullptr;
      stream.completed_ = true;
      stream.completion_ = nullptr;
      stream.context_ = nullptr;
      if (!Transition(stream, StreamState::kClosed)) continue;
      if (notify) completion(context, id, 14, {});
    }
  }
  Impl() noexcept {
    nghttp2_session_callbacks* callbacks{};
    if (nghttp2_session_callbacks_new(&callbacks) != 0) return;
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, &InvalidFrame);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, &HeaderReceived);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, &DataReceived);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, &FrameReceived);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, &Closed);
    const auto result = nghttp2_session_client_new(&native_, callbacks, this);
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
Nghttp2ClientSession::Nghttp2ClientSession() noexcept : impl_(new(std::nothrow) Impl) {}
Nghttp2ClientSession::~Nghttp2ClientSession() = default;
bool Nghttp2ClientSession::Ready() const noexcept { return impl_ && impl_->state_ == State::kOpen; }
Nghttp2ClientSession::State Nghttp2ClientSession::GetState() const noexcept {
  return impl_ ? impl_->state_ : State::kFailed;
}
std::size_t Nghttp2ClientSession::ActiveStreams() const noexcept {
  if (!impl_) return 0;
  return static_cast<std::size_t>(std::ranges::count_if(impl_->streams_, [](const auto& stream) {
    return stream.state_ != Impl::StreamState::kFree && stream.state_ != Impl::StreamState::kClosed;
  }));
}
std::uint64_t Nghttp2ClientSession::RejectedTransitions() const noexcept {
  return impl_ ? impl_->rejected_transitions_ : 0;
}
std::expected<std::size_t, Status> Nghttp2ClientSession::Receive(
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
std::expected<std::span<const std::byte>, Status> Nghttp2ClientSession::Send() noexcept {
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
std::expected<void, Status> Nghttp2ClientSession::Ping() noexcept {
  if (!Ready()) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_ping(impl_->native_, NGHTTP2_FLAG_NONE, nullptr);
  if (result != 0) return std::unexpected(NativeError(result));
  return {};
}
std::expected<void, Status> Nghttp2ClientSession::GoAway() noexcept {
  if (!Ready()) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_goaway(impl_->native_, NGHTTP2_FLAG_NONE,
                                            nghttp2_session_get_last_proc_stream_id(impl_->native_),
                                            NGHTTP2_NO_ERROR, nullptr, 0);
  if (result != 0) return std::unexpected(NativeError(result));
  impl_->state_ = State::kDraining;
  return {};
}
std::expected<std::int32_t, Status> Nghttp2ClientSession::Submit(
    std::string_view path, std::span<const std::byte> payload, void* context, Completion completion,
    std::string_view content_type) noexcept {
  if (!Ready()) return std::unexpected(Status{StatusCode::kUnavailable});
  if (path.empty() || path.front() != '/' || path.size() > 512 || completion == nullptr ||
      !h2_detail::ValidContentType(content_type))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_ARGUMENT));
  if (payload.size() > kMaxMessageBytes)
    return std::unexpected(NativeError(NGHTTP2_ERR_BUFFER_ERROR));
  auto* stream = impl_->Allocate(0);
  if (stream == nullptr) return std::unexpected(NativeError(NGHTTP2_ERR_NOMEM));
  stream->context_ = context;
  stream->completion_ = completion;
  std::ranges::copy(content_type, stream->expected_type_.data());
  stream->expected_type_size_ = content_type.size();
  Impl::SetOutput(*stream, payload);
  const std::array headers{Header(":method", "POST"),
                           Header(":scheme", "http"),
                           Header(":authority", "localhost"),
                           Header(":path", path),
                           Header("content-type", content_type),
                           Header("te", "trailers")};
  nghttp2_data_provider2 provider{{.ptr = stream}, &Impl::Read};
  const auto id = nghttp2_submit_request2(impl_->native_, nullptr, headers.data(), headers.size(),
                                          &provider, nullptr);
  if (id < 0) {
    stream->state_ = Impl::StreamState::kClosed;
    return std::unexpected(NativeError(id));
  }
  stream->id_ = id;
  return id;
}
std::expected<void, Status> Nghttp2ClientSession::Cancel(std::int32_t id) noexcept {
  if (!impl_ || !impl_->native_) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  auto* stream = impl_->Find(id);
  if (stream == nullptr) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_ARGUMENT));
  stream->completion_ = nullptr;
  stream->context_ = nullptr;
  stream->completed_ = true;
  const auto result =
      nghttp2_submit_rst_stream(impl_->native_, NGHTTP2_FLAG_NONE, id, NGHTTP2_CANCEL);
  if (result != 0) return std::unexpected(NativeError(result));
  return {};
}

}  // namespace grpcxx
