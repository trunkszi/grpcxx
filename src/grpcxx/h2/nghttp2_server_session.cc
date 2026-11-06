#include "grpcxx/h2/nghttp2_server_session.h"

#include <fmt/format.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <new>
#include <optional>
#include <ranges>
#include <uarch/uarch.hpp>

#include "grpcxx/h2/content_type.h"
#include "grpcxx/h2/nghttp2_mem_pool.h"

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status NativeError(int code) noexcept {
  uarch::cold_path();
  if (code == NGHTTP2_ERR_NOMEM || code == NGHTTP2_ERR_BUFFER_ERROR ||
      code == NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE)
    return Status{StatusCode::kResourceExhausted};
  if (code == NGHTTP2_ERR_SESSION_CLOSING) return Status{StatusCode::kUnavailable};
  if (code == NGHTTP2_ERR_INVALID_ARGUMENT) return Status{StatusCode::kInvalidArgument};
  if (code == NGHTTP2_ERR_INVALID_STATE) return Status{StatusCode::kFailedPrecondition};
  return Status{StatusCode::kInternal};
}

[[nodiscard, gnu::always_inline]] inline nghttp2_nv Header(std::string_view name,
                                                           std::string_view value) noexcept {
  return {reinterpret_cast<std::uint8_t*>(const_cast<char*>(name.data())),
          reinterpret_cast<std::uint8_t*>(const_cast<char*>(value.data())), name.size(),
          value.size(), NGHTTP2_NV_FLAG_NONE};
}

enum class H2ServerStreamState : std::uint8_t {
  kFree = 0,
  kReceiving = 1,
  kDispatched = 2,
  kSending = 3,
  kClosed = 4,
};

struct H2ServerStreamMetaSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Field{0, 3}]] H2ServerStreamState state{};
  [[= uarch::bits::Flag{3}]] bool input_rejected{};
  [[= uarch::bits::Flag{4}]] bool content_type_ok{};
  [[= uarch::bits::Flag{5}]] bool method_post{};
  [[= uarch::bits::Flag{6}]] bool content_type_seen{};
};

using H2ServerStreamMetaWord = uarch::BitWord<H2ServerStreamMetaSpec>;

}  // namespace

struct Nghttp2ServerSession::Impl {
  using StreamState = H2ServerStreamState;

  struct alignas(32) H2ServerStreamHot final {
    std::uint64_t generation_{0};
    std::int32_t id_{0};
    H2ServerStreamMetaWord meta_{};
    std::uint32_t input_size_{0};
    std::uint32_t output_size_{0};
    std::uint32_t output_offset_{0};
    std::uint32_t path_size_{0};
  };
  static_assert(sizeof(H2ServerStreamHot) == 32);
  static_assert(uarch::NoPadding<H2ServerStreamHot>);

  struct H2ServerStreamCold final {
    H2ServerStreamCold() noexcept {}
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> input_;
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> output_;
    std::array<char, 512> path_;
    std::array<char, 8> status_text_;
  };

  static constexpr auto kReadyToDeliverPattern =
      uarch::BitPattern<H2ServerStreamMetaSpec>{}
          .field_eq<^^H2ServerStreamMetaSpec::state>(StreamState::kDispatched)
          .flag_set<^^H2ServerStreamMetaSpec::method_post>()
          .flag_set<^^H2ServerStreamMetaSpec::content_type_ok>()
          .flag_clear<^^H2ServerStreamMetaSpec::input_rejected>();

  static constexpr auto kDispatchedPattern =
      uarch::BitPattern<H2ServerStreamMetaSpec>{}.field_eq<^^H2ServerStreamMetaSpec::state>(
          StreamState::kDispatched);

  static constexpr auto kReceivingNotRejectedPattern =
      uarch::BitPattern<H2ServerStreamMetaSpec>{}
          .field_eq<^^H2ServerStreamMetaSpec::state>(StreamState::kReceiving)
          .flag_clear<^^H2ServerStreamMetaSpec::input_rejected>();

  static constexpr std::uint64_t kAllStreamBits = (1ULL << kMaxStreams) - 1ULL;

  nghttp2_session* native_{};
  std::array<H2ServerStreamHot, kMaxStreams> hot_{};
  uarch::Bitmap64 free_streams_{uarch::Bitmap64::from_raw(kAllStreamBits)};
  uarch::Bitmap64 active_streams_{uarch::Bitmap64::empty()};
  State state_{State::kFailed};
  bool pumping_{};
  std::uint64_t generation_{}, rejected_transitions_{};
  void* handler_context_{};
  RequestHandler handler_{};
  std::array<char, h2_detail::kMaxContentTypeBytes> content_type_{};
  std::size_t content_type_size_{};
  std::array<H2ServerStreamCold, kMaxStreams> cold_;
  h2_detail::H2SlabPool mem_pool_;

  [[nodiscard]] std::string_view ContentType() const noexcept {
    return {content_type_.data(), content_type_size_};
  }

  [[nodiscard, gnu::always_inline]] static StreamState GetStreamState(
      const H2ServerStreamHot& hot) noexcept {
    return hot.meta_.get<^^H2ServerStreamMetaSpec::state>();
  }

  bool Transition(std::size_t index, StreamState next) noexcept {
    auto& hot = hot_[index];
    const StreamState current = GetStreamState(hot);
    bool accepted{};
    switch (current) {
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
    if (uarch::unlikely(!accepted)) {
      ++rejected_transitions_;
      return false;
    }
    hot.meta_ = hot.meta_.with<^^H2ServerStreamMetaSpec::state>(next);
    const auto bit = static_cast<std::uint32_t>(index);
    if (next == StreamState::kReceiving) {
      free_streams_ = free_streams_.clear(bit);
      active_streams_ = active_streams_.set(bit);
    } else if (next == StreamState::kClosed) {
      active_streams_ = active_streams_.clear(bit);
      free_streams_ = free_streams_.set(bit);
    }
    return true;
  }

  [[nodiscard]] std::optional<std::size_t> Find(std::int32_t id) const noexcept {
    for (const std::uint32_t idx : active_streams_ | uarch::views::ones) {
      if (hot_[idx].id_ == id) {
        return static_cast<std::size_t>(idx);
      }
    }
    return std::nullopt;
  }

  [[nodiscard]] std::optional<std::size_t> Allocate(std::int32_t id) noexcept {
    const auto free_bit = free_streams_.lowest_one();
    if (uarch::unlikely(!free_bit.has_value())) {
      ++rejected_transitions_;
      return std::nullopt;
    }
    const std::size_t idx = *free_bit;
    if (uarch::unlikely(!Transition(idx, StreamState::kReceiving))) {
      return std::nullopt;
    }
    auto& hot = hot_[idx];
    hot.id_ = id;
    hot.generation_ = ++generation_;
    hot.meta_ =
        H2ServerStreamMetaWord{}.with<^^H2ServerStreamMetaSpec::state>(StreamState::kReceiving);
    hot.input_size_ = 0U;
    hot.output_size_ = 0U;
    hot.output_offset_ = 0U;
    hot.path_size_ = 0U;
    return idx;
  }

  static int InvalidFrame(nghttp2_session*, const nghttp2_frame*, int, void* context) noexcept {
    ++static_cast<Impl*>(context)->rejected_transitions_;
    return 0;
  }

  static int Begin(nghttp2_session*, const nghttp2_frame* frame, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
      if (uarch::unlikely(self.state_ != State::kOpen ||
                          !self.Allocate(frame->hd.stream_id).has_value())) {
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
    const auto idx = self.Find(frame->hd.stream_id);
    if (!idx.has_value()) return 0;
    auto& hot = self.hot_[*idx];
    auto& cold = self.cold_[*idx];
    const std::string_view key{reinterpret_cast<const char*>(name), name_len};
    const std::string_view text{reinterpret_cast<const char*>(value), value_len};
    if (key == ":path") {
      if (uarch::unlikely(hot.path_size_ != 0U || text.size() > cold.path_.size() ||
                          text.empty())) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      std::memcpy(cold.path_.data(), text.data(), text.size());
      hot.path_size_ = static_cast<std::uint32_t>(text.size());
    } else if (key == ":method") {
      hot.meta_ = hot.meta_.with<^^H2ServerStreamMetaSpec::method_post>(text == "POST");
    } else if (key == "content-type") {
      if (uarch::unlikely(hot.meta_.test<^^H2ServerStreamMetaSpec::content_type_seen>())) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      hot.meta_ = hot.meta_.with<^^H2ServerStreamMetaSpec::content_type_seen>(true)
                      .with<^^H2ServerStreamMetaSpec::content_type_ok>(
                          h2_detail::AcceptsContentType(self.ContentType(), text));
    }
    return 0;
  }

  static int DataReceived(nghttp2_session* native, std::uint8_t, std::int32_t id,
                          const std::uint8_t* bytes, std::size_t size, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    const auto idx = self.Find(id);
    if (!idx.has_value()) return 0;
    auto& hot = self.hot_[*idx];
    if (hot.meta_.test<^^H2ServerStreamMetaSpec::input_rejected>()) return 0;
    auto& cold = self.cold_[*idx];
    if (uarch::unlikely(!kReceivingNotRejectedPattern.matches(hot.meta_) ||
                        size > cold.input_.size() - 3U - hot.input_size_)) {
      ++self.rejected_transitions_;
      hot.meta_ = hot.meta_.with<^^H2ServerStreamMetaSpec::input_rejected>(true);
      if (nghttp2_submit_rst_stream(native, NGHTTP2_FLAG_NONE, id, NGHTTP2_PROTOCOL_ERROR) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      return 0;
    }
    if (size != 0U) {
      std::memcpy(cold.input_.data() + 3U + hot.input_size_, bytes, size);
    }
    hot.input_size_ += static_cast<std::uint32_t>(size);
    return 0;
  }

  [[nodiscard]] static std::expected<std::span<const std::byte>, int> Payload(
      const H2ServerStreamHot& hot, const H2ServerStreamCold& cold) noexcept {
    if (uarch::unlikely(hot.input_size_ < 5U || cold.input_[3] != std::byte{})) {
      return std::unexpected(13);
    }
    std::uint32_t size{};
    std::memcpy(&size, cold.input_.data() + 4U, sizeof(size));
    if constexpr (std::endian::native == std::endian::little) {
      size = std::byteswap(size);
    }
    if (uarch::unlikely(size != hot.input_size_ - 5U)) {
      return std::unexpected(13);
    }
    return std::span<const std::byte>{cold.input_}.subspan(8U, size);
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
          if (const auto idx = self.Find(frame->hd.stream_id);
              idx.has_value() &&
              !self.hot_[*idx].meta_.test<^^H2ServerStreamMetaSpec::input_rejected>()) {
            self.Deliver(*idx);
          }
        }
        break;
      case NGHTTP2_PUSH_PROMISE:
        ++self.rejected_transitions_;
        break;
      default:
        break;
    }
    return 0;
  }

  static int Closed(nghttp2_session*, std::int32_t id, std::uint32_t error,
                    void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    if (const auto idx = self.Find(id); idx.has_value()) {
      (void)error;
      (void)self.Transition(*idx, StreamState::kClosed);
    }
    return 0;
  }

  static nghttp2_ssize Read(nghttp2_session* session, std::int32_t id, std::uint8_t* buffer,
                            std::size_t length, std::uint32_t* flags, nghttp2_data_source* source,
                            void* user_data) noexcept {
    auto& self = *static_cast<Impl*>(user_data);
    const auto idx = reinterpret_cast<std::uintptr_t>(source->ptr);
    auto& hot = self.hot_[idx];
    const auto& cold = self.cold_[idx];
    const std::size_t remaining = hot.output_size_ - hot.output_offset_;
    const std::size_t count = std::min(length, remaining);
    if (count != 0U) {
      std::memcpy(buffer, cold.output_.data() + 3U + hot.output_offset_, count);
    }
    hot.output_offset_ += static_cast<std::uint32_t>(count);
    if (hot.output_offset_ == hot.output_size_) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      const auto trailer = Header("grpc-status", "0");
      if (nghttp2_submit_trailer(session, id, &trailer, 1) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return static_cast<nghttp2_ssize>(count);
  }

  static void SetOutput(H2ServerStreamHot& hot, H2ServerStreamCold& cold,
                        std::span<const std::byte> payload) noexcept {
    cold.output_[3] = std::byte{};
    auto be_length = static_cast<std::uint32_t>(payload.size());
    if constexpr (std::endian::native == std::endian::little) {
      be_length = std::byteswap(be_length);
    }
    std::memcpy(cold.output_.data() + 4U, &be_length, sizeof(be_length));
    if (!payload.empty()) {
      std::memcpy(cold.output_.data() + 8U, payload.data(), payload.size());
    }
    hot.output_size_ = static_cast<std::uint32_t>(payload.size() + 5U);
    hot.output_offset_ = 0U;
  }

  void Deliver(std::size_t idx) noexcept {
    auto& hot = hot_[idx];
    if (uarch::unlikely(GetStreamState(hot) != StreamState::kReceiving)) {
      ++rejected_transitions_;
      return;
    }
    if (uarch::unlikely(!Transition(idx, StreamState::kDispatched))) return;
    auto& cold = cold_[idx];
    auto payload = Payload(hot, cold);
    if (uarch::unlikely(!payload || !kReadyToDeliverPattern.matches(hot.meta_) ||
                        hot.path_size_ == 0U)) {
      ++rejected_transitions_;
      const auto result = Complete(idx, {}, 13);
      if (!result) state_ = State::kFailed;
      return;
    }
    if (handler_ != nullptr) {
      handler_(handler_context_, hot.id_, hot.generation_, {cold.path_.data(), hot.path_size_},
               *payload);
    } else if (auto result = Complete(idx, {}, 12); !result) {
      state_ = State::kFailed;
    }
  }

  std::expected<void, int> Complete(std::size_t idx, std::span<const std::byte> payload,
                                    int grpc_status, std::string_view content_type = {}) noexcept {
    auto& hot = hot_[idx];
    if (uarch::unlikely(!kDispatchedPattern.matches(hot.meta_))) {
      ++rejected_transitions_;
      return std::unexpected(NGHTTP2_ERR_INVALID_STATE);
    }
    if (uarch::unlikely(grpc_status < 0 || grpc_status > 16)) {
      return std::unexpected(NGHTTP2_ERR_INVALID_ARGUMENT);
    }
    if (content_type.empty()) content_type = ContentType();
    if (uarch::unlikely(!h2_detail::ValidContentType(content_type))) {
      return std::unexpected(NGHTTP2_ERR_INVALID_ARGUMENT);
    }
    if (uarch::unlikely(payload.size() > kMaxMessageBytes)) {
      return std::unexpected(NGHTTP2_ERR_BUFFER_ERROR);
    }
    auto& cold = cold_[idx];
    const auto formatted =
        fmt::format_to_n(cold.status_text_.data(), cold.status_text_.size(), "{}", grpc_status);
    const std::string_view status{cold.status_text_.data(), formatted.size};
    std::array headers{Header(":status", "200"), Header("content-type", content_type),
                       Header("grpc-status", status)};
    int result{};
    if (grpc_status != 0) {
      result = nghttp2_submit_headers(native_, NGHTTP2_FLAG_END_STREAM, hot.id_, nullptr,
                                      headers.data(), headers.size(), nullptr);
    } else {
      SetOutput(hot, cold, payload);
      nghttp2_data_provider2 provider{
          {.ptr = reinterpret_cast<void*>(static_cast<std::uintptr_t>(idx))}, &Read};
      result = nghttp2_submit_response2(native_, hot.id_, headers.data(), 2, &provider);
    }
    if (uarch::unlikely(result < 0)) return std::unexpected(result);
    if (uarch::unlikely(!Transition(idx, StreamState::kSending))) {
      return std::unexpected(NGHTTP2_ERR_INVALID_STATE);
    }
    return {};
  }

  void FailPending() noexcept {
    for (const std::uint32_t idx : active_streams_ | uarch::views::ones) {
      if (!Transition(idx, StreamState::kClosed)) {
        state_ = State::kFailed;
      }
    }
  }

  explicit Impl(std::string_view content_type) noexcept {
    if (!h2_detail::ValidContentType(content_type)) return;
    std::memcpy(content_type_.data(), content_type.data(), content_type.size());
    content_type_size_ = content_type.size();
    nghttp2_session_callbacks* callbacks{};
    if (nghttp2_session_callbacks_new(&callbacks) != 0) return;
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, &InvalidFrame);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, &Begin);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, &HeaderReceived);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, &DataReceived);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, &FrameReceived);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, &Closed);
    nghttp2_mem mem = mem_pool_.MakeNghttp2Mem();
    const auto result = nghttp2_session_server_new3(&native_, callbacks, this, nullptr, &mem);
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
  return impl_ ? static_cast<std::size_t>(impl_->active_streams_.count()) : 0U;
}

std::uint64_t Nghttp2ServerSession::RejectedTransitions() const noexcept {
  return impl_ ? impl_->rejected_transitions_ : 0;
}

std::expected<std::size_t, Status> Nghttp2ServerSession::Receive(
    std::span<const std::byte> bytes) noexcept {
  if (uarch::unlikely(!impl_ || !impl_->native_ || impl_->state_ == State::kFailed ||
                      impl_->state_ == State::kClosed))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  if (uarch::unlikely(impl_->pumping_)) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  impl_->pumping_ = true;
  const auto result = nghttp2_session_mem_recv2(
      impl_->native_, reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
  if (uarch::unlikely(result < 0)) {
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
  if (uarch::unlikely(!impl_ || !impl_->native_ || impl_->state_ == State::kFailed ||
                      impl_->state_ == State::kClosed))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  if (uarch::unlikely(impl_->pumping_)) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  impl_->pumping_ = true;
  const std::uint8_t* bytes{};
  const auto count = nghttp2_session_mem_send2(impl_->native_, &bytes);
  if (uarch::unlikely(count < 0)) {
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
  if (uarch::unlikely(!Ready())) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_ping(impl_->native_, NGHTTP2_FLAG_NONE, nullptr);
  if (uarch::unlikely(result != 0)) return std::unexpected(NativeError(result));
  return {};
}

std::expected<void, Status> Nghttp2ServerSession::GoAway() noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_goaway(impl_->native_, NGHTTP2_FLAG_NONE,
                                            nghttp2_session_get_last_proc_stream_id(impl_->native_),
                                            NGHTTP2_NO_ERROR, nullptr, 0);
  if (uarch::unlikely(result != 0)) return std::unexpected(NativeError(result));
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
  if (uarch::unlikely(!impl_ || impl_->state_ == State::kFailed || impl_->state_ == State::kClosed))
    return false;
  const auto idx = impl_->Find(id);
  if (!idx.has_value()) return false;
  const auto& hot = impl_->hot_[*idx];
  return hot.generation_ == generation && Impl::kDispatchedPattern.matches(hot.meta_);
}

std::expected<void, Status> Nghttp2ServerSession::Complete(std::int32_t id,
                                                           std::uint64_t generation,
                                                           std::span<const std::byte> payload,
                                                           int grpc_status,
                                                           std::string_view content_type) noexcept {
  if (uarch::unlikely(!impl_ || impl_->state_ == State::kFailed || impl_->state_ == State::kClosed))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto idx = impl_->Find(id);
  if (uarch::unlikely(!idx.has_value() || impl_->hot_[*idx].generation_ != generation)) {
    ++impl_->rejected_transitions_;
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  }
  auto result = impl_->Complete(*idx, payload, grpc_status, content_type);
  if (uarch::unlikely(!result)) return std::unexpected(NativeError(result.error()));
  return {};
}

}  // namespace grpcxx
