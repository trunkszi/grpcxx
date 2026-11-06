#include "grpcxx/h2/nghttp2_client_session.h"

#include <fmt/format.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
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

enum class H2ClientStreamState : std::uint8_t {
  kFree = 0,
  kReceiving = 1,
  kClosed = 2,
};

struct H2ClientStreamMetaSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Field{0, 2}]] H2ClientStreamState state{};
  [[= uarch::bits::Flag{2}]] bool input_rejected{};
  [[= uarch::bits::Flag{3}]] bool status_seen{};
  [[= uarch::bits::Flag{4}]] bool content_type_ok{};
  [[= uarch::bits::Flag{5}]] bool completed{};
  [[= uarch::bits::Flag{6}]] bool content_type_seen{};
};

using H2ClientStreamMetaWord = uarch::BitWord<H2ClientStreamMetaSpec>;

}  // namespace

struct Nghttp2ClientSession::Impl {
  using StreamState = H2ClientStreamState;

  struct alignas(16) H2ClientStreamHot final {
    void* context_{nullptr};
    Completion completion_{nullptr};
    std::int32_t id_{0};
    H2ClientStreamMetaWord meta_{};
    std::int32_t grpc_status_{13};
    std::int32_t http_status_{0};
    std::uint32_t input_size_{0};
    std::uint32_t output_size_{0};
    std::uint32_t output_offset_{0};
    std::uint32_t expected_type_size_{0};
  };
  static_assert(sizeof(H2ClientStreamHot) == 48);
  static_assert(uarch::NoPadding<H2ClientStreamHot>);

  struct H2ClientStreamCold final {
    H2ClientStreamCold() noexcept {}
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> input_;
    alignas(8) std::array<std::byte, kMaxMessageBytes + 8> output_;
    std::array<char, h2_detail::kMaxContentTypeBytes> expected_type_;
  };

  static constexpr std::uint64_t kAllStreamBits = (1ULL << kMaxStreams) - 1ULL;

  nghttp2_session* native_{};
  std::array<H2ClientStreamHot, kMaxStreams> hot_{};
  uarch::Bitmap64 free_streams_{uarch::Bitmap64::from_raw(kAllStreamBits)};
  uarch::Bitmap64 active_streams_{uarch::Bitmap64::empty()};
  State state_{State::kFailed};
  bool pumping_{};
  std::uint64_t rejected_transitions_{};
  std::array<H2ClientStreamCold, kMaxStreams> cold_;
  h2_detail::H2SlabPool mem_pool_;

  [[nodiscard, gnu::always_inline]] static StreamState GetStreamState(
      const H2ClientStreamHot& hot) noexcept {
    return hot.meta_.get<^^H2ClientStreamMetaSpec::state>();
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
    hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::state>(next);
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
    hot.meta_ =
        H2ClientStreamMetaWord{}.with<^^H2ClientStreamMetaSpec::state>(StreamState::kReceiving);
    hot.grpc_status_ = 13;
    hot.http_status_ = 0;
    hot.input_size_ = 0U;
    hot.output_size_ = 0U;
    hot.output_offset_ = 0U;
    hot.expected_type_size_ = 0U;
    return idx;
  }

  static int InvalidFrame(nghttp2_session*, const nghttp2_frame*, int, void* context) noexcept {
    ++static_cast<Impl*>(context)->rejected_transitions_;
    return 0;
  }

  static int HeaderReceived(nghttp2_session*, const nghttp2_frame* frame, const std::uint8_t* name,
                            std::size_t name_len, const std::uint8_t* value, std::size_t value_len,
                            std::uint8_t, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    const auto idx = self.Find(frame->hd.stream_id);
    if (!idx.has_value()) return 0;
    auto& hot = self.hot_[*idx];
    const auto& cold = self.cold_[*idx];
    const std::string_view key{reinterpret_cast<const char*>(name), name_len};
    const std::string_view text{reinterpret_cast<const char*>(value), value_len};
    if (key == ":status") {
      int status{};
      const auto parsed = std::from_chars(text.data(), text.data() + text.size(), status);
      if (uarch::unlikely(text.size() != 3U || parsed.ec != std::errc{} ||
                          parsed.ptr != text.data() + text.size() || status < 100 ||
                          status > 599)) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      hot.http_status_ = status;
    } else if (key == "content-type") {
      if (uarch::unlikely(hot.meta_.test<^^H2ClientStreamMetaSpec::content_type_seen>())) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      hot.meta_ =
          hot.meta_.with<^^H2ClientStreamMetaSpec::content_type_seen>(true)
              .with<^^H2ClientStreamMetaSpec::content_type_ok>(h2_detail::AcceptsContentType(
                  {cold.expected_type_.data(), hot.expected_type_size_}, text));
    } else if (key == "grpc-status") {
      int status{};
      auto result = std::from_chars(text.data(), text.data() + text.size(), status);
      if (uarch::unlikely(hot.meta_.test<^^H2ClientStreamMetaSpec::status_seen>() ||
                          result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
                          status < 0 || status > 16 ||
                          (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0)) {
        ++self.rejected_transitions_;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
      hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::status_seen>(true);
      hot.grpc_status_ = status;
    }
    return 0;
  }

  static int DataReceived(nghttp2_session* native, std::uint8_t, std::int32_t id,
                          const std::uint8_t* bytes, std::size_t size, void* context) noexcept {
    auto& self = *static_cast<Impl*>(context);
    const auto idx = self.Find(id);
    if (!idx.has_value()) return 0;
    auto& hot = self.hot_[*idx];
    if (hot.meta_.test<^^H2ClientStreamMetaSpec::input_rejected>()) return 0;
    auto& cold = self.cold_[*idx];
    if (uarch::unlikely(hot.meta_.test<^^H2ClientStreamMetaSpec::completed>() ||
                        size > cold.input_.size() - 3U - hot.input_size_)) {
      ++self.rejected_transitions_;
      hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::input_rejected>(true);
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
      const H2ClientStreamHot& hot, const H2ClientStreamCold& cold) noexcept {
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
        for (const std::uint32_t idx : self.active_streams_ | uarch::views::ones) {
          auto& hot = self.hot_[idx];
          if (hot.id_ > frame->goaway.last_stream_id &&
              !hot.meta_.test<^^H2ClientStreamMetaSpec::completed>()) {
            hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::completed>(true);
            if (hot.completion_) hot.completion_(hot.context_, hot.id_, 14, {});
          }
        }
        break;
      case NGHTTP2_HEADERS:
      case NGHTTP2_DATA:
        if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
          if (const auto idx = self.Find(frame->hd.stream_id);
              idx.has_value() &&
              !self.hot_[*idx].meta_.test<^^H2ClientStreamMetaSpec::input_rejected>()) {
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
      auto& hot = self.hot_[*idx];
      if (!hot.meta_.test<^^H2ClientStreamMetaSpec::completed>() && hot.completion_ != nullptr) {
        hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::completed>(true);
        hot.completion_(hot.context_, id, error == NGHTTP2_CANCEL ? 1 : 14, {});
      }
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
      (void)session;
      (void)id;
    }
    return static_cast<nghttp2_ssize>(count);
  }

  static void SetOutput(H2ClientStreamHot& hot, H2ClientStreamCold& cold,
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
    if (uarch::unlikely(hot.meta_.test<^^H2ClientStreamMetaSpec::completed>())) {
      ++rejected_transitions_;
      return;
    }
    hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::completed>(true);
    const auto& cold = cold_[idx];
    auto payload = Payload(hot, cold);
    int status = 13;
    if (hot.meta_.test<^^H2ClientStreamMetaSpec::status_seen>()) {
      status = hot.grpc_status_;
    } else if (hot.http_status_ != 0 && hot.http_status_ != 200) {
      const auto mapped = Status::FromHttpStatus(hot.http_status_).Code();
      status = static_cast<int>(mapped == StatusCode::kOk ? StatusCode::kInternal : mapped);
    }
    if (status == 0 && (!payload || !hot.meta_.test<^^H2ClientStreamMetaSpec::content_type_ok>() ||
                        hot.http_status_ != 200)) {
      ++rejected_transitions_;
      status = 13;
    }
    if (hot.completion_ != nullptr) {
      hot.completion_(hot.context_, hot.id_, status,
                      payload ? *payload : std::span<const std::byte>{});
    }
  }

  void FailPending() noexcept {
    for (const std::uint32_t idx : active_streams_ | uarch::views::ones) {
      auto& hot = hot_[idx];
      auto completion = hot.completion_;
      auto* context = hot.context_;
      const auto id = hot.id_;
      const bool notify =
          !hot.meta_.test<^^H2ClientStreamMetaSpec::completed>() && completion != nullptr;
      hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::completed>(true);
      hot.completion_ = nullptr;
      hot.context_ = nullptr;
      if (!Transition(idx, StreamState::kClosed)) continue;
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
    nghttp2_mem mem = mem_pool_.MakeNghttp2Mem();
    const auto result = nghttp2_session_client_new3(&native_, callbacks, this, nullptr, &mem);
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
  return impl_ ? static_cast<std::size_t>(impl_->active_streams_.count()) : 0U;
}

std::uint64_t Nghttp2ClientSession::RejectedTransitions() const noexcept {
  return impl_ ? impl_->rejected_transitions_ : 0;
}

std::expected<std::size_t, Status> Nghttp2ClientSession::Receive(
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

std::expected<std::span<const std::byte>, Status> Nghttp2ClientSession::Send() noexcept {
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

std::expected<void, Status> Nghttp2ClientSession::Ping() noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_ping(impl_->native_, NGHTTP2_FLAG_NONE, nullptr);
  if (uarch::unlikely(result != 0)) return std::unexpected(NativeError(result));
  return {};
}

std::expected<void, Status> Nghttp2ClientSession::GoAway() noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto result = nghttp2_submit_goaway(impl_->native_, NGHTTP2_FLAG_NONE,
                                            nghttp2_session_get_last_proc_stream_id(impl_->native_),
                                            NGHTTP2_NO_ERROR, nullptr, 0);
  if (uarch::unlikely(result != 0)) return std::unexpected(NativeError(result));
  impl_->state_ = State::kDraining;
  return {};
}

std::expected<std::int32_t, Status> Nghttp2ClientSession::Submit(
    std::string_view path, std::span<const std::byte> payload, void* context, Completion completion,
    std::string_view content_type) noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(Status{StatusCode::kUnavailable});
  if (uarch::unlikely(path.empty() || path.front() != '/' || path.size() > 512U ||
                      completion == nullptr || !h2_detail::ValidContentType(content_type)))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_ARGUMENT));
  if (uarch::unlikely(payload.size() > kMaxMessageBytes))
    return std::unexpected(NativeError(NGHTTP2_ERR_BUFFER_ERROR));
  const auto idx = impl_->Allocate(0);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(NativeError(NGHTTP2_ERR_NOMEM));
  auto& hot = impl_->hot_[*idx];
  auto& cold = impl_->cold_[*idx];
  hot.context_ = context;
  hot.completion_ = completion;
  std::memcpy(cold.expected_type_.data(), content_type.data(), content_type.size());
  hot.expected_type_size_ = static_cast<std::uint32_t>(content_type.size());
  Impl::SetOutput(hot, cold, payload);
  const std::array headers{Header(":method", "POST"),
                           Header(":scheme", "http"),
                           Header(":authority", "localhost"),
                           Header(":path", path),
                           Header("content-type", content_type),
                           Header("te", "trailers")};
  nghttp2_data_provider2 provider{
      {.ptr = reinterpret_cast<void*>(static_cast<std::uintptr_t>(*idx))}, &Impl::Read};
  const auto id = nghttp2_submit_request2(impl_->native_, nullptr, headers.data(), headers.size(),
                                          &provider, nullptr);
  if (uarch::unlikely(id < 0)) {
    (void)impl_->Transition(*idx, Impl::StreamState::kClosed);
    return std::unexpected(NativeError(id));
  }
  hot.id_ = id;
  return id;
}

std::expected<void, Status> Nghttp2ClientSession::Cancel(std::int32_t id) noexcept {
  if (uarch::unlikely(!impl_ || !impl_->native_))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_STATE));
  const auto idx = impl_->Find(id);
  if (uarch::unlikely(!idx.has_value()))
    return std::unexpected(NativeError(NGHTTP2_ERR_INVALID_ARGUMENT));
  auto& hot = impl_->hot_[*idx];
  hot.completion_ = nullptr;
  hot.context_ = nullptr;
  hot.meta_ = hot.meta_.with<^^H2ClientStreamMetaSpec::completed>(true);
  const auto result =
      nghttp2_submit_rst_stream(impl_->native_, NGHTTP2_FLAG_NONE, id, NGHTTP2_CANCEL);
  if (uarch::unlikely(result != 0)) return std::unexpected(NativeError(result));
  return {};
}

}  // namespace grpcxx
