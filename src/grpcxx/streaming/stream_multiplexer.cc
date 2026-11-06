#include "grpcxx/streaming/stream_multiplexer.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status Invalid() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kFailedPrecondition};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status Full() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kResourceExhausted};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status Cancelled() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kCancelled};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status OutOfRangeStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kOutOfRange};
}

struct StreamSlotMetaSpec {
  using Word = std::uint64_t;
  [[= uarch::bits::Flag{0}]] bool used{};
  [[= uarch::bits::Flag{1}]] bool server{};
  [[= uarch::bits::Flag{2}]] bool sent_headers{};
  [[= uarch::bits::Flag{3}]] bool received_headers{};
  [[= uarch::bits::Flag{4}]] bool pressured{};
  [[= uarch::bits::Flag{5}]] bool reset_pending{};
  [[= uarch::bits::Flag{6}]] bool reset_notified{};
  [[= uarch::bits::Flag{7}]] bool close_notified{};
  [[= uarch::bits::Flag{8}]] bool inbound_in_progress{};
  [[= uarch::bits::Flag{9}]] bool end_stream_sent{};
  [[= uarch::bits::Flag{10}]] bool half_closed_remote{};
  [[= uarch::bits::Flag{11}]] bool trailers_queued{};
  [[= uarch::bits::Flag{12}]] bool callback_active{};
  [[= uarch::bits::Flag{13}]] bool deferred_recycle{};
  [[= uarch::bits::Field{16, 4}]] StreamState state{};
  [[= uarch::bits::Field{20, 4}]] RpcKind kind{};
  [[= uarch::bits::Field{24, 8}]] std::uint8_t callbacks_active{};
  [[= uarch::bits::Field{32, 32}]] std::uint32_t generation{};
};

using StreamSlotMetaWord = uarch::BitWord<StreamSlotMetaSpec>;

constexpr auto kReceiveHeadersReadyPattern =
    uarch::BitPattern<StreamSlotMetaSpec>{}
        .flag_set<^^StreamSlotMetaSpec::used>()
        .flag_clear<^^StreamSlotMetaSpec::received_headers>()
        .flag_clear<^^StreamSlotMetaSpec::inbound_in_progress>()
        .flag_clear<^^StreamSlotMetaSpec::callback_active>();

constexpr auto kCancelPatch = uarch::BitPatch<StreamSlotMetaSpec>{}
                                  .set_flag<^^StreamSlotMetaSpec::pressured>(false)
                                  .set_flag<^^StreamSlotMetaSpec::inbound_in_progress>(false)
                                  .set_flag<^^StreamSlotMetaSpec::reset_pending>(true);

}  // namespace

struct StreamMultiplexer::Entry {
  StreamFrameKind kind_{};
  MetadataMap metadata_;
  Status status_;
  std::size_t size_{};
  std::size_t offset_{};
};

struct alignas(64) StreamMultiplexer::StreamSlotHot final {
  StreamSlotMetaWord meta_{};
  std::uint32_t head_{0U};
  std::uint32_t count_{0U};
  std::size_t queued_bytes_{0U};
  std::size_t window_{0U};
  std::size_t inbound_size_{0U};
  std::size_t sent_messages_{0U};
  std::size_t received_messages_{0U};
  std::uint64_t reserved_{0U};

  [[nodiscard, gnu::always_inline]] StreamState StateValue() const noexcept {
    return meta_.get<^^StreamSlotMetaSpec::state>();
  }
  [[nodiscard, gnu::always_inline]] RpcKind KindValue() const noexcept {
    return meta_.get<^^StreamSlotMetaSpec::kind>();
  }
  [[nodiscard, gnu::always_inline]] std::uint32_t GenerationValue() const noexcept {
    return meta_.get<^^StreamSlotMetaSpec::generation>();
  }
  [[gnu::always_inline]] void EnterCallback() noexcept {
    const auto d =
        static_cast<std::uint8_t>(meta_.get<^^StreamSlotMetaSpec::callbacks_active>() + 1U);
    meta_ = meta_.with<^^StreamSlotMetaSpec::callbacks_active>(d)
                .with<^^StreamSlotMetaSpec::callback_active>(true);
  }
  [[gnu::always_inline]] void LeaveCallback() noexcept {
    const auto d =
        static_cast<std::uint8_t>(meta_.get<^^StreamSlotMetaSpec::callbacks_active>() - 1U);
    meta_ = meta_.with<^^StreamSlotMetaSpec::callbacks_active>(d)
                .with<^^StreamSlotMetaSpec::callback_active>(d != 0U);
  }
  static void VerifyLayout() noexcept {
    static_assert(sizeof(StreamSlotHot) == 64 && uarch::NoPadding<StreamSlotHot>);
  }
};

struct StreamMultiplexer::StreamSlotCold final {
  std::unique_ptr<Entry[]> entries_;
  std::unique_ptr<std::byte[]> bytes_;
  std::unique_ptr<std::byte[]> inbound_;
  ReadyHandler on_ready_;
  ReadyHandler on_cancel_;
  HeadersHandler on_headers_;
  MessageHandler on_message_;
  ReadyHandler on_half_close_;
  CloseHandler on_close_;
};

StreamMultiplexer::StreamMultiplexer() noexcept : StreamMultiplexer(Options{}) {}

StreamMultiplexer::StreamMultiplexer(Options options) noexcept : options_(options) {
  std::size_t total_queue_bytes = 0U;
  if (options.max_streams == 0U || options.queue_depth == 0U || options.max_message_bytes == 0U ||
      options.high_water_bytes == 0U || options.low_water_bytes == 0U ||
      options.low_water_bytes >= options.high_water_bytes ||
      __builtin_mul_overflow(static_cast<std::size_t>(options.queue_depth),
                             options.max_message_bytes, &total_queue_bytes) ||
      options.high_water_bytes > total_queue_bytes) {
    return;
  }

  queue_depth_div_ = *uarch::FastDivisorU32::try_new(options.queue_depth);
  bitmap_words_ = (static_cast<std::size_t>(options.max_streams) + 63U) >> 6U;

  free_words_.reset(new (std::nothrow) uarch::Bitmap64[bitmap_words_]);
  active_words_.reset(new (std::nothrow) uarch::Bitmap64[bitmap_words_]);
  hot_.reset(new (std::nothrow) StreamSlotHot[options.max_streams]);
  cold_.reset(new (std::nothrow) StreamSlotCold[options.max_streams]);
  if (!free_words_ || !active_words_ || !hot_ || !cold_) {
    hot_.reset();
    return;
  }

  const std::size_t l0_words = std::min<std::size_t>(bitmap_words_, 64U);
  free_l0_ = uarch::Bitmap64::from_raw(l0_words == 64U ? ~0ULL : ((1ULL << l0_words) - 1ULL));
  active_l0_ = uarch::Bitmap64::empty();
  for (std::size_t w = 0U; w < bitmap_words_; ++w) {
    const std::size_t remaining = static_cast<std::size_t>(options.max_streams) - (w << 6U);
    const std::uint64_t mask = remaining >= 64U ? ~0ULL : ((1ULL << remaining) - 1ULL);
    free_words_[w] = uarch::Bitmap64::from_raw(mask);
    active_words_[w] = uarch::Bitmap64::empty();
  }

  for (auto& cold_slot : std::span{cold_.get(), options.max_streams}) {
    cold_slot.entries_.reset(new (std::nothrow) Entry[options.queue_depth]);
    cold_slot.bytes_.reset(new (std::nothrow) std::byte[total_queue_bytes]);
    cold_slot.inbound_.reset(new (std::nothrow) std::byte[options.max_message_bytes]);
    if (!cold_slot.entries_ || !cold_slot.bytes_ || !cold_slot.inbound_) {
      hot_.reset();
      return;
    }
    for (auto& entry : std::span{cold_slot.entries_.get(), options.queue_depth}) {
      entry.status_ = Status{StatusCode::kUnknown, std::string(256, '\0')};
    }
  }
}

StreamMultiplexer::~StreamMultiplexer() = default;
bool StreamMultiplexer::Ready() const noexcept { return hot_ != nullptr; }
std::uint64_t StreamMultiplexer::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}
std::optional<std::uint32_t> StreamMultiplexer::FindIndex(StreamHandle stream) noexcept {
  if (uarch::unlikely(wire_active_)) return std::nullopt;
  return std::as_const(*this).FindIndex(stream);
}
std::optional<std::uint32_t> StreamMultiplexer::FindIndex(StreamHandle stream) const noexcept {
  if (uarch::unlikely(!hot_ || stream.index >= options_.max_streams)) return std::nullopt;
  const auto& hot = hot_[stream.index];
  if (uarch::unlikely(!hot.meta_.test<^^StreamSlotMetaSpec::used>() ||
                      hot.GenerationValue() != stream.generation)) {
    return std::nullopt;
  }
  return stream.index;
}

std::expected<StreamHandle, Status> StreamMultiplexer::Open(RpcKind kind, bool is_server) noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(Full());
  const auto try_word =
      [&](std::size_t w) noexcept -> std::optional<std::expected<StreamHandle, Status>> {
    while (const auto bit = free_words_[w].lowest_one()) {
      const auto index = static_cast<std::uint32_t>((w << 6U) + *bit);
      auto& hot = hot_[index];
      const std::uint32_t current_gen = hot.GenerationValue();
      if (uarch::unlikely(current_gen == std::numeric_limits<std::uint32_t>::max())) {
        free_words_[w] = free_words_[w].clear(*bit);
        if (w < 64U && free_words_[w].is_empty())
          free_l0_ = free_l0_.clear(static_cast<std::uint32_t>(w));
        continue;
      }
      const std::uint32_t next_gen = current_gen + 1U;
      hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::generation>(next_gen)
                      .with<^^StreamSlotMetaSpec::used>(true)
                      .with<^^StreamSlotMetaSpec::server>(is_server)
                      .with<^^StreamSlotMetaSpec::kind>(kind);
      hot.window_ = options_.initial_window;
      if (auto result = Transition(hot, Event::kOpen, false); uarch::unlikely(!result)) {
        hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::used>(false);
        return std::unexpected(result.error());
      }
      free_words_[w] = free_words_[w].clear(*bit);
      if (w < 64U && free_words_[w].is_empty())
        free_l0_ = free_l0_.clear(static_cast<std::uint32_t>(w));
      active_words_[w] = active_words_[w].set(*bit);
      if (w < 64U) active_l0_ = active_l0_.set(static_cast<std::uint32_t>(w));
      return StreamHandle{index, next_gen};
    }
    return std::nullopt;
  };
  while (const auto w = free_l0_.lowest_one()) {
    if (auto res = try_word(*w)) return *res;
  }
  for (std::size_t w = 64U; w < bitmap_words_; ++w) {
    if (auto res = try_word(w)) return *res;
  }
  return std::unexpected(Full());
}

std::expected<void, Status> StreamMultiplexer::Release(StreamHandle stream) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  const StreamState st = hot.StateValue();
  if (uarch::unlikely((st != StreamState::kClosed && st != StreamState::kReset) ||
                      hot.count_ != 0U || hot.meta_.test<^^StreamSlotMetaSpec::reset_pending>() ||
                      hot.meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  const std::uint32_t gen = hot.GenerationValue();
  hot = StreamSlotHot{.meta_ = StreamSlotMetaWord{}.with<^^StreamSlotMetaSpec::generation>(gen)};

  auto& cold = cold_[*idx];
  cold.on_ready_ = {};
  cold.on_cancel_ = {};
  cold.on_headers_ = {};
  cold.on_message_ = {};
  cold.on_half_close_ = {};
  cold.on_close_ = {};
  const std::size_t w = static_cast<std::size_t>(*idx) >> 6U;
  const std::uint32_t bit = *idx & 63U;
  active_words_[w] = active_words_[w].clear(bit);
  if (w < 64U && active_words_[w].is_empty())
    active_l0_ = active_l0_.clear(static_cast<std::uint32_t>(w));
  if (gen < std::numeric_limits<std::uint32_t>::max()) {
    free_words_[w] = free_words_[w].set(bit);
    if (w < 64U) free_l0_ = free_l0_.set(static_cast<std::uint32_t>(w));
  }
  return {};
}
std::expected<StreamState, Status> StreamMultiplexer::State(StreamHandle stream) const noexcept {
  const auto idx = FindIndex(stream);
  return idx ? std::expected<StreamState, Status>{hot_[*idx].StateValue()}
             : std::unexpected(Invalid());
}
std::expected<std::size_t, Status> StreamMultiplexer::QueuedBytes(
    StreamHandle stream) const noexcept {
  const auto idx = FindIndex(stream);
  return idx ? std::expected<std::size_t, Status>{hot_[*idx].queued_bytes_}
             : std::unexpected(Invalid());
}
void StreamMultiplexer::SetWireHandler(WireHandler handler) noexcept {
  if (uarch::unlikely(polling_)) {
    ++rejected_transitions_;
    return;
  }
  wire_handler_ = std::move(handler);
}
std::expected<void, Status> StreamMultiplexer::SetOnWriteReady(StreamHandle stream,
                                                               ReadyHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_ready_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnCancel(StreamHandle stream,
                                                           ReadyHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_cancel_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnHeaders(StreamHandle stream,
                                                            HeadersHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_headers_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnMessage(StreamHandle stream,
                                                            MessageHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_message_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnHalfClose(StreamHandle stream,
                                                              ReadyHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_half_close_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnClose(StreamHandle stream,
                                                          CloseHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>()))
    return std::unexpected(Invalid());
  cold_[*idx].on_close_ = std::move(handler);
  return {};
}
bool StreamMultiplexer::CanSend(const StreamSlotHot& slot) const noexcept {
  const StreamState st = slot.StateValue();
  return st == StreamState::kOpen || st == StreamState::kRemoteHalfClosed;
}
bool StreamMultiplexer::CanReceive(const StreamSlotHot& slot) const noexcept {
  const StreamState st = slot.StateValue();
  return st == StreamState::kOpen || st == StreamState::kLocalHalfClosed;
}

bool StreamMultiplexer::Limited(const StreamSlotHot& slot, bool outbound) const noexcept {
  const bool is_server = slot.meta_.test<^^StreamSlotMetaSpec::server>();
  const bool server_direction = outbound ? is_server : !is_server;
  switch (slot.KindValue()) {
    case RpcKind::kUnary:
      return true;
    case RpcKind::kServerStreaming:
      return !server_direction;
    case RpcKind::kClientStreaming:
      return server_direction;
    case RpcKind::kBidiStreaming:
      return false;
  }
  std::unreachable();
}
std::expected<void, Status> StreamMultiplexer::Transition(StreamSlotHot& slot, Event event,
                                                          bool peer) noexcept {
  const StreamState current = slot.StateValue();
  StreamState next = current;
  bool valid = false;
  switch (current) {
    case StreamState::kIdle:
      valid = (event == Event::kOpen);
      next = StreamState::kOpen;
      break;
    case StreamState::kOpen:
      valid = (event == Event::kLocalEnd || event == Event::kRemoteEnd || event == Event::kReset);
      next = (event == Event::kLocalEnd)    ? StreamState::kLocalHalfClosed
             : (event == Event::kRemoteEnd) ? StreamState::kRemoteHalfClosed
                                            : StreamState::kReset;
      break;
    case StreamState::kLocalHalfClosed:
      valid = (event == Event::kRemoteEnd || event == Event::kReset);
      next = (event == Event::kRemoteEnd) ? StreamState::kClosed : StreamState::kReset;
      break;
    case StreamState::kRemoteHalfClosed:
      valid = (event == Event::kLocalEnd || event == Event::kReset);
      next = (event == Event::kLocalEnd) ? StreamState::kClosed : StreamState::kReset;
      break;
    case StreamState::kClosed:
      valid = (event == Event::kReset && slot.count_ != 0U);
      next = StreamState::kReset;
      break;
    case StreamState::kReset:
      break;
  }
  if (uarch::unlikely(!valid)) {
    if (peer) ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  slot.meta_ = slot.meta_.with<^^StreamSlotMetaSpec::state>(next);
  if (event == Event::kRemoteEnd) {
    slot.meta_ = slot.meta_.with<^^StreamSlotMetaSpec::half_closed_remote>(true);
  }
  return {};
}
std::expected<void, Status> StreamMultiplexer::Queue(StreamSlotHot& hot, StreamSlotCold& cold,
                                                     StreamFrameKind kind,
                                                     std::span<const std::byte> bytes,
                                                     const MetadataMap* metadata,
                                                     const Status& status) noexcept {
  if (uarch::unlikely(status.Message().size() > 256U)) return std::unexpected(Full());
  if (uarch::unlikely(hot.count_ == options_.queue_depth ||
                      bytes.size() > options_.max_message_bytes ||
                      bytes.size() > options_.high_water_bytes - hot.queued_bytes_)) {
    return std::unexpected(Full());
  }
  const std::uint32_t index = queue_depth_div_.rem(hot.head_ + hot.count_);
  auto& entry = cold.entries_[index];
  entry.kind_ = kind;
  entry.size_ = bytes.size();
  entry.offset_ = 0U;
  entry.status_ = status;
  entry.metadata_ = metadata ? *metadata : MetadataMap{};
  if (!bytes.empty()) {
    std::byte* __restrict dst =
        cold.bytes_.get() + static_cast<std::size_t>(index) * options_.max_message_bytes;
    std::memcpy(dst, bytes.data(), bytes.size());
  }
  hot.queued_bytes_ += bytes.size();
  ++hot.count_;
  return {};
}
std::expected<void, Status> StreamMultiplexer::SendHeaders(StreamHandle stream,
                                                           const MetadataMap& metadata) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(!CanSend(hot) || hot.meta_.test<^^StreamSlotMetaSpec::sent_headers>() ||
                      hot.sent_messages_ != 0U)) {
    return std::unexpected(Invalid());
  }
  auto result = Queue(hot, cold_[*idx], StreamFrameKind::kHeaders, {}, &metadata, {});
  if (result) hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::sent_headers>(true);
  return result;
}
std::expected<WriteStatus, Status> StreamMultiplexer::Send(
    StreamHandle stream, std::span<const std::byte> bytes) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(!CanSend(hot) || (Limited(hot, true) && hot.sent_messages_ != 0U))) {
    return std::unexpected(Invalid());
  }
  if (auto result = Queue(hot, cold_[*idx], StreamFrameKind::kMessage, bytes, nullptr, {});
      uarch::unlikely(!result)) {
    return std::unexpected(result.error());
  }
  ++hot.sent_messages_;
  if (hot.queued_bytes_ >= options_.high_water_bytes || hot.count_ == options_.queue_depth ||
      hot.window_ == 0U) {
    hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::pressured>(true);
  }
  return hot.meta_.test<^^StreamSlotMetaSpec::pressured>() ? WriteStatus::kHighWaterMarkReached
                                                           : WriteStatus::kQueued;
}
std::expected<void, Status> StreamMultiplexer::HalfClose(StreamHandle stream) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(!CanSend(hot))) return std::unexpected(Invalid());
  if (auto result = Queue(hot, cold_[*idx], StreamFrameKind::kHalfClose, {}, nullptr, {});
      uarch::unlikely(!result)) {
    return result;
  }
  hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::end_stream_sent>(true);
  return Transition(hot, Event::kLocalEnd, false);
}
std::expected<void, Status> StreamMultiplexer::Finish(StreamHandle stream, const Status& status,
                                                      const MetadataMap& trailers) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(!hot.meta_.test<^^StreamSlotMetaSpec::server>() || !CanSend(hot))) {
    return std::unexpected(Invalid());
  }
  if (auto result = Queue(hot, cold_[*idx], StreamFrameKind::kTrailers, {}, &trailers, status);
      uarch::unlikely(!result)) {
    return result;
  }
  hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::trailers_queued>(true)
                  .with<^^StreamSlotMetaSpec::end_stream_sent>(true);
  return Transition(hot, Event::kLocalEnd, false);
}
std::expected<void, Status> StreamMultiplexer::Cancel(StreamHandle stream) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (auto result = Transition(hot, Event::kReset, false); uarch::unlikely(!result)) return result;
  hot.count_ = 0U;
  hot.queued_bytes_ = 0U;
  hot.inbound_size_ = 0U;
  hot.meta_ = kCancelPatch.apply(hot.meta_);
  return {};
}
std::expected<void, Status> StreamMultiplexer::UpdateWindow(StreamHandle stream,
                                                            std::size_t increment) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(increment == 0U ||
                      increment > std::numeric_limits<std::size_t>::max() - hot.window_)) {
    return std::unexpected(OutOfRangeStatus());
  }
  hot.window_ += increment;
  return {};
}
std::expected<void, Status> StreamMultiplexer::Feed(StreamHandle stream,
                                                    const StreamFrame& frame) noexcept {
  switch (frame.kind) {
    case StreamFrameKind::kHeaders:
      if (uarch::unlikely(!frame.metadata)) return std::unexpected(Invalid());
      return ReceiveHeaders(stream, *frame.metadata);
    case StreamFrameKind::kMessage:
      return ReceiveChunk(stream, frame.payload, frame.message_end);
    case StreamFrameKind::kHalfClose:
      return ReceiveHalfClose(stream);
    case StreamFrameKind::kTrailers:
      if (uarch::unlikely(!frame.metadata)) return std::unexpected(Invalid());
      return ReceiveTrailers(stream, frame.status, *frame.metadata);
    case StreamFrameKind::kReset:
      return ReceiveReset(stream);
  }
  std::unreachable();
}
std::expected<void, Status> StreamMultiplexer::ReceiveHeaders(
    StreamHandle stream, const MetadataMap& metadata) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(!kReceiveHeadersReadyPattern.matches(hot.meta_) || !CanReceive(hot) ||
                      hot.received_messages_ != 0U)) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::received_headers>(true);
  auto& cold = cold_[*idx];
  if (cold.on_headers_) {
    hot.EnterCallback();
    cold.on_headers_(metadata);
    hot.LeaveCallback();
  }
  return {};
}
std::expected<void, Status> StreamMultiplexer::ReceiveMessage(
    StreamHandle stream, std::span<const std::byte> bytes) noexcept {
  return ReceiveChunk(stream, bytes, true);
}
std::expected<void, Status> StreamMultiplexer::ReceiveChunk(StreamHandle stream,
                                                            std::span<const std::byte> bytes,
                                                            bool message_end) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(hot.meta_.test<^^StreamSlotMetaSpec::callback_active>() || !CanReceive(hot) ||
                      (Limited(hot, false) && hot.received_messages_ != 0U))) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (uarch::unlikely(bytes.size() > options_.max_message_bytes - hot.inbound_size_)) {
    return std::unexpected(Full());
  }
  auto& cold = cold_[*idx];
  if (!bytes.empty())
    std::memcpy(cold.inbound_.get() + hot.inbound_size_, bytes.data(), bytes.size());
  hot.inbound_size_ += bytes.size();
  if (message_end) {
    hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::inbound_in_progress>(false);
    ++hot.received_messages_;
    const auto message = std::span<const std::byte>{cold.inbound_.get(), hot.inbound_size_};
    hot.inbound_size_ = 0U;
    if (cold.on_message_) {
      hot.EnterCallback();
      cold.on_message_(message);
      hot.LeaveCallback();
    }
  } else {
    hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::inbound_in_progress>(true);
  }
  return {};
}
std::expected<void, Status> StreamMultiplexer::ReceiveHalfClose(StreamHandle stream) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(hot.meta_.test<^^StreamSlotMetaSpec::callback_active>() ||
                      hot.meta_.test<^^StreamSlotMetaSpec::inbound_in_progress>())) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(hot, Event::kRemoteEnd, true); uarch::unlikely(!result))
    return result;
  auto& cold = cold_[*idx];
  if (cold.on_half_close_) {
    hot.EnterCallback();
    cold.on_half_close_();
    hot.LeaveCallback();
  }
  return {};
}
void StreamMultiplexer::NotifyReset(StreamSlotHot& hot, StreamSlotCold& cold) noexcept {
  if (hot.meta_.test<^^StreamSlotMetaSpec::reset_notified>()) return;
  hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::reset_notified>(true);
  hot.EnterCallback();
  if (cold.on_cancel_) cold.on_cancel_();
  const MetadataMap empty;
  if (cold.on_close_ && !hot.meta_.test<^^StreamSlotMetaSpec::close_notified>()) {
    hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::close_notified>(true);
    cold.on_close_(Cancelled(), empty);
  }
  hot.LeaveCallback();
}
std::expected<void, Status> StreamMultiplexer::ReceiveReset(StreamHandle stream) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(hot.meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(hot, Event::kReset, true); uarch::unlikely(!result)) return result;
  hot.count_ = 0U;
  hot.queued_bytes_ = 0U;
  hot.inbound_size_ = 0U;
  hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::pressured>(false)
                  .with<^^StreamSlotMetaSpec::inbound_in_progress>(false);
  NotifyReset(hot, cold_[*idx]);
  return {};
}
std::expected<void, Status> StreamMultiplexer::ReceiveTrailers(
    StreamHandle stream, const Status& status, const MetadataMap& trailers) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx.has_value())) return std::unexpected(Invalid());
  auto& hot = hot_[*idx];
  if (uarch::unlikely(hot.meta_.test<^^StreamSlotMetaSpec::callback_active>() ||
                      hot.meta_.test<^^StreamSlotMetaSpec::server>() ||
                      hot.meta_.test<^^StreamSlotMetaSpec::inbound_in_progress>())) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(hot, Event::kRemoteEnd, true); uarch::unlikely(!result))
    return result;
  if (!hot.meta_.test<^^StreamSlotMetaSpec::close_notified>()) {
    hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::close_notified>(true);
    auto& cold = cold_[*idx];
    if (cold.on_close_) {
      hot.EnterCallback();
      cold.on_close_(status, trailers);
      hot.LeaveCallback();
    }
  }
  return {};
}
std::optional<std::uint32_t> StreamMultiplexer::NextActiveFrom(
    std::uint32_t start_slot) const noexcept {
  const std::uint32_t slot =
      uarch::select_register(uarch::predicate(start_slot < options_.max_streams), start_slot, 0U);
  const std::uint32_t sw = slot >> 6U;
  const std::uint32_t sb = slot & 63U;
  if (uarch::likely(bitmap_words_ <= 64U)) {
    if (active_l0_.is_empty()) return std::nullopt;
    if (const auto bit = active_words_[sw].next_one_from(sb)) return (sw << 6U) + *bit;
    if (const auto w = active_l0_.next_one_from(sw + 1U))
      return (*w << 6U) + *active_words_[*w].lowest_one();
    if (const auto w = active_l0_.lowest_one(); w && *w < sw)
      return (*w << 6U) + *active_words_[*w].lowest_one();
    if (const auto bit = active_words_[sw].lowest_one(); bit && *bit < sb) return (sw << 6U) + *bit;
    return std::nullopt;
  }
  if (const auto bit = active_words_[sw].next_one_from(sb)) return (sw << 6U) + *bit;
  for (std::size_t off = 1U; off < bitmap_words_; ++off) {
    const std::size_t w = (sw + off < bitmap_words_) ? (sw + off) : (sw + off - bitmap_words_);
    if (const auto bit = active_words_[w].lowest_one())
      return static_cast<std::uint32_t>(w << 6U) + *bit;
  }
  if (const auto bit = active_words_[sw].lowest_one(); bit && *bit < sb) return (sw << 6U) + *bit;
  return std::nullopt;
}
std::expected<PollResult, Status> StreamMultiplexer::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp) noexcept {
  if (uarch::unlikely(!Ready())) return std::unexpected(Full());
  if (uarch::unlikely(polling_)) return std::unexpected(Invalid());
  PollResult result{};
  const auto has_work = [this](std::size_t w) noexcept {
    for (const std::uint32_t bit : active_words_[w] | uarch::views::ones) {
      const auto& hot = hot_[(w << 6U) + bit];
      if (hot.meta_.test<^^StreamSlotMetaSpec::reset_pending>() || hot.count_ != 0U ||
          (hot.meta_.test<^^StreamSlotMetaSpec::pressured>() &&
           hot.queued_bytes_ < options_.low_water_bytes && hot.window_ != 0U &&
           hot.count_ < options_.queue_depth)) {
        return true;
      }
    }
    return false;
  };
  const auto pending = [this, &has_work]() noexcept {
    for (const std::uint32_t w : active_l0_ | uarch::views::ones)
      if (has_work(w)) return true;
    for (std::size_t w = 64U; w < bitmap_words_; ++w)
      if (has_work(w)) return true;
    return false;
  };
  const auto maybe_notify_ready = [this, &result, &budget](StreamSlotHot& hot,
                                                           StreamSlotCold& cold) noexcept {
    if (hot.meta_.test<^^StreamSlotMetaSpec::pressured>() &&
        hot.queued_bytes_ < options_.low_water_bytes && hot.count_ < options_.queue_depth &&
        hot.window_ != 0U && result.commands < budget.commands) {
      hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::pressured>(false);
      ++result.commands;
      if (cold.on_ready_) {
        hot.EnterCallback();
        cold.on_ready_();
        hot.LeaveCallback();
      }
    }
  };
  result.pending = pending();
  if (budget.frames == 0U || !wire_handler_) return result;
  polling_ = true;
  std::uint32_t scanned = 0U;
  while (result.frames < budget.frames) {
    const auto active_idx = NextActiveFrom(next_slot_);
    if (!active_idx) break;
    const std::uint32_t index = *active_idx;
    const std::uint32_t step = (index >= next_slot_)
                                   ? (index - next_slot_ + 1U)
                                   : (options_.max_streams - next_slot_ + index + 1U);
    if (scanned + step > options_.max_streams) break;
    scanned += step;
    next_slot_ =
        uarch::select_register(uarch::predicate(index + 1U < options_.max_streams), index + 1U, 0U);
    auto& hot = hot_[index];
    auto& cold = cold_[index];
    const std::uint32_t prev_cmds = result.commands;
    maybe_notify_ready(hot, cold);
    if (result.commands != prev_cmds) scanned = 0U;
    const bool reset_pending = hot.meta_.test<^^StreamSlotMetaSpec::reset_pending>();
    if (!reset_pending && hot.count_ == 0U) continue;
    StreamFrame frame{.stream = StreamHandle{index, hot.GenerationValue()}};
    if (reset_pending) {
      frame.kind = StreamFrameKind::kReset;
      frame.status = Cancelled();
    } else {
      auto& entry = cold.entries_[hot.head_];
      frame.kind = entry.kind_;
      frame.metadata = &entry.metadata_;
      frame.end_stream =
          entry.kind_ == StreamFrameKind::kHalfClose || entry.kind_ == StreamFrameKind::kTrailers;
      if (entry.kind_ == StreamFrameKind::kMessage) {
        const std::size_t remaining = entry.size_ - entry.offset_;
        if (remaining != 0U && hot.window_ == 0U) continue;
        const std::size_t count = std::min(remaining, hot.window_);
        frame.payload = {cold.bytes_.get() +
                             static_cast<std::size_t>(hot.head_) * options_.max_message_bytes +
                             entry.offset_,
                         count};
        frame.message_end = count == remaining;
      }
      frame.status = std::move(entry.status_);
    }
    wire_active_ = true;
    auto sent = wire_handler_(frame);
    wire_active_ = false;
    if (!reset_pending) cold.entries_[hot.head_].status_ = std::move(frame.status);
    if (uarch::unlikely(!sent)) {
      polling_ = false;
      return std::unexpected(std::move(sent.error()));
    }
    ++result.frames;
    scanned = 0U;
    if (reset_pending) {
      hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::reset_pending>(false);
      NotifyReset(hot, cold);
      continue;
    }
    auto& entry = cold.entries_[hot.head_];
    entry.offset_ += frame.payload.size();
    hot.queued_bytes_ -= frame.payload.size();
    if (entry.kind_ == StreamFrameKind::kMessage) hot.window_ -= frame.payload.size();
    if (frame.message_end) {
      hot.head_ = queue_depth_div_.rem(hot.head_ + 1U);
      --hot.count_;
    }
    maybe_notify_ready(hot, cold);
  }
  polling_ = false;
  result.pending = pending();
  return result;
}

}  // namespace grpcxx
