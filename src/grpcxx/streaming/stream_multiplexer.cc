#include "grpcxx/streaming/stream_multiplexer.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/streaming/stream_multiplexer_internal.h"

namespace grpcxx {

using streaming_detail::Cancelled;
using streaming_detail::Full;
using streaming_detail::Invalid;
using streaming_detail::StreamSlotMetaSpec;
using streaming_detail::StreamSlotMetaWord;

StreamMultiplexer::StreamMultiplexer() noexcept : StreamMultiplexer(Options{}) {}

StreamMultiplexer::StreamMultiplexer(Options options) noexcept : options_(options) {
  std::size_t total_queue_bytes = 0U;
  std::size_t total_arena_bytes = 0U;
  if (options.max_streams == 0U || options.queue_depth == 0U || options.max_message_bytes == 0U ||
      options.high_water_bytes == 0U || options.low_water_bytes == 0U ||
      options.low_water_bytes >= options.high_water_bytes ||
      __builtin_mul_overflow(static_cast<std::size_t>(options.queue_depth),
                             options.max_message_bytes, &total_queue_bytes) ||
      options.high_water_bytes > total_queue_bytes ||
      __builtin_mul_overflow(static_cast<std::size_t>(options.max_streams), total_queue_bytes,
                             &total_arena_bytes)) {
    return;
  }

  queue_depth_div_ = *uarch::FastDivisorU32::try_new(options.queue_depth);
  bitmap_words_ = (static_cast<std::size_t>(options.max_streams) + 63U) >> 6U;

  free_words_ = std::make_unique<uarch::Bitmap64[]>(bitmap_words_);
  active_words_ = std::make_unique<uarch::Bitmap64[]>(bitmap_words_);
  hot_ = std::make_unique<StreamSlotHot[]>(options.max_streams);
  cold_ = std::make_unique<StreamSlotCold[]>(options.max_streams);

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
    cold_slot.entries_ = std::make_unique<Entry[]>(options.queue_depth);
    cold_slot.bytes_ = std::make_unique<std::byte[]>(total_queue_bytes);
    cold_slot.inbound_ = std::make_unique<std::byte[]>(options.max_message_bytes);
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
        if (w < 64U && free_words_[w].is_empty()) {
          free_l0_ = free_l0_.clear(static_cast<std::uint32_t>(w));
        }
        continue;
      }
      hot.meta_ = hot.meta_.wrapping_add_top_field<^^StreamSlotMetaSpec::generation>(1ULL)
                      .with<^^StreamSlotMetaSpec::used>(true)
                      .with<^^StreamSlotMetaSpec::server>(is_server)
                      .with<^^StreamSlotMetaSpec::kind>(kind);
      hot.window_ = StreamMultiplexerOptions{options_.max_streams, options_.queue_depth,
                                             options_.initial_window}
                        .initial_window;
      if (auto result = Transition(hot, Event::kOpen, false); uarch::unlikely(!result)) {
        hot.meta_ = hot.meta_.with<^^StreamSlotMetaSpec::used>(false);
        return std::unexpected(result.error());
      }
      free_words_[w] = free_words_[w].clear(*bit);
      if (w < 64U && free_words_[w].is_empty()) {
        free_l0_ = free_l0_.clear(static_cast<std::uint32_t>(w));
      }
      active_words_[w] = active_words_[w].set(*bit);
      if (w < 64U) active_l0_ = active_l0_.set(static_cast<std::uint32_t>(w));
      return StreamId::Pack(index, hot.GenerationValue()).Handle();
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
  if (w < 64U && active_words_[w].is_empty()) {
    active_l0_ = active_l0_.clear(static_cast<std::uint32_t>(w));
  }
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
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  cold_[*idx].on_ready_ = std::move(handler);
  return {};
}

std::expected<void, Status> StreamMultiplexer::SetOnCancel(StreamHandle stream,
                                                           ReadyHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  cold_[*idx].on_cancel_ = std::move(handler);
  return {};
}

std::expected<void, Status> StreamMultiplexer::SetOnHeaders(StreamHandle stream,
                                                            HeadersHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  cold_[*idx].on_headers_ = std::move(handler);
  return {};
}

std::expected<void, Status> StreamMultiplexer::SetOnMessage(StreamHandle stream,
                                                            MessageHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  cold_[*idx].on_message_ = std::move(handler);
  return {};
}

std::expected<void, Status> StreamMultiplexer::SetOnHalfClose(StreamHandle stream,
                                                              ReadyHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
  cold_[*idx].on_half_close_ = std::move(handler);
  return {};
}

std::expected<void, Status> StreamMultiplexer::SetOnClose(StreamHandle stream,
                                                          CloseHandler handler) noexcept {
  const auto idx = FindIndex(stream);
  if (uarch::unlikely(!idx || hot_[*idx].meta_.test<^^StreamSlotMetaSpec::callback_active>())) {
    return std::unexpected(Invalid());
  }
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

}  // namespace grpcxx
