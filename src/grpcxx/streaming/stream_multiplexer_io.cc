#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <ranges>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/streaming/stream_multiplexer.h"
#include "grpcxx/streaming/stream_multiplexer_internal.h"

namespace grpcxx {

using streaming_detail::Cancelled;
using streaming_detail::Full;
using streaming_detail::Invalid;
using streaming_detail::kCancelPatch;
using streaming_detail::kReceiveHeadersReadyPattern;
using streaming_detail::OutOfRangeStatus;
using streaming_detail::StreamSlotMetaSpec;

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
  if (!bytes.empty()) {
    std::memcpy(cold.inbound_.get() + hot.inbound_size_, bytes.data(), bytes.size());
  }
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
  if (auto result = Transition(hot, Event::kRemoteEnd, true); uarch::unlikely(!result)) {
    return result;
  }
  auto& cold = cold_[*idx];
  if (cold.on_half_close_) {
    hot.EnterCallback();
    cold.on_half_close_();
    hot.LeaveCallback();
  }
  return {};
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
  if (auto result = Transition(hot, Event::kRemoteEnd, true); uarch::unlikely(!result)) {
    return result;
  }
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
    if (const auto w = active_l0_.next_one_from(sw + 1U)) {
      return (*w << 6U) + *active_words_[*w].lowest_one();
    }
    if (const auto w = active_l0_.lowest_one(); w && *w < sw) {
      return (*w << 6U) + *active_words_[*w].lowest_one();
    }
    if (const auto bit = active_words_[sw].lowest_one(); bit && *bit < sb) return (sw << 6U) + *bit;
    return std::nullopt;
  }
  if (const auto bit = active_words_[sw].next_one_from(sb)) return (sw << 6U) + *bit;
  for (std::size_t off = 1U; off < bitmap_words_; ++off) {
    const std::size_t w = (sw + off < bitmap_words_) ? (sw + off) : (sw + off - bitmap_words_);
    if (const auto bit = active_words_[w].lowest_one()) {
      return static_cast<std::uint32_t>(w << 6U) + *bit;
    }
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
      uarch::mem::prefetch_read<uarch::mem::L1>(cold_[(w << 6U) + bit]);
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
    for (const std::uint32_t w : active_l0_ | uarch::views::ones) {
      if (has_work(w)) return true;
    }
    for (std::size_t w = 64U; w < bitmap_words_; ++w) {
      if (has_work(w)) return true;
    }
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
    uarch::mem::prefetch_read<uarch::mem::L1>(cold_[index]);
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
