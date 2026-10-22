#include "grpcxx/streaming/stream_multiplexer.h"

#include <algorithm>
#include <limits>
#include <new>
#include <ranges>
#include <utility>

namespace grpcxx {
namespace {

Status Missing() noexcept { return Status{StatusCode::kFailedPrecondition}; }
Status Invalid() noexcept { return Status{StatusCode::kFailedPrecondition}; }
Status Full() noexcept { return Status{StatusCode::kResourceExhausted}; }
Status Cancelled() noexcept { return Status{StatusCode::kCancelled}; }

}  // namespace

struct StreamMultiplexer::Entry {
  StreamFrameKind kind_{};
  MetadataMap metadata_;
  Status status_;
  std::size_t size_{};
  std::size_t offset_{};
};

struct StreamMultiplexer::Slot {
  std::unique_ptr<Entry[]> entries_;
  std::unique_ptr<std::byte[]> bytes_;
  std::unique_ptr<std::byte[]> inbound_;
  ReadyHandler on_ready_;
  ReadyHandler on_cancel_;
  HeadersHandler on_headers_;
  MessageHandler on_message_;
  ReadyHandler on_half_close_;
  CloseHandler on_close_;
  RpcKind kind_{};
  StreamState state_{StreamState::kIdle};
  std::uint32_t generation_{};
  std::uint32_t head_{};
  std::uint32_t count_{};
  std::size_t queued_bytes_{};
  std::size_t window_{};
  std::size_t inbound_size_{};
  std::size_t sent_messages_{};
  std::size_t received_messages_{};
  std::uint32_t callbacks_active_{};
  bool inbound_in_progress_{};
  bool active_{};
  bool server_{};
  bool sent_headers_{};
  bool received_headers_{};
  bool pressured_{};
  bool reset_pending_{};
  bool reset_notified_{};
  bool close_notified_{};
};

StreamMultiplexer::StreamMultiplexer() noexcept : StreamMultiplexer(Options{}) {}

StreamMultiplexer::StreamMultiplexer(Options options) noexcept : options_(options) {
  if (options.max_streams == 0 || options.queue_depth == 0 || options.max_message_bytes == 0 ||
      options.high_water_bytes == 0 || options.low_water_bytes == 0 ||
      options.low_water_bytes >= options.high_water_bytes ||
      options.queue_depth > std::numeric_limits<std::size_t>::max() / options.max_message_bytes ||
      options.high_water_bytes > options.queue_depth * options.max_message_bytes) {
    return;
  }
  slots_.reset(new (std::nothrow) Slot[options.max_streams]);
  if (!slots_) return;
  for (auto& slot : std::span{slots_.get(), options.max_streams}) {
    slot.entries_.reset(new (std::nothrow) Entry[options.queue_depth]);
    slot.bytes_.reset(new (std::nothrow)
                          std::byte[options.queue_depth * options.max_message_bytes]);
    slot.inbound_.reset(new (std::nothrow) std::byte[options.max_message_bytes]);
    if (!slot.entries_ || !slot.bytes_ || !slot.inbound_) {
      slots_.reset();
      return;
    }
    for (auto& entry : std::span{slot.entries_.get(), options.queue_depth}) {
      entry.status_ = Status{StatusCode::kUnknown, std::string(256, '\0')};
    }
  }
}

StreamMultiplexer::~StreamMultiplexer() = default;
bool StreamMultiplexer::Ready() const noexcept { return slots_ != nullptr; }
std::uint64_t StreamMultiplexer::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

StreamMultiplexer::Slot* StreamMultiplexer::Find(StreamHandle stream) noexcept {
  if (wire_active_ || !slots_ || stream.index >= options_.max_streams) return nullptr;
  auto& slot = slots_[stream.index];
  return slot.active_ && slot.generation_ == stream.generation ? &slot : nullptr;
}
const StreamMultiplexer::Slot* StreamMultiplexer::Find(StreamHandle stream) const noexcept {
  if (!slots_ || stream.index >= options_.max_streams) return nullptr;
  const auto& slot = slots_[stream.index];
  return slot.active_ && slot.generation_ == stream.generation ? &slot : nullptr;
}

std::expected<StreamHandle, Status> StreamMultiplexer::Open(RpcKind kind, bool is_server) noexcept {
  if (!Ready()) return std::unexpected(Full());
  for (const auto index : std::views::iota(std::uint32_t{0}, options_.max_streams)) {
    auto& slot = slots_[index];
    if (slot.active_ || slot.generation_ == std::numeric_limits<std::uint32_t>::max()) continue;
    ++slot.generation_;
    slot.active_ = true;
    slot.server_ = is_server;
    slot.kind_ = kind;
    slot.window_ = options_.initial_window;
    if (auto result = Transition(slot, Event::kOpen, false); !result) {
      slot.active_ = false;
      return std::unexpected(result.error());
    }
    return StreamHandle{index, slot.generation_};
  }
  return std::unexpected(Full());
}

std::expected<void, Status> StreamMultiplexer::Release(StreamHandle stream) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if ((slot->state_ != StreamState::kClosed && slot->state_ != StreamState::kReset) ||
      slot->count_ != 0 || slot->reset_pending_)
    return std::unexpected(Invalid());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->active_ = false;
  slot->state_ = StreamState::kIdle;
  slot->on_ready_ = {};
  slot->on_cancel_ = {};
  slot->on_headers_ = {};
  slot->on_message_ = {};
  slot->on_half_close_ = {};
  slot->on_close_ = {};
  slot->head_ = 0;
  slot->queued_bytes_ = 0;
  slot->inbound_size_ = 0;
  slot->inbound_in_progress_ = false;
  slot->sent_messages_ = 0;
  slot->received_messages_ = 0;
  slot->sent_headers_ = false;
  slot->received_headers_ = false;
  slot->pressured_ = false;
  slot->reset_notified_ = false;
  slot->close_notified_ = false;
  return {};
}

std::expected<StreamState, Status> StreamMultiplexer::State(StreamHandle stream) const noexcept {
  const auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  return slot->state_;
}
std::expected<std::size_t, Status> StreamMultiplexer::QueuedBytes(
    StreamHandle stream) const noexcept {
  const auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  return slot->queued_bytes_;
}
void StreamMultiplexer::SetWireHandler(WireHandler handler) noexcept {
  if (polling_) {
    ++rejected_transitions_;
    return;
  }
  wire_handler_ = std::move(handler);
}

std::expected<void, Status> StreamMultiplexer::SetOnWriteReady(StreamHandle stream,
                                                               ReadyHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_ready_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnCancel(StreamHandle stream,
                                                           ReadyHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_cancel_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnHeaders(StreamHandle stream,
                                                            HeadersHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_headers_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnMessage(StreamHandle stream,
                                                            MessageHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_message_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnHalfClose(StreamHandle stream,
                                                              ReadyHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_half_close_ = std::move(handler);
  return {};
}
std::expected<void, Status> StreamMultiplexer::SetOnClose(StreamHandle stream,
                                                          CloseHandler handler) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) return std::unexpected(Invalid());
  slot->on_close_ = std::move(handler);
  return {};
}

bool StreamMultiplexer::CanSend(const Slot& slot) const noexcept {
  return slot.state_ == StreamState::kOpen || slot.state_ == StreamState::kRemoteHalfClosed;
}
bool StreamMultiplexer::CanReceive(const Slot& slot) const noexcept {
  return slot.state_ == StreamState::kOpen || slot.state_ == StreamState::kLocalHalfClosed;
}
bool StreamMultiplexer::Limited(const Slot& slot, bool outbound) const noexcept {
  const bool server_direction = outbound ? slot.server_ : !slot.server_;
  switch (slot.kind_) {
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

std::expected<void, Status> StreamMultiplexer::Transition(Slot& slot, Event event,
                                                          bool peer) noexcept {
  StreamState next = slot.state_;
  bool valid = false;
  switch (slot.state_) {
    case StreamState::kIdle:
      if (event == Event::kOpen) {
        next = StreamState::kOpen;
        valid = true;
      }
      break;
    case StreamState::kOpen:
      if (event == Event::kLocalEnd) {
        next = StreamState::kLocalHalfClosed;
        valid = true;
      }
      if (event == Event::kRemoteEnd) {
        next = StreamState::kRemoteHalfClosed;
        valid = true;
      }
      if (event == Event::kReset) {
        next = StreamState::kReset;
        valid = true;
      }
      break;
    case StreamState::kLocalHalfClosed:
      if (event == Event::kRemoteEnd) {
        next = StreamState::kClosed;
        valid = true;
      }
      if (event == Event::kReset) {
        next = StreamState::kReset;
        valid = true;
      }
      break;
    case StreamState::kRemoteHalfClosed:
      if (event == Event::kLocalEnd) {
        next = StreamState::kClosed;
        valid = true;
      }
      if (event == Event::kReset) {
        next = StreamState::kReset;
        valid = true;
      }
      break;
    case StreamState::kClosed:
      if (event == Event::kReset && slot.count_ != 0) {
        next = StreamState::kReset;
        valid = true;
      }
      break;
    case StreamState::kReset:
      break;
  }
  if (!valid) {
    if (peer) ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  slot.state_ = next;
  return {};
}

std::expected<void, Status> StreamMultiplexer::Queue(Slot& slot, StreamFrameKind kind,
                                                     std::span<const std::byte> bytes,
                                                     const MetadataMap* metadata,
                                                     const Status& status) noexcept {
  if (status.Message().size() > 256) return std::unexpected(Full());
  if (slot.count_ == options_.queue_depth || bytes.size() > options_.max_message_bytes ||
      bytes.size() > options_.high_water_bytes - slot.queued_bytes_) {
    return std::unexpected(Full());
  }
  const auto index = (static_cast<std::size_t>(slot.head_) + slot.count_) % options_.queue_depth;
  auto& entry = slot.entries_[index];
  entry.kind_ = kind;
  entry.size_ = bytes.size();
  entry.offset_ = 0;
  entry.status_ = status;
  entry.metadata_ = metadata ? *metadata : MetadataMap{};
  auto destination =
      std::span{slot.bytes_.get() + index * options_.max_message_bytes, bytes.size()};
  std::ranges::copy(bytes, destination.data());
  slot.queued_bytes_ += bytes.size();
  ++slot.count_;
  return {};
}

std::expected<void, Status> StreamMultiplexer::SendHeaders(StreamHandle stream,
                                                           const MetadataMap& metadata) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (!CanSend(*slot) || slot->sent_headers_ || slot->sent_messages_ != 0)
    return std::unexpected(Invalid());
  auto result = Queue(*slot, StreamFrameKind::kHeaders, {}, &metadata, {});
  if (result) slot->sent_headers_ = true;
  return result;
}
std::expected<WriteStatus, Status> StreamMultiplexer::Send(
    StreamHandle stream, std::span<const std::byte> bytes) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (!CanSend(*slot) || (Limited(*slot, true) && slot->sent_messages_ != 0))
    return std::unexpected(Invalid());
  if (auto result = Queue(*slot, StreamFrameKind::kMessage, bytes, nullptr, {}); !result) {
    return std::unexpected(result.error());
  }
  ++slot->sent_messages_;
  if (slot->queued_bytes_ >= options_.high_water_bytes || slot->count_ == options_.queue_depth ||
      slot->window_ == 0)
    slot->pressured_ = true;
  return slot->pressured_ ? WriteStatus::kHighWaterMarkReached : WriteStatus::kQueued;
}
std::expected<void, Status> StreamMultiplexer::HalfClose(StreamHandle stream) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (!CanSend(*slot)) return std::unexpected(Invalid());
  if (auto result = Queue(*slot, StreamFrameKind::kHalfClose, {}, nullptr, {}); !result)
    return result;
  return Transition(*slot, Event::kLocalEnd, false);
}
std::expected<void, Status> StreamMultiplexer::Finish(StreamHandle stream, const Status& status,
                                                      const MetadataMap& trailers) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (!slot->server_ || !CanSend(*slot)) return std::unexpected(Invalid());
  if (auto result = Queue(*slot, StreamFrameKind::kTrailers, {}, &trailers, status); !result)
    return result;
  return Transition(*slot, Event::kLocalEnd, false);
}
std::expected<void, Status> StreamMultiplexer::Cancel(StreamHandle stream) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (auto result = Transition(*slot, Event::kReset, false); !result) return result;
  slot->count_ = 0;
  slot->queued_bytes_ = 0;
  slot->pressured_ = false;
  slot->inbound_size_ = 0;
  slot->inbound_in_progress_ = false;
  slot->reset_pending_ = true;
  return {};
}
std::expected<void, Status> StreamMultiplexer::UpdateWindow(StreamHandle stream,
                                                            std::size_t increment) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (increment == 0 || increment > std::numeric_limits<std::size_t>::max() - slot->window_) {
    return std::unexpected(Status{StatusCode::kOutOfRange});
  }
  slot->window_ += increment;
  return {};
}

std::expected<void, Status> StreamMultiplexer::Feed(StreamHandle stream,
                                                    const StreamFrame& frame) noexcept {
  switch (frame.kind) {
    case StreamFrameKind::kHeaders:
      if (!frame.metadata) return std::unexpected(Invalid());
      return ReceiveHeaders(stream, *frame.metadata);
    case StreamFrameKind::kMessage:
      return ReceiveChunk(stream, frame.payload, frame.message_end);
    case StreamFrameKind::kHalfClose:
      return ReceiveHalfClose(stream);
    case StreamFrameKind::kTrailers:
      if (!frame.metadata) return std::unexpected(Invalid());
      return ReceiveTrailers(stream, frame.status, *frame.metadata);
    case StreamFrameKind::kReset:
      return ReceiveReset(stream);
  }
  std::unreachable();
}
std::expected<void, Status> StreamMultiplexer::ReceiveHeaders(
    StreamHandle stream, const MetadataMap& metadata) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (!CanReceive(*slot) || slot->received_headers_ || slot->received_messages_ != 0 ||
      slot->inbound_in_progress_) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  slot->received_headers_ = true;
  if (slot->on_headers_) {
    ++slot->callbacks_active_;
    slot->on_headers_(metadata);
    --slot->callbacks_active_;
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
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (!CanReceive(*slot) || (Limited(*slot, false) && slot->received_messages_ != 0)) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (bytes.size() > options_.max_message_bytes - slot->inbound_size_)
    return std::unexpected(Full());
  std::ranges::copy(bytes, slot->inbound_.get() + slot->inbound_size_);
  slot->inbound_size_ += bytes.size();
  slot->inbound_in_progress_ = !message_end;
  if (message_end) {
    ++slot->received_messages_;
    const auto message = std::span<const std::byte>{slot->inbound_.get(), slot->inbound_size_};
    slot->inbound_size_ = 0;
    if (slot->on_message_) {
      ++slot->callbacks_active_;
      slot->on_message_(message);
      --slot->callbacks_active_;
    }
  }
  return {};
}
std::expected<void, Status> StreamMultiplexer::ReceiveHalfClose(StreamHandle stream) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (slot->inbound_in_progress_) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(*slot, Event::kRemoteEnd, true); !result) return result;
  if (slot->on_half_close_) {
    ++slot->callbacks_active_;
    slot->on_half_close_();
    --slot->callbacks_active_;
  }
  return {};
}
void StreamMultiplexer::NotifyReset(Slot& slot) noexcept {
  if (slot.reset_notified_) return;
  slot.reset_notified_ = true;
  ++slot.callbacks_active_;
  if (slot.on_cancel_) slot.on_cancel_();
  const MetadataMap empty;
  if (slot.on_close_ && !slot.close_notified_) {
    slot.close_notified_ = true;
    slot.on_close_(Cancelled(), empty);
  }
  --slot.callbacks_active_;
}
std::expected<void, Status> StreamMultiplexer::ReceiveReset(StreamHandle stream) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(*slot, Event::kReset, true); !result) return result;
  slot->count_ = 0;
  slot->queued_bytes_ = 0;
  slot->pressured_ = false;
  slot->inbound_size_ = 0;
  slot->inbound_in_progress_ = false;
  NotifyReset(*slot);
  return {};
}
std::expected<void, Status> StreamMultiplexer::ReceiveTrailers(
    StreamHandle stream, const Status& status, const MetadataMap& trailers) noexcept {
  auto* slot = Find(stream);
  if (!slot) return std::unexpected(Missing());
  if (slot->callbacks_active_ != 0) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (slot->server_ || slot->inbound_in_progress_) {
    ++rejected_transitions_;
    return std::unexpected(Invalid());
  }
  if (auto result = Transition(*slot, Event::kRemoteEnd, true); !result) return result;
  if (!slot->close_notified_) {
    slot->close_notified_ = true;
    if (slot->on_close_) {
      ++slot->callbacks_active_;
      slot->on_close_(status, trailers);
      --slot->callbacks_active_;
    }
  }
  return {};
}

std::expected<PollResult, Status> StreamMultiplexer::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp) noexcept {
  if (!Ready()) return std::unexpected(Full());
  if (polling_) return std::unexpected(Invalid());
  PollResult result{};
  const auto pending = [this]() noexcept {
    return std::ranges::any_of(
        std::span{slots_.get(), options_.max_streams}, [this](const Slot& slot) noexcept {
          return slot.active_ &&
                 (slot.reset_pending_ || slot.count_ != 0 ||
                  (slot.pressured_ && slot.queued_bytes_ < options_.low_water_bytes &&
                   slot.window_ != 0 && slot.count_ < options_.queue_depth));
        });
  };
  result.pending = pending();
  if (budget.frames == 0 || !wire_handler_) return result;
  polling_ = true;
  std::uint32_t idle = 0;
  while (result.frames < budget.frames && idle < options_.max_streams) {
    const auto index = next_slot_;
    next_slot_ = (next_slot_ + 1) % options_.max_streams;
    auto& slot = slots_[index];
    if (!slot.active_) {
      ++idle;
      continue;
    }
    if (slot.pressured_ && slot.queued_bytes_ < options_.low_water_bytes &&
        slot.count_ < options_.queue_depth && slot.window_ != 0 &&
        result.commands < budget.commands) {
      slot.pressured_ = false;
      ++result.commands;
      if (slot.on_ready_) {
        ++slot.callbacks_active_;
        slot.on_ready_();
        --slot.callbacks_active_;
      }
    }
    if (!slot.reset_pending_ && slot.count_ == 0) {
      ++idle;
      continue;
    }
    const StreamHandle handle{index, slot.generation_};
    StreamFrame frame{.stream = handle};
    if (slot.reset_pending_) {
      frame.kind = StreamFrameKind::kReset;
      frame.status = Cancelled();
    } else {
      auto& entry = slot.entries_[slot.head_];
      frame.kind = entry.kind_;
      frame.metadata = &entry.metadata_;
      frame.end_stream =
          entry.kind_ == StreamFrameKind::kHalfClose || entry.kind_ == StreamFrameKind::kTrailers;
      if (entry.kind_ == StreamFrameKind::kMessage) {
        const auto remaining = entry.size_ - entry.offset_;
        if (remaining != 0 && slot.window_ == 0) {
          ++idle;
          continue;
        }
        const auto count = std::min(remaining, slot.window_);
        frame.payload = {
            slot.bytes_.get() + slot.head_ * options_.max_message_bytes + entry.offset_, count};
        frame.message_end = count == remaining;
      }
    }
    if (!slot.reset_pending_) frame.status = std::move(slot.entries_[slot.head_].status_);
    wire_active_ = true;
    auto sent = wire_handler_(frame);
    wire_active_ = false;
    if (!slot.reset_pending_) slot.entries_[slot.head_].status_ = std::move(frame.status);
    if (!sent) {
      polling_ = false;
      return std::unexpected(std::move(sent.error()));
    }
    ++result.frames;
    idle = 0;
    if (slot.reset_pending_) {
      slot.reset_pending_ = false;
      NotifyReset(slot);
      continue;
    }
    auto& entry = slot.entries_[slot.head_];
    entry.offset_ += frame.payload.size();
    slot.queued_bytes_ -= frame.payload.size();
    if (entry.kind_ == StreamFrameKind::kMessage) slot.window_ -= frame.payload.size();
    if (frame.message_end) {
      slot.head_ = (slot.head_ + 1) % options_.queue_depth;
      --slot.count_;
    }
    if (slot.pressured_ && slot.queued_bytes_ < options_.low_water_bytes &&
        slot.count_ < options_.queue_depth && slot.window_ != 0 &&
        result.commands < budget.commands) {
      ++result.commands;
      slot.pressured_ = false;
      if (slot.on_ready_) {
        ++slot.callbacks_active_;
        slot.on_ready_();
        --slot.callbacks_active_;
      }
    }
  }
  polling_ = false;
  result.pending = pending();
  return result;
}

}  // namespace grpcxx
