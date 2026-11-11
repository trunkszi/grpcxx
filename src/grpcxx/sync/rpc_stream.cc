#include <chrono>
#include <string>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/sync/synchronous_channel_internal.h"

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeStreamError(StatusCode code,
                                                               std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard]] std::expected<void, Status> AdoptWire(SynchronousChannel::Impl& impl,
                                                    std::expected<bool, Status> io) noexcept {
  if (io.has_value()) return {};
  impl.FailAll(io.error());
  return std::unexpected(impl.failure);
}

}  // namespace

RpcStream::RpcStream(SynchronousChannel& channel, std::unique_ptr<State> state) noexcept
    : channel_(&channel), state_(std::move(state)) {
  RebindHandle();
}

RpcStream::RpcStream(RpcStream&& other) noexcept
    : channel_(std::exchange(other.channel_, nullptr)), state_(std::move(other.state_)) {
  RebindHandle();
}

RpcStream& RpcStream::operator=(RpcStream&& other) noexcept {
  if (this != &other) {
    ReleaseOnTeardown();
    channel_ = std::exchange(other.channel_, nullptr);
    state_ = std::move(other.state_);
    RebindHandle();
  }
  return *this;
}

RpcStream::~RpcStream() noexcept {
  if (channel_ == nullptr) {
    state_.reset();
    return;
  }
  ReleaseOnTeardown();
}

void RpcStream::DetachChannel() noexcept { channel_ = nullptr; }

void RpcStream::RebindHandle() noexcept {
  if (channel_ == nullptr || state_ == nullptr) return;
  const auto slot = state_->slot_index;
  if (slot < SynchronousChannel::Impl::kMaxSlots) channel_->impl_->handles[slot] = this;
}

std::expected<void, Status> RpcStream::RequireAttached() noexcept {
  if (channel_ != nullptr && state_ != nullptr) return {};
  return std::unexpected(MakeStreamError(StatusCode::kFailedPrecondition,
                                         state_ == nullptr ? "stream moved" : "channel destroyed"));
}

void RpcStream::ReleaseOnTeardown() noexcept {
  if (channel_ == nullptr || state_ == nullptr) return;
  auto& impl = *channel_->impl_;
  if (!impl.IsClosed() && !state_->IsPeerClosed() && state_->id != 0U && !state_->RstSent()) {
    if (impl.h2) {
      if (auto rst = impl.h2->SubmitRstStream(state_->id, nexora::Http2ErrorCode::kCancel);
          rst ||
          rst.error() == nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
        state_->meta = state_->meta.with<^^SyncStreamMetaSpec::rst_sent>(true);
      } else if (impl.failure.Ok()) {
        impl.failure = MakeStreamError(StatusCode::kUnavailable, rst.error().Message());
      }
      if (auto flushed = impl.FlushWire(); !flushed && impl.failure.Ok()) {
        impl.failure = std::move(flushed.error());
      }
    }
  }
  const auto slot = state_->slot_index;
  if (slot < SynchronousChannel::Impl::kMaxSlots && impl.streams[slot] == state_.get()) {
    impl.streams[slot] = nullptr;
    if (impl.handles[slot] == this) impl.handles[slot] = nullptr;
    impl.active_streams_mask = impl.active_streams_mask.clear(slot);
  }
  state_.reset();
  channel_ = nullptr;
}

std::expected<void, Status> RpcStream::Send(std::span<const std::byte> payload) noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) return attached;
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) return owned;
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (uarch::unlikely(!state_->IsWritable())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "stream is not writable"));
  }
  if (uarch::unlikely(
          (state_->kind == RpcKind::kUnary || state_->kind == RpcKind::kServerStreaming) &&
          state_->sent_messages >= 1uz)) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "RPC allows only one request message"));
  }
  if (uarch::unlikely(std::chrono::steady_clock::now() >= state_->deadline)) {
    impl.AbortStream(*state_,
                     MakeStreamError(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"), true);
    if (auto flushed = AdoptWire(impl, impl.FlushWire()); !flushed) return flushed;
    return std::unexpected(state_->status);
  }
  if (uarch::unlikely(payload.size() > impl.options.max_message_bytes)) {
    return std::unexpected(
        MakeStreamError(StatusCode::kResourceExhausted, "request message exceeds limit"));
  }

  auto encoded = impl.compression.Compress(state_->options.compression, payload);
  if (uarch::unlikely(!encoded.has_value())) return std::unexpected(std::move(encoded.error()));
  if (uarch::unlikely(encoded->WireSize() > impl.options.max_queued_bytes)) {
    return std::unexpected(
        MakeStreamError(StatusCode::kResourceExhausted, "request message exceeds queue"));
  }

  while (state_->outgoing_bytes + encoded->WireSize() > impl.options.max_queued_bytes) {
    auto waited = impl.PollUntilProgress(true, state_->deadline);
    if (uarch::unlikely(!waited.has_value())) {
      if (waited.error().Code() == StatusCode::kDeadlineExceeded) {
        impl.AbortStream(*state_, waited.error(), true);
        if (auto flushed = AdoptWire(impl, impl.FlushWire()); !flushed) return flushed;
      }
      return std::unexpected(state_->IsTerminal() ? state_->status : waited.error());
    }
    if (uarch::unlikely(state_->IsTerminal())) return std::unexpected(state_->status);
  }

  state_->outgoing_bytes += encoded->WireSize();
  state_->outgoing.push_back(std::move(*encoded));
  ++state_->sent_messages;
  if (auto pumped = impl.PumpOutgoing(); uarch::unlikely(!pumped.has_value())) {
    return std::unexpected(std::move(pumped.error()));
  }
  return AdoptWire(impl, impl.FlushWire());
}

std::expected<void, Status> RpcStream::HalfClose() noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) return attached;
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) return owned;
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (uarch::unlikely(!state_->IsWritable())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "stream cannot half-close"));
  }
  if (uarch::unlikely(
          (state_->kind == RpcKind::kUnary || state_->kind == RpcKind::kServerStreaming) &&
          state_->sent_messages != 1uz)) {
    return std::unexpected(MakeStreamError(StatusCode::kFailedPrecondition,
                                           "RPC requires exactly one request message"));
  }
  if (uarch::unlikely(std::chrono::steady_clock::now() >= state_->deadline)) {
    impl.AbortStream(*state_,
                     MakeStreamError(StatusCode::kDeadlineExceeded, "RPC deadline exceeded"), true);
    if (auto flushed = AdoptWire(impl, impl.FlushWire()); !flushed) return flushed;
    return std::unexpected(state_->status);
  }
  state_->meta = state_->meta.with<^^SyncStreamMetaSpec::request_ended>(true)
                     .with<^^SyncStreamMetaSpec::phase>(SyncStreamPhase::kHalfClosed);
  if (auto pumped = impl.PumpOutgoing(); uarch::unlikely(!pumped.has_value())) {
    return std::unexpected(std::move(pumped.error()));
  }
  return AdoptWire(impl, impl.FlushWire());
}

std::expected<std::optional<std::vector<std::byte>>, Status> RpcStream::TryRecvNext() noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) {
    return std::unexpected(std::move(attached.error()));
  }
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) {
    return std::unexpected(std::move(owned.error()));
  }
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (!state_->IsTerminal() && state_->incoming.empty()) {
    auto stepped = impl.PollStep();
    if (uarch::unlikely(!stepped.has_value() && !state_->IsTerminal())) {
      return std::unexpected(std::move(stepped.error()));
    }
  }
  if (!state_->incoming.empty()) {
    auto message = std::move(state_->incoming.front());
    state_->incoming.pop_front();
    state_->incoming_bytes -= message.size();
    state_->last_payload = message;
    return std::optional<std::vector<std::byte>>{std::move(message)};
  }
  if (uarch::unlikely(state_->IsTerminal() && !state_->status.Ok())) {
    return std::unexpected(state_->status);
  }
  return std::nullopt;
}

std::expected<std::optional<std::vector<std::byte>>, Status> RpcStream::RecvNext() noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) {
    return std::unexpected(std::move(attached.error()));
  }
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) {
    return std::unexpected(std::move(owned.error()));
  }
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  for (;;) {
    if (!state_->incoming.empty()) {
      auto message = std::move(state_->incoming.front());
      state_->incoming.pop_front();
      state_->incoming_bytes -= message.size();
      state_->last_payload = message;
      return std::optional<std::vector<std::byte>>{std::move(message)};
    }
    if (state_->IsTerminal()) {
      if (!state_->status.Ok()) return std::unexpected(state_->status);
      return std::nullopt;
    }
    const bool want_write =
        !state_->outgoing.empty() || (state_->RequestEnded() && !state_->EndStreamSubmitted());
    auto waited = impl.PollUntilProgress(want_write, state_->deadline);
    if (uarch::unlikely(!waited.has_value())) {
      if (waited.error().Code() == StatusCode::kDeadlineExceeded) {
        impl.AbortStream(*state_, waited.error(), true);
        if (auto flushed = AdoptWire(impl, impl.FlushWire()); !flushed) {
          return std::unexpected(std::move(flushed.error()));
        }
      }
      if (state_->IsTerminal()) return std::unexpected(state_->status);
      return std::unexpected(std::move(waited.error()));
    }
  }
}

std::expected<RpcResponse, Status> RpcStream::Finish() noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) {
    return std::unexpected(std::move(attached.error()));
  }
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) {
    return std::unexpected(std::move(owned.error()));
  }
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (state_->IsWritable()) {
    if (auto closed = HalfClose(); uarch::unlikely(!closed.has_value())) {
      return std::unexpected(std::move(closed.error()));
    }
  }
  while (!state_->IsTerminal()) {
    const bool want_write = !state_->outgoing.empty() || !state_->EndStreamSubmitted();
    auto waited = impl.PollUntilProgress(want_write, state_->deadline);
    if (uarch::unlikely(!waited.has_value() && !state_->IsTerminal())) {
      if (waited.error().Code() == StatusCode::kDeadlineExceeded) {
        impl.AbortStream(*state_, waited.error(), true);
        if (auto flushed = AdoptWire(impl, impl.FlushWire()); !flushed) {
          return std::unexpected(std::move(flushed.error()));
        }
        break;
      }
      return std::unexpected(std::move(waited.error()));
    }
  }
  while (!state_->incoming.empty()) {
    state_->last_payload = std::move(state_->incoming.front());
    state_->incoming.pop_front();
  }
  state_->incoming_bytes = 0uz;
  if (uarch::unlikely(
          state_->status.Ok() &&
          (state_->kind == RpcKind::kUnary || state_->kind == RpcKind::kClientStreaming) &&
          state_->received_messages != 1uz)) {
    state_->status =
        MakeStreamError(StatusCode::kInternal, "RPC requires exactly one response message");
    state_->meta = state_->meta.with<^^SyncStreamMetaSpec::local_failure>(true);
  }
  if (uarch::unlikely(state_->IsLocalFailure() && !state_->status.Ok())) {
    return std::unexpected(state_->status);
  }
  return RpcResponse{
      .payload = std::move(state_->last_payload),
      .initial_metadata = state_->initial,
      .trailing_metadata = state_->trailers,
      .status = state_->status,
  };
}

std::expected<void, Status> RpcStream::Cancel() noexcept {
  if (auto attached = RequireAttached(); uarch::unlikely(!attached.has_value())) return attached;
  auto& impl = *channel_->impl_;
  if (auto owned = impl.CheckOwner(); uarch::unlikely(!owned.has_value())) return owned;
  if (uarch::unlikely(impl.IsPolling())) {
    return std::unexpected(
        MakeStreamError(StatusCode::kFailedPrecondition, "recursive channel operation"));
  }
  if (state_->IsTerminal()) return {};
  impl.AbortStream(*state_, MakeStreamError(StatusCode::kCancelled, "RPC cancelled"), true);
  return AdoptWire(impl, impl.FlushWire());
}

std::uint32_t RpcStream::StreamId() const noexcept { return state_ ? state_->id : 0U; }

bool RpcStream::IsDone() const noexcept {
  return !state_ || (state_->IsTerminal() && state_->incoming.empty());
}

const MetadataMap& RpcStream::InitialMetadata() const noexcept {
  if (channel_ == nullptr || state_ == nullptr) {
    static const MetadataMap kEmpty{};
    return kEmpty;
  }
  return state_->initial;
}

const MetadataMap& RpcStream::TrailingMetadata() const noexcept {
  if (channel_ == nullptr || state_ == nullptr) {
    static const MetadataMap kEmpty{};
    return kEmpty;
  }
  return state_->trailers;
}

const Status& RpcStream::FinalStatus() const noexcept {
  if (channel_ != nullptr && state_ != nullptr) return state_->status;
  static const Status kMoved{StatusCode::kFailedPrecondition, "stream moved"};
  static const Status kDestroyed{StatusCode::kFailedPrecondition, "channel destroyed"};
  return state_ == nullptr ? kMoved : kDestroyed;
}

}  // namespace grpcxx
