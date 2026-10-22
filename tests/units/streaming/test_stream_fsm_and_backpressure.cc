/** @file Bounded streaming state, flow-control and handle lifetime contracts. */
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdio>
#include <source_location>
#include <utility>

#include "grpcxx/streaming/client_stream_writer.h"
#include "grpcxx/streaming/server_stream_responder.h"

namespace grpcxx {
namespace {

std::size_t failures{};

auto Check(bool success,
           std::source_location site = std::source_location::current()) noexcept -> bool {
  if (!success) {
    ++failures;
    fmt::print(stderr, "{}:{}: check failed\n", site.file_name(), site.line());
  }
  return success;
}

constexpr std::array kMessage{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

/** Small, deterministic queue limits expose backpressure without network timing. */
auto Limits() noexcept -> StreamMultiplexer::Options {
  return {.max_streams = 2,
          .queue_depth = 4,
          .max_message_bytes = 8,
          .high_water_bytes = 8,
          .low_water_bytes = 4,
          .initial_window = 64};
}

auto OneFrame() noexcept -> PollBudget {
  PollBudget budget;
  budget.frames = 1;
  return budget;
}

void StreamFsm_HalfCloseKeepsResponseDirectionOpenAndRejectsFurtherData() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kClientStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  Check(static_cast<bool>(streams.Send(*handle, kMessage)));
  if (!Check(static_cast<bool>(streams.HalfClose(*handle)))) return;
  Check((*streams.State(*handle)) == (StreamState::kLocalHalfClosed));
  auto rejected = streams.Send(*handle, kMessage);
  if (!Check(!static_cast<bool>(rejected))) return;
  Check((rejected.error().Code()) == (StatusCode::kFailedPrecondition));
  std::size_t responses{};
  if (!Check(static_cast<bool>(streams.SetOnMessage(
          *handle, [&responses](std::span<const std::byte>) noexcept { ++responses; }))))
    return;
  Check(static_cast<bool>(streams.ReceiveMessage(*handle, kMessage)));
  Check((responses) == (1));
  Check(static_cast<bool>(streams.ReceiveTrailers(*handle, Status{}, MetadataMap{})));
  Check((*streams.State(*handle)) == (StreamState::kClosed));
}

void StreamFsm_SingleMessageDirectionsEnforceCardinality() noexcept {
  for (const auto kind : {RpcKind::kUnary, RpcKind::kServerStreaming}) {
    StreamMultiplexer streams{Limits()};
    auto client = streams.Open(kind);
    if (!Check(static_cast<bool>(client))) return;
    if (!Check(static_cast<bool>(streams.Send(*client, kMessage)))) return;
    auto second = streams.Send(*client, kMessage);
    if (!Check(!static_cast<bool>(second))) return;
    Check((second.error().Code()) == (StatusCode::kFailedPrecondition));
  }
  for (const auto kind : {RpcKind::kUnary, RpcKind::kClientStreaming}) {
    StreamMultiplexer streams{Limits()};
    auto server = streams.Open(kind, true);
    if (!Check(static_cast<bool>(server))) return;
    if (!Check(static_cast<bool>(streams.Send(*server, kMessage)))) return;
    Check(!static_cast<bool>(streams.Send(*server, kMessage)));
  }
}

void StreamBackpressure_HighWaterIsAcceptedAndReadinessFiresAfterDrain() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  std::size_t ready{}, sent{};
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  if (!Check(static_cast<bool>(streams.SetOnWriteReady(*handle, [&ready]() noexcept { ++ready; }))))
    return;
  auto first = streams.Send(*handle, kMessage);
  auto second = streams.Send(*handle, kMessage);
  if (!Check(static_cast<bool>(first))) return;
  if (!Check(static_cast<bool>(second))) return;
  Check((*first) == (WriteStatus::kQueued));
  Check((*second) == (WriteStatus::kHighWaterMarkReached));
  Check((*streams.QueuedBytes(*handle)) == (8));
  auto full = streams.Send(*handle, kMessage);
  if (!Check(!static_cast<bool>(full))) return;
  Check((full.error().Code()) == (StatusCode::kResourceExhausted));
  Check((*streams.QueuedBytes(*handle)) == (8));
  auto polled = streams.PollOnce(OneFrame(), 0);
  if (!Check(static_cast<bool>(polled))) return;
  Check((polled->frames) <= (1));
  Check((sent) == (1));
  Check((ready) == (0));
  Check((*streams.QueuedBytes(*handle)) == (4));
  if (!Check(static_cast<bool>(streams.PollOnce(OneFrame(), 1)))) return;
  Check((sent) == (2));
  Check((ready) == (1));
}

void StreamBackpressure_ZeroWindowDefersDataWithoutSpinningThenWindowUpdateDrains() noexcept {
  auto limits = Limits();
  limits.initial_window = 0;
  StreamMultiplexer streams{limits};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  std::size_t sent{};
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  if (!Check(static_cast<bool>(streams.Send(*handle, kMessage)))) return;
  auto blocked = streams.PollOnce(OneFrame(), 0);
  if (!Check(static_cast<bool>(blocked))) return;
  Check((sent) == (0));
  Check((blocked->frames) == (0));
  if (!Check(static_cast<bool>(streams.UpdateWindow(*handle, kMessage.size())))) return;
  if (!Check(static_cast<bool>(streams.PollOnce(OneFrame(), 1)))) return;
  Check((sent) == (1));
  Check((*streams.QueuedBytes(*handle)) == (0));
}

void StreamBackpressure_QueueDepthBoundsControlFramesAsWellAsPayloads() noexcept {
  auto limits = Limits();
  limits.queue_depth = 1;
  StreamMultiplexer streams{limits};
  auto handle = streams.Open(RpcKind::kBidiStreaming, true);
  if (!Check(static_cast<bool>(handle))) return;
  if (!Check(static_cast<bool>(streams.SendHeaders(*handle, MetadataMap{})))) return;
  auto rejected = streams.Send(*handle, kMessage);
  if (!Check(!static_cast<bool>(rejected))) return;
  Check((rejected.error().Code()) == (StatusCode::kResourceExhausted));
  Check((*streams.QueuedBytes(*handle)) == (0));
}

void StreamFsm_InvalidDataAfterRemoteHalfCloseIncrementsDiagnosticCounter() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming, true);
  if (!Check(static_cast<bool>(handle))) return;
  if (!Check(static_cast<bool>(streams.ReceiveHalfClose(*handle)))) return;
  const auto before = streams.RejectedTransitions();
  auto rejected = streams.ReceiveMessage(*handle, kMessage);
  if (!Check(!static_cast<bool>(rejected))) return;
  Check((rejected.error().Code()) == (StatusCode::kFailedPrecondition));
  Check((streams.RejectedTransitions()) == (before + 1));
}

void StreamFsm_CancelDiscardsQueuedDataAndNotifiesExactlyOnce() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  std::size_t cancelled{}, closed{}, reset{}, messages{};
  if (!Check(static_cast<bool>(
          streams.SetOnCancel(*handle, [&cancelled]() noexcept { ++cancelled; }))))
    return;
  if (!Check(static_cast<bool>(
          streams.SetOnClose(*handle, [&closed](const Status& status, const MetadataMap&) noexcept {
            Check((status.Code()) == (StatusCode::kCancelled));
            ++closed;
          }))))
    return;
  streams.SetWireHandler(
      [&reset, &messages](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
        reset += frame.kind == StreamFrameKind::kReset;
        messages += frame.kind == StreamFrameKind::kMessage;
        return {};
      });
  if (!Check(static_cast<bool>(streams.Send(*handle, kMessage)))) return;
  if (!Check(static_cast<bool>(streams.Cancel(*handle)))) return;
  if (!Check(static_cast<bool>(streams.PollOnce(OneFrame(), 0)))) return;
  Check((cancelled) == (1));
  Check((closed) == (1));
  Check((reset) == (1));
  Check((messages) == (0));
  Check((*streams.QueuedBytes(*handle)) == (0));
}

void StreamFsm_ReusingAReleasedSlotRejectsStaleGenerationWriters() noexcept {
  auto limits = Limits();
  limits.max_streams = 1;
  StreamMultiplexer streams{limits};
  auto original = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(original))) return;
  RawClientStreamWriter stale{streams, *original};
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  if (!Check(static_cast<bool>(streams.Cancel(*original)))) return;
  if (!Check(static_cast<bool>(streams.PollOnce(OneFrame(), 0)))) return;
  if (!Check(static_cast<bool>(streams.Release(*original)))) return;
  auto replacement = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(replacement))) return;
  Check((original->index) == (replacement->index));
  Check((original->generation) != (replacement->generation));
  Check(!static_cast<bool>(stale.Send(kMessage)));
  Check((*streams.QueuedBytes(*replacement)) == (0));
  Check(static_cast<bool>(streams.Send(*replacement, kMessage)));
}

void StreamFsm_OversizedPayloadNeverChangesTheQueue() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  std::array<std::byte, 9> oversized{};
  auto rejected = streams.Send(*handle, oversized);
  if (!Check(!static_cast<bool>(rejected))) return;
  Check((rejected.error().Code()) == (StatusCode::kResourceExhausted));
  Check((*streams.QueuedBytes(*handle)) == (0));
}

void StreamWrappers_TypedCodecErrorsLeaveTheStreamWritable() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kClientStreaming);
  if (!Check(static_cast<bool>(handle))) return;
  std::array<std::byte, 1> storage{};
  ClientStreamWriter<unsigned> writer{
      RawClientStreamWriter{streams, *handle},
      [&storage](
          const unsigned& value) noexcept -> std::expected<std::span<const std::byte>, Status> {
        if (value > 255) {
          return std::unexpected(Status{StatusCode::kInvalidArgument, "value exceeds codec width"});
        }
        storage[0] = std::byte{static_cast<unsigned char>(value)};
        return std::span<const std::byte>{storage};
      }};
  auto rejected = writer.Send(256);
  if (!Check(!static_cast<bool>(rejected))) return;
  Check((rejected.error().Code()) == (StatusCode::kInvalidArgument));
  Check((*streams.QueuedBytes(*handle)) == (0));
  Check(static_cast<bool>(writer.Send(7)));
  Check((*streams.QueuedBytes(*handle)) == (1));
  Check(static_cast<bool>(writer.HalfClose()));
}

void StreamBackpressure_PollBudgetBoundsWorkAcrossMultipleReadyStreams() noexcept {
  StreamMultiplexer streams{Limits()};
  auto first = streams.Open(RpcKind::kBidiStreaming);
  auto second = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(static_cast<bool>(first))) return;
  if (!Check(static_cast<bool>(second))) return;
  if (!Check(static_cast<bool>(streams.Send(*first, kMessage)))) return;
  if (!Check(static_cast<bool>(streams.Send(*second, kMessage)))) return;
  std::size_t sent{};
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  auto first_tick = streams.PollOnce(OneFrame(), 0);
  if (!Check(static_cast<bool>(first_tick))) return;
  Check((first_tick->frames) == (1));
  Check((sent) == (1));
  auto second_tick = streams.PollOnce(OneFrame(), 1);
  if (!Check(static_cast<bool>(second_tick))) return;
  Check((second_tick->frames) == (1));
  Check((sent) == (2));
}

/** Partial transport chunks become exactly one borrowed application message. */
void StreamBackpressure_PartialWindowReassemblesOneMessage() noexcept {
  auto limits = Limits();
  limits.initial_window = 2;
  StreamMultiplexer sender{limits}, receiver{limits};
  auto left = sender.Open(RpcKind::kClientStreaming);
  auto right = receiver.Open(RpcKind::kClientStreaming, true);
  if (!Check(left.has_value() && right.has_value())) return;
  std::size_t chunks{}, messages{};
  bool first_incomplete{}, matched{};
  if (!Check(receiver
                 .SetOnMessage(*right,
                               [&messages, &matched](std::span<const std::byte> payload) noexcept {
                                 ++messages;
                                 matched = std::ranges::equal(payload, kMessage);
                               })
                 .has_value()))
    return;
  sender.SetWireHandler(
      [&receiver, &chunks, &first_incomplete, handle = *right](const StreamFrame& frame) noexcept {
        ++chunks;
        if (chunks == 1) first_incomplete = !frame.message_end && frame.payload.size() == 2;
        return receiver.Feed(handle, frame);
      });
  if (!Check(sender.Send(*left, kMessage).has_value())) return;
  if (!Check(sender.PollOnce(OneFrame(), 0).has_value())) return;
  Check(chunks == 1 && first_incomplete && messages == 0);
  Check(*sender.QueuedBytes(*left) == 2);
  if (!Check(sender.UpdateWindow(*left, 2).has_value())) return;
  if (!Check(sender.PollOnce(OneFrame(), 1).has_value())) return;
  Check(chunks == 2 && messages == 1 && matched);
  Check(*sender.QueuedBytes(*left) == 0);
}

/** A WINDOW_UPDATE can publish readiness even when there is no frame left to flush. */
void StreamBackpressure_WindowUpdateNotifiesWithoutQueuedFrames() noexcept {
  auto limits = Limits();
  limits.initial_window = 8;
  StreamMultiplexer streams{limits};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(handle.has_value())) return;
  std::size_t readiness{};
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  if (!Check(
          streams.SetOnWriteReady(*handle, [&readiness]() noexcept { ++readiness; }).has_value()))
    return;
  if (!Check(streams.Send(*handle, kMessage).has_value())) return;
  if (!Check(streams.Send(*handle, kMessage).has_value())) return;
  if (!Check(streams.PollOnce(OneFrame(), 0).has_value())) return;
  if (!Check(streams.PollOnce(OneFrame(), 1).has_value())) return;
  Check(*streams.QueuedBytes(*handle) == 0 && readiness == 0);
  if (!Check(streams.UpdateWindow(*handle, 4).has_value())) return;
  auto ready = streams.PollOnce(OneFrame(), 2);
  if (!Check(ready.has_value())) return;
  Check(ready->frames == 0 && ready->commands == 1 && readiness == 1);
  if (!Check(streams.PollOnce(OneFrame(), 3).has_value())) return;
  Check(readiness == 1);
}

/** A terminal callback cannot destroy or replace the callback object currently executing. */
void StreamFsm_CallbackMutationAndReleaseAreRejectedDuringDispatch() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(handle.has_value())) return;
  bool release_rejected{}, replacement_rejected{};
  auto installed = streams.SetOnClose(*handle, [&streams, &release_rejected, &replacement_rejected,
                                                stream = *handle](const Status&,
                                                                  const MetadataMap&) noexcept {
    auto release = streams.Release(stream);
    release_rejected = !release && release.error().Code() == StatusCode::kFailedPrecondition;
    auto replaced = streams.SetOnClose(stream, [](const Status&, const MetadataMap&) noexcept {});
    replacement_rejected = !replaced && replaced.error().Code() == StatusCode::kFailedPrecondition;
  });
  if (!Check(installed.has_value())) return;
  if (!Check(streams.ReceiveReset(*handle).has_value())) return;
  Check(release_rejected && replacement_rejected);
  Check(streams.Release(*handle).has_value());
}

/** Write readiness consumes command budget even when triggered by frame drainage. */
void StreamBackpressure_ReadinessRespectsCommandBudgetAndResumesNextTick() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(handle.has_value())) return;
  std::size_t ready{};
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  if (!Check(streams.SetOnWriteReady(*handle, [&ready]() noexcept { ++ready; }).has_value()))
    return;
  if (!Check(streams.Send(*handle, kMessage).has_value() &&
             streams.Send(*handle, kMessage).has_value()))
    return;
  PollBudget drain;
  drain.frames = 2;
  drain.commands = 0;
  auto drained = streams.PollOnce(drain, 0);
  if (!Check(drained.has_value())) return;
  Check(drained->frames == 2 && drained->commands == 0 && drained->pending && ready == 0);
  Check(*streams.QueuedBytes(*handle) == 0);
  PollBudget resume;
  resume.frames = 1;
  resume.commands = 1;
  auto resumed = streams.PollOnce(resume, 1);
  if (!Check(resumed.has_value())) return;
  Check(resumed->frames == 0 && resumed->commands == 1 && ready == 1);
  auto repeated = streams.PollOnce(resume, 2);
  if (!Check(repeated.has_value())) return;
  Check(repeated->commands == 0 && ready == 1);
}

/** A zero low-water mark cannot provide a strict below-threshold recovery event. */
void StreamBackpressure_ZeroLowWaterMarkFailsClosed() noexcept {
  auto options = Limits();
  options.low_water_bytes = 0;
  StreamMultiplexer streams{options};
  auto opened = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(!opened.has_value())) return;
  Check(opened.error().Code() == StatusCode::kResourceExhausted);
}

/** Peer cancellation still wins while a logically closed response remains queued. */
void StreamFsm_ResetDiscardsCompletedButUnflushedResponse() noexcept {
  for (const bool peer_reset : {false, true}) {
    StreamMultiplexer streams{Limits()};
    auto handle = streams.Open(RpcKind::kBidiStreaming, true);
    if (!Check(handle.has_value())) return;
    std::size_t messages{}, resets{}, cancellations{}, closes{};
    streams.SetWireHandler(
        [&messages, &resets](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
          messages += frame.kind == StreamFrameKind::kMessage;
          resets += frame.kind == StreamFrameKind::kReset;
          return {};
        });
    if (!Check(streams.SetOnCancel(*handle, [&cancellations]() noexcept { ++cancellations; })
                   .has_value()))
      return;
    if (!Check(streams
                   .SetOnClose(*handle,
                               [&closes](const Status& status, const MetadataMap&) noexcept {
                                 Check(status.Code() == StatusCode::kCancelled);
                                 ++closes;
                               })
                   .has_value()))
      return;
    if (!Check(streams.ReceiveHalfClose(*handle).has_value() &&
               streams.Send(*handle, kMessage).has_value() &&
               streams.Finish(*handle, Status{}).has_value()))
      return;
    Check(*streams.State(*handle) == StreamState::kClosed);
    auto cancelled = peer_reset ? streams.ReceiveReset(*handle) : streams.Cancel(*handle);
    if (!Check(cancelled.has_value())) return;
    if (!Check(streams.PollOnce(PollBudget{}, 0).has_value())) return;
    Check(messages == 0 && resets == (peer_reset ? 0U : 1U));
    Check(cancellations == 1 && closes == 1 && *streams.QueuedBytes(*handle) == 0);
    Check(streams.Release(*handle).has_value());
  }
}

/** Even an empty fragment starts a message that END_STREAM must not truncate. */
void StreamFsm_EmptyPartialMessageCannotBeSilentlyHalfClosed() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kClientStreaming, true);
  if (!Check(handle.has_value())) return;
  std::size_t messages{}, half_closes{};
  if (!Check(streams
                 .SetOnMessage(*handle,
                               [&messages](std::span<const std::byte> payload) noexcept {
                                 Check(payload.empty());
                                 ++messages;
                               })
                 .has_value()))
    return;
  if (!Check(streams.SetOnHalfClose(*handle, [&half_closes]() noexcept { ++half_closes; })
                 .has_value()))
    return;
  const StreamFrame fragment{.kind = StreamFrameKind::kMessage, .message_end = false};
  if (!Check(streams.Feed(*handle, fragment).has_value())) return;
  const auto before = streams.RejectedTransitions();
  auto rejected = streams.ReceiveHalfClose(*handle);
  if (!Check(!rejected.has_value())) return;
  Check(rejected.error().Code() == StatusCode::kFailedPrecondition);
  Check(streams.RejectedTransitions() == before + 1 && half_closes == 0 && messages == 0);
  Check(*streams.State(*handle) == StreamState::kOpen);
  if (!Check(streams.ReceiveMessage(*handle, {}).has_value())) return;
  if (!Check(streams.ReceiveHalfClose(*handle).has_value())) return;
  Check(messages == 1 && half_closes == 1);
}

/** Rejected transport writes retain their exact payload, queue order and window credit. */
void StreamBackpressure_RejectedWireWriteRetriesWithoutDroppingOrDuplicatingFrames() noexcept {
  auto options = Limits();
  options.initial_window = kMessage.size();
  StreamMultiplexer streams{options};
  auto handle = streams.Open(RpcKind::kClientStreaming);
  if (!Check(handle.has_value())) return;
  std::size_t attempts{}, messages{}, half_closes{};
  streams.SetWireHandler([&attempts, &messages, &half_closes](
                             const StreamFrame& frame) noexcept -> std::expected<void, Status> {
    ++attempts;
    if (attempts == 1) return std::unexpected(Status{StatusCode::kUnavailable});
    if (frame.kind == StreamFrameKind::kMessage) {
      Check(std::ranges::equal(frame.payload, kMessage));
      Check(frame.message_end);
      ++messages;
    }
    if (frame.kind == StreamFrameKind::kHalfClose) {
      Check(messages == 1 && frame.end_stream);
      ++half_closes;
    }
    return {};
  });
  if (!Check(streams.Send(*handle, kMessage).has_value() && streams.HalfClose(*handle).has_value()))
    return;
  auto failed = streams.PollOnce(OneFrame(), 0);
  if (!Check(!failed.has_value())) return;
  Check(failed.error().Code() == StatusCode::kUnavailable);
  Check(*streams.QueuedBytes(*handle) == kMessage.size() && messages == 0);
  if (!Check(streams.PollOnce(OneFrame(), 1).has_value() &&
             streams.PollOnce(OneFrame(), 2).has_value()))
    return;
  Check(attempts == 3 && messages == 1 && half_closes == 1);
  Check(*streams.QueuedBytes(*handle) == 0);
}

/** Duplicate peer control events are rejected without repeating user notifications. */
void StreamFsm_DuplicatePeerHeadersTrailersAndResetAreCounted() noexcept {
  StreamMultiplexer streams{Limits()};
  auto handle = streams.Open(RpcKind::kBidiStreaming);
  if (!Check(handle.has_value())) return;
  std::size_t headers{}, closes{}, cancellations{};
  if (!Check(streams.SetOnHeaders(*handle, [&headers](const MetadataMap&) noexcept { ++headers; })
                 .has_value()))
    return;
  if (!Check(streams
                 .SetOnClose(*handle,
                             [&closes](const Status&, const MetadataMap&) noexcept { ++closes; })
                 .has_value()))
    return;
  if (!Check(streams.SetOnCancel(*handle, [&cancellations]() noexcept { ++cancellations; })
                 .has_value()))
    return;
  const auto before = streams.RejectedTransitions();
  if (!Check(streams.ReceiveHeaders(*handle, MetadataMap{}).has_value())) return;
  Check(!streams.ReceiveHeaders(*handle, MetadataMap{}).has_value());
  if (!Check(streams.ReceiveTrailers(*handle, Status{}, MetadataMap{}).has_value())) return;
  Check(!streams.ReceiveTrailers(*handle, Status{}, MetadataMap{}).has_value());
  if (!Check(streams.ReceiveReset(*handle).has_value())) return;
  Check(!streams.ReceiveReset(*handle).has_value());
  Check(streams.RejectedTransitions() == before + 3);
  Check(headers == 1 && closes == 1 && cancellations == 1);
}

}  // namespace
}  // namespace grpcxx

int main() {
  grpcxx::StreamBackpressure_ReadinessRespectsCommandBudgetAndResumesNextTick();
  grpcxx::StreamBackpressure_ZeroLowWaterMarkFailsClosed();
  grpcxx::StreamFsm_ResetDiscardsCompletedButUnflushedResponse();
  grpcxx::StreamFsm_EmptyPartialMessageCannotBeSilentlyHalfClosed();
  grpcxx::StreamBackpressure_RejectedWireWriteRetriesWithoutDroppingOrDuplicatingFrames();
  grpcxx::StreamFsm_DuplicatePeerHeadersTrailersAndResetAreCounted();
  grpcxx::StreamBackpressure_PartialWindowReassemblesOneMessage();
  grpcxx::StreamBackpressure_WindowUpdateNotifiesWithoutQueuedFrames();
  grpcxx::StreamFsm_CallbackMutationAndReleaseAreRejectedDuringDispatch();
  grpcxx::StreamFsm_HalfCloseKeepsResponseDirectionOpenAndRejectsFurtherData();
  grpcxx::StreamFsm_SingleMessageDirectionsEnforceCardinality();
  grpcxx::StreamBackpressure_HighWaterIsAcceptedAndReadinessFiresAfterDrain();
  grpcxx::StreamBackpressure_ZeroWindowDefersDataWithoutSpinningThenWindowUpdateDrains();
  grpcxx::StreamBackpressure_QueueDepthBoundsControlFramesAsWellAsPayloads();
  grpcxx::StreamFsm_InvalidDataAfterRemoteHalfCloseIncrementsDiagnosticCounter();
  grpcxx::StreamFsm_CancelDiscardsQueuedDataAndNotifiesExactlyOnce();
  grpcxx::StreamFsm_ReusingAReleasedSlotRejectsStaleGenerationWriters();
  grpcxx::StreamFsm_OversizedPayloadNeverChangesTheQueue();
  grpcxx::StreamWrappers_TypedCodecErrorsLeaveTheStreamWritable();
  grpcxx::StreamBackpressure_PollBudgetBoundsWorkAcrossMultipleReadyStreams();
  fmt::print("Streaming FSM Seam 2: 20 cases, {} failed (in-memory events; no sockets)\n",
             grpcxx::failures);
  return grpcxx::failures == 0 ? 0 : 1;
}
