/** @file Bounded streaming state machine, cardinality, and generational handle tests. */
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdlib>
#include <span>
#include <uarch/uarch.hpp>

#include "grpcxx/streaming/client_stream_writer.h"
#include "grpcxx/streaming/server_stream_responder.h"

// Host sysroot compatibility shims for GTest built against newer glibc headers.
extern "C" long __isoc23_strtol(const char* text, char** end, int base) {
  return std::strtol(text, end, base);
}

extern "C" unsigned long long __isoc23_strtoull(const char* text, char** end, int base) {
  return std::strtoull(text, end, base);
}

namespace grpcxx {
namespace {

constexpr std::array<std::byte, 4> kMessage{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

static_assert(uarch::NoPadding<StreamId> && uarch::RegisterPassable<StreamId> &&
              uarch::RegisterLeaf<StreamId>);
static_assert(uarch::NoPadding<StreamMultiplexerOptions> &&
              uarch::RegisterPassable<StreamMultiplexerOptions> &&
              uarch::GprPair<StreamMultiplexerOptions>);

[[nodiscard]] StreamMultiplexer::Options Limits() noexcept {
  return {.max_streams = 2,
          .queue_depth = 4,
          .max_message_bytes = 8,
          .high_water_bytes = 8,
          .low_water_bytes = 4,
          .initial_window = 64};
}

[[nodiscard]] PollBudget OneFrame() noexcept {
  PollBudget budget{};
  budget.frames = 1U;
  return budget;
}

TEST(StreamLayout, RegisterIdentityAndOptionsGprPair) {
  constexpr auto kId = StreamId::Pack(7U, 3U);
  EXPECT_EQ(kId.value, 0x0000'0003'0000'0007ULL);
  EXPECT_EQ(kId.Index(), 7U);
  EXPECT_EQ(kId.Generation(), 3U);
  const StreamHandle handle = kId.Handle();
  EXPECT_EQ(handle.index, 7U);
  EXPECT_EQ(handle.generation, 3U);

  constexpr StreamMultiplexerOptions kLimits{64U, 16U, 65535U};
  EXPECT_EQ(sizeof(StreamMultiplexerOptions), 16U);
  EXPECT_EQ(kLimits.max_streams, 64U);
  EXPECT_EQ(kLimits.queue_depth, 16U);
  EXPECT_EQ(kLimits.initial_window, 65535U);
}

TEST(StreamFsm, HalfCloseKeepsResponseOpen) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kClientStreaming);
  ASSERT_TRUE(handle.has_value());
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.HalfClose(*handle).has_value());
  EXPECT_EQ(*streams.State(*handle), StreamState::kLocalHalfClosed);

  const auto rejected = streams.Send(*handle, kMessage);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kFailedPrecondition);

  std::size_t responses = 0U;
  ASSERT_TRUE(
      streams
          .SetOnMessage(*handle, [&responses](std::span<const std::byte>) noexcept { ++responses; })
          .has_value());
  ASSERT_TRUE(streams.ReceiveMessage(*handle, kMessage).has_value());
  EXPECT_EQ(responses, 1U);
  ASSERT_TRUE(streams.ReceiveTrailers(*handle, Status{}, MetadataMap{}).has_value());
  EXPECT_EQ(*streams.State(*handle), StreamState::kClosed);
}

TEST(StreamFsm, SingleMessageEnforcesCardinality) {
  for (const auto kind : {RpcKind::kUnary, RpcKind::kServerStreaming}) {
    StreamMultiplexer streams{Limits()};
    const auto client = streams.Open(kind);
    ASSERT_TRUE(client.has_value());
    ASSERT_TRUE(streams.Send(*client, kMessage).has_value());
    const auto second = streams.Send(*client, kMessage);
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error().Code(), StatusCode::kFailedPrecondition);
  }
  for (const auto kind : {RpcKind::kUnary, RpcKind::kClientStreaming}) {
    StreamMultiplexer streams{Limits()};
    const auto server = streams.Open(kind, true);
    ASSERT_TRUE(server.has_value());
    ASSERT_TRUE(streams.Send(*server, kMessage).has_value());
    EXPECT_FALSE(streams.Send(*server, kMessage).has_value());
  }
}

TEST(StreamFsm, DataAfterRemoteHalfCloseCounted) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming, true);
  ASSERT_TRUE(handle.has_value());
  ASSERT_TRUE(streams.ReceiveHalfClose(*handle).has_value());
  const auto before = streams.RejectedTransitions();
  const auto rejected = streams.ReceiveMessage(*handle, kMessage);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_EQ(streams.RejectedTransitions(), before + 1U);
}

TEST(StreamFsm, CancelDiscardsQueueAndNotifiesOnce) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::size_t cancelled = 0U;
  std::size_t closed = 0U;
  std::size_t reset = 0U;
  std::size_t messages = 0U;
  ASSERT_TRUE(streams.SetOnCancel(*handle, [&cancelled]() noexcept { ++cancelled; }).has_value());
  ASSERT_TRUE(streams
                  .SetOnClose(*handle,
                              [&closed](const Status& status, const MetadataMap&) noexcept {
                                EXPECT_EQ(status.Code(), StatusCode::kCancelled);
                                ++closed;
                              })
                  .has_value());
  streams.SetWireHandler(
      [&reset, &messages](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
        reset += static_cast<std::size_t>(frame.kind == StreamFrameKind::kReset);
        messages += static_cast<std::size_t>(frame.kind == StreamFrameKind::kMessage);
        return {};
      });
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.Cancel(*handle).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 0U).has_value());
  EXPECT_EQ(cancelled, 1U);
  EXPECT_EQ(closed, 1U);
  EXPECT_EQ(reset, 1U);
  EXPECT_EQ(messages, 0U);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
}

TEST(StreamFsm, ReusedSlotRejectsStaleGeneration) {
  auto limits = Limits();
  limits.max_streams = 1U;
  StreamMultiplexer streams{limits};
  const auto original = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(original.has_value());
  RawClientStreamWriter stale{streams, *original};
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  ASSERT_TRUE(streams.Cancel(*original).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 0U).has_value());
  ASSERT_TRUE(streams.Release(*original).has_value());

  const auto replacement = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(replacement.has_value());
  EXPECT_EQ(original->index, replacement->index);
  EXPECT_NE(original->generation, replacement->generation);
  EXPECT_FALSE(stale.Send(kMessage).has_value());
  EXPECT_EQ(*streams.QueuedBytes(*replacement), 0U);
  EXPECT_TRUE(streams.Send(*replacement, kMessage).has_value());
}

TEST(StreamFsm, OversizedPayloadLeavesQueueUntouched) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::array<std::byte, 9> oversized{};
  const auto rejected = streams.Send(*handle, oversized);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
}

TEST(StreamFsm, TypedCodecErrorKeepsStreamWritable) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kClientStreaming);
  ASSERT_TRUE(handle.has_value());
  std::array<std::byte, 1> storage{};
  ClientStreamWriter<unsigned> writer{
      RawClientStreamWriter{streams, *handle},
      [&storage](
          const unsigned& value) noexcept -> std::expected<std::span<const std::byte>, Status> {
        if (value > 255U) {
          return std::unexpected(Status{StatusCode::kInvalidArgument, "value exceeds codec width"});
        }
        storage[0] = std::byte{static_cast<unsigned char>(value)};
        return std::span<const std::byte>{storage};
      }};
  const auto rejected = writer.Send(256U);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
  ASSERT_TRUE(writer.Send(7U).has_value());
  EXPECT_EQ(*streams.QueuedBytes(*handle), 1U);
  EXPECT_TRUE(writer.HalfClose().has_value());
}

TEST(StreamFsm, CallbackMutationRejectedInFlight) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  bool release_rejected = false;
  bool replacement_rejected = false;
  ASSERT_TRUE(streams
                  .SetOnClose(*handle,
                              [&streams, &release_rejected, &replacement_rejected,
                               stream = *handle](const Status&, const MetadataMap&) noexcept {
                                const auto rel = streams.Release(stream);
                                release_rejected =
                                    !rel && rel.error().Code() == StatusCode::kFailedPrecondition;
                                const auto rep = streams.SetOnClose(
                                    stream, [](const Status&, const MetadataMap&) noexcept {});
                                replacement_rejected =
                                    !rep && rep.error().Code() == StatusCode::kFailedPrecondition;
                              })
                  .has_value());
  ASSERT_TRUE(streams.ReceiveReset(*handle).has_value());
  EXPECT_TRUE(release_rejected);
  EXPECT_TRUE(replacement_rejected);
  EXPECT_TRUE(streams.Release(*handle).has_value());
}

TEST(StreamFsm, ResetDiscardsUnflushedClosedResponse) {
  for (const bool peer_reset : {false, true}) {
    StreamMultiplexer streams{Limits()};
    const auto handle = streams.Open(RpcKind::kBidiStreaming, true);
    ASSERT_TRUE(handle.has_value());
    std::size_t messages = 0U;
    std::size_t resets = 0U;
    std::size_t cancellations = 0U;
    std::size_t closes = 0U;
    streams.SetWireHandler(
        [&messages, &resets](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
          messages += static_cast<std::size_t>(frame.kind == StreamFrameKind::kMessage);
          resets += static_cast<std::size_t>(frame.kind == StreamFrameKind::kReset);
          return {};
        });
    ASSERT_TRUE(
        streams.SetOnCancel(*handle, [&cancellations]() noexcept { ++cancellations; }).has_value());
    ASSERT_TRUE(streams
                    .SetOnClose(*handle,
                                [&closes](const Status& status, const MetadataMap&) noexcept {
                                  EXPECT_EQ(status.Code(), StatusCode::kCancelled);
                                  ++closes;
                                })
                    .has_value());
    ASSERT_TRUE(streams.ReceiveHalfClose(*handle).has_value());
    ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
    ASSERT_TRUE(streams.Finish(*handle, Status{}).has_value());
    EXPECT_EQ(*streams.State(*handle), StreamState::kClosed);

    const auto cancelled = peer_reset ? streams.ReceiveReset(*handle) : streams.Cancel(*handle);
    ASSERT_TRUE(cancelled.has_value());
    ASSERT_TRUE(streams.PollOnce(PollBudget{}, 0U).has_value());
    EXPECT_EQ(messages, 0U);
    EXPECT_EQ(resets, peer_reset ? 0U : 1U);
    EXPECT_EQ(cancellations, 1U);
    EXPECT_EQ(closes, 1U);
    EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
    EXPECT_TRUE(streams.Release(*handle).has_value());
  }
}

TEST(StreamFsm, EmptyFragmentBlocksSilentHalfClose) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kClientStreaming, true);
  ASSERT_TRUE(handle.has_value());
  std::size_t messages = 0U;
  std::size_t half_closes = 0U;
  ASSERT_TRUE(streams
                  .SetOnMessage(*handle,
                                [&messages](std::span<const std::byte> payload) noexcept {
                                  EXPECT_TRUE(payload.empty());
                                  ++messages;
                                })
                  .has_value());
  ASSERT_TRUE(
      streams.SetOnHalfClose(*handle, [&half_closes]() noexcept { ++half_closes; }).has_value());

  const StreamFrame fragment{.kind = StreamFrameKind::kMessage, .message_end = false};
  ASSERT_TRUE(streams.Feed(*handle, fragment).has_value());
  const auto before = streams.RejectedTransitions();
  const auto rejected = streams.ReceiveHalfClose(*handle);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_EQ(streams.RejectedTransitions(), before + 1U);
  EXPECT_EQ(half_closes, 0U);
  EXPECT_EQ(messages, 0U);
  EXPECT_EQ(*streams.State(*handle), StreamState::kOpen);

  ASSERT_TRUE(streams.ReceiveMessage(*handle, {}).has_value());
  ASSERT_TRUE(streams.ReceiveHalfClose(*handle).has_value());
  EXPECT_EQ(messages, 1U);
  EXPECT_EQ(half_closes, 1U);
}

TEST(StreamFsm, DuplicatePeerControlEventsCounted) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::size_t headers = 0U;
  std::size_t closes = 0U;
  std::size_t cancellations = 0U;
  ASSERT_TRUE(streams.SetOnHeaders(*handle, [&headers](const MetadataMap&) noexcept { ++headers; })
                  .has_value());
  ASSERT_TRUE(
      streams
          .SetOnClose(*handle, [&closes](const Status&, const MetadataMap&) noexcept { ++closes; })
          .has_value());
  ASSERT_TRUE(
      streams.SetOnCancel(*handle, [&cancellations]() noexcept { ++cancellations; }).has_value());

  const auto before = streams.RejectedTransitions();
  ASSERT_TRUE(streams.ReceiveHeaders(*handle, MetadataMap{}).has_value());
  EXPECT_FALSE(streams.ReceiveHeaders(*handle, MetadataMap{}).has_value());
  ASSERT_TRUE(streams.ReceiveTrailers(*handle, Status{}, MetadataMap{}).has_value());
  EXPECT_FALSE(streams.ReceiveTrailers(*handle, Status{}, MetadataMap{}).has_value());
  ASSERT_TRUE(streams.ReceiveReset(*handle).has_value());
  EXPECT_FALSE(streams.ReceiveReset(*handle).has_value());
  EXPECT_EQ(streams.RejectedTransitions(), before + 3U);
  EXPECT_EQ(headers, 1U);
  EXPECT_EQ(closes, 1U);
  EXPECT_EQ(cancellations, 1U);
}

}  // namespace
}  // namespace grpcxx
