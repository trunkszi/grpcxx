/** @file Bounded streaming flow-control, high/low watermarks, and scheduling tests. */
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ranges>
#include <span>
#include <vector>

#include "grpcxx/streaming/stream_multiplexer.h"

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

TEST(StreamBackpressure, DrainNotifiesWriteReady) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::size_t ready = 0U;
  std::size_t sent = 0U;
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  ASSERT_TRUE(streams.SetOnWriteReady(*handle, [&ready]() noexcept { ++ready; }).has_value());

  const auto first = streams.Send(*handle, kMessage);
  const auto second = streams.Send(*handle, kMessage);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*first, WriteStatus::kQueued);
  EXPECT_EQ(*second, WriteStatus::kHighWaterMarkReached);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 8U);

  const auto full = streams.Send(*handle, kMessage);
  ASSERT_FALSE(full.has_value());
  EXPECT_EQ(full.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 8U);

  const auto polled = streams.PollOnce(OneFrame(), 0U);
  ASSERT_TRUE(polled.has_value());
  EXPECT_LE(polled->frames, 1U);
  EXPECT_EQ(sent, 1U);
  EXPECT_EQ(ready, 0U);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 4U);

  ASSERT_TRUE(streams.PollOnce(OneFrame(), 1U).has_value());
  EXPECT_EQ(sent, 2U);
  EXPECT_EQ(ready, 1U);
}

TEST(StreamBackpressure, ZeroWindowDefersUntilWindowUpdate) {
  auto limits = Limits();
  limits.initial_window = 0U;
  StreamMultiplexer streams{limits};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::size_t sent = 0U;
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());

  const auto blocked = streams.PollOnce(OneFrame(), 0U);
  ASSERT_TRUE(blocked.has_value());
  EXPECT_EQ(sent, 0U);
  EXPECT_EQ(blocked->frames, 0U);

  ASSERT_TRUE(streams.UpdateWindow(*handle, kMessage.size()).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 1U).has_value());
  EXPECT_EQ(sent, 1U);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
}

TEST(StreamBackpressure, QueueBoundsControlAndData) {
  auto limits = Limits();
  limits.queue_depth = 1U;
  StreamMultiplexer streams{limits};
  const auto handle = streams.Open(RpcKind::kBidiStreaming, true);
  ASSERT_TRUE(handle.has_value());
  ASSERT_TRUE(streams.SendHeaders(*handle, MetadataMap{}).has_value());
  const auto rejected = streams.Send(*handle, kMessage);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
}

TEST(StreamBackpressure, PollBudgetBoundsReadyStreams) {
  StreamMultiplexer streams{Limits()};
  const auto first = streams.Open(RpcKind::kBidiStreaming);
  const auto second = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(streams.Send(*first, kMessage).has_value());
  ASSERT_TRUE(streams.Send(*second, kMessage).has_value());

  std::size_t sent = 0U;
  streams.SetWireHandler([&sent](const StreamFrame&) noexcept -> std::expected<void, Status> {
    ++sent;
    return {};
  });
  const auto first_tick = streams.PollOnce(OneFrame(), 0U);
  ASSERT_TRUE(first_tick.has_value());
  EXPECT_EQ(first_tick->frames, 1U);
  EXPECT_EQ(sent, 1U);

  const auto second_tick = streams.PollOnce(OneFrame(), 1U);
  ASSERT_TRUE(second_tick.has_value());
  EXPECT_EQ(second_tick->frames, 1U);
  EXPECT_EQ(sent, 2U);
}

TEST(StreamBackpressure, PartialWindowReassemblesMessage) {
  auto limits = Limits();
  limits.initial_window = 2U;
  StreamMultiplexer sender{limits};
  StreamMultiplexer receiver{limits};
  const auto left = sender.Open(RpcKind::kClientStreaming);
  const auto right = receiver.Open(RpcKind::kClientStreaming, true);
  ASSERT_TRUE(left.has_value());
  ASSERT_TRUE(right.has_value());

  std::size_t chunks = 0U;
  std::size_t messages = 0U;
  bool first_incomplete = false;
  bool matched = false;
  ASSERT_TRUE(receiver
                  .SetOnMessage(*right,
                                [&messages, &matched](std::span<const std::byte> payload) noexcept {
                                  ++messages;
                                  matched = std::ranges::equal(payload, kMessage);
                                })
                  .has_value());
  sender.SetWireHandler(
      [&receiver, &chunks, &first_incomplete, handle = *right](const StreamFrame& frame) noexcept {
        ++chunks;
        if (chunks == 1U) {
          first_incomplete = !frame.message_end && frame.payload.size() == 2U;
        }
        return receiver.Feed(handle, frame);
      });

  ASSERT_TRUE(sender.Send(*left, kMessage).has_value());
  ASSERT_TRUE(sender.PollOnce(OneFrame(), 0U).has_value());
  EXPECT_EQ(chunks, 1U);
  EXPECT_TRUE(first_incomplete);
  EXPECT_EQ(messages, 0U);
  EXPECT_EQ(*sender.QueuedBytes(*left), 2U);

  ASSERT_TRUE(sender.UpdateWindow(*left, 2U).has_value());
  ASSERT_TRUE(sender.PollOnce(OneFrame(), 1U).has_value());
  EXPECT_EQ(chunks, 2U);
  EXPECT_EQ(messages, 1U);
  EXPECT_TRUE(matched);
  EXPECT_EQ(*sender.QueuedBytes(*left), 0U);
}

TEST(StreamBackpressure, WindowUpdateNotifiesWithoutQueuedFrames) {
  auto limits = Limits();
  limits.initial_window = 8U;
  StreamMultiplexer streams{limits};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());

  std::size_t readiness = 0U;
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  ASSERT_TRUE(
      streams.SetOnWriteReady(*handle, [&readiness]() noexcept { ++readiness; }).has_value());
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 0U).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 1U).has_value());
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
  EXPECT_EQ(readiness, 0U);

  ASSERT_TRUE(streams.UpdateWindow(*handle, 4U).has_value());
  const auto ready = streams.PollOnce(OneFrame(), 2U);
  ASSERT_TRUE(ready.has_value());
  EXPECT_EQ(ready->frames, 0U);
  EXPECT_EQ(ready->commands, 1U);
  EXPECT_EQ(readiness, 1U);

  ASSERT_TRUE(streams.PollOnce(OneFrame(), 3U).has_value());
  EXPECT_EQ(readiness, 1U);
}

TEST(StreamBackpressure, ReadinessRespectsCommandBudget) {
  StreamMultiplexer streams{Limits()};
  const auto handle = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_TRUE(handle.has_value());
  std::size_t ready = 0U;
  streams.SetWireHandler(
      [](const StreamFrame&) noexcept -> std::expected<void, Status> { return {}; });
  ASSERT_TRUE(streams.SetOnWriteReady(*handle, [&ready]() noexcept { ++ready; }).has_value());
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());

  PollBudget drain{};
  drain.frames = 2U;
  drain.commands = 0U;
  const auto drained = streams.PollOnce(drain, 0U);
  ASSERT_TRUE(drained.has_value());
  EXPECT_EQ(drained->frames, 2U);
  EXPECT_EQ(drained->commands, 0U);
  EXPECT_TRUE(drained->pending);
  EXPECT_EQ(ready, 0U);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);

  PollBudget resume{};
  resume.frames = 1U;
  resume.commands = 1U;
  const auto resumed = streams.PollOnce(resume, 1U);
  ASSERT_TRUE(resumed.has_value());
  EXPECT_EQ(resumed->frames, 0U);
  EXPECT_EQ(resumed->commands, 1U);
  EXPECT_EQ(ready, 1U);

  const auto repeated = streams.PollOnce(resume, 2U);
  ASSERT_TRUE(repeated.has_value());
  EXPECT_EQ(repeated->commands, 0U);
  EXPECT_EQ(ready, 1U);
}

TEST(StreamBackpressure, ZeroLowWaterMarkFailsClosed) {
  auto options = Limits();
  options.low_water_bytes = 0U;
  StreamMultiplexer streams{options};
  const auto opened = streams.Open(RpcKind::kBidiStreaming);
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().Code(), StatusCode::kResourceExhausted);
}

TEST(StreamBackpressure, RejectedWireWriteRetriesWithoutLoss) {
  auto options = Limits();
  options.initial_window = kMessage.size();
  StreamMultiplexer streams{options};
  const auto handle = streams.Open(RpcKind::kClientStreaming);
  ASSERT_TRUE(handle.has_value());

  std::size_t attempts = 0U;
  std::size_t messages = 0U;
  std::size_t half_closes = 0U;
  streams.SetWireHandler([&attempts, &messages, &half_closes](
                             const StreamFrame& frame) noexcept -> std::expected<void, Status> {
    ++attempts;
    if (attempts == 1U) {
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
    if (frame.kind == StreamFrameKind::kMessage) {
      EXPECT_TRUE(std::ranges::equal(frame.payload, kMessage));
      EXPECT_TRUE(frame.message_end);
      ++messages;
    }
    if (frame.kind == StreamFrameKind::kHalfClose) {
      EXPECT_EQ(messages, 1U);
      EXPECT_TRUE(frame.end_stream);
      ++half_closes;
    }
    return {};
  });
  ASSERT_TRUE(streams.Send(*handle, kMessage).has_value());
  ASSERT_TRUE(streams.HalfClose(*handle).has_value());

  const auto failed = streams.PollOnce(OneFrame(), 0U);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().Code(), StatusCode::kUnavailable);
  EXPECT_EQ(*streams.QueuedBytes(*handle), kMessage.size());
  EXPECT_EQ(messages, 0U);

  ASSERT_TRUE(streams.PollOnce(OneFrame(), 1U).has_value());
  ASSERT_TRUE(streams.PollOnce(OneFrame(), 2U).has_value());
  EXPECT_EQ(attempts, 3U);
  EXPECT_EQ(messages, 1U);
  EXPECT_EQ(half_closes, 1U);
  EXPECT_EQ(*streams.QueuedBytes(*handle), 0U);
}

TEST(StreamBackpressure, BitmapAndDivisorWrap) {
  const StreamMultiplexer::Options opts{
      .max_streams = 67U,
      .queue_depth = 5U,
      .max_message_bytes = 8U,
      .high_water_bytes = 24U,
      .low_water_bytes = 8U,
      .initial_window = 64U,
  };
  StreamMultiplexer streams{opts};
  ASSERT_TRUE(streams.Ready());

  std::size_t delivered = 0U;
  streams.SetWireHandler(
      [&delivered](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
        if (frame.kind == StreamFrameKind::kMessage && frame.message_end) {
          ++delivered;
        }
        return {};
      });

  std::array<StreamHandle, 67> handles{};
  for (std::size_t i = 0U; i < handles.size(); ++i) {
    const auto opened = streams.Open(RpcKind::kBidiStreaming);
    ASSERT_TRUE(opened.has_value());
    handles[i] = *opened;
  }
  EXPECT_FALSE(streams.Open(RpcKind::kBidiStreaming).has_value());

  for (std::uint64_t cycle = 0U; cycle < 3U; ++cycle) {
    for (const auto& h : handles) {
      ASSERT_TRUE(streams.Send(h, kMessage).has_value());
    }
    PollBudget budget{};
    budget.frames = 128U;
    ASSERT_TRUE(streams.PollOnce(budget, cycle).has_value());
  }
  EXPECT_EQ(delivered, 67U * 3U);

  for (std::size_t i = 0U; i < handles.size(); ++i) {
    if (i == 1U || i == 65U) continue;
    ASSERT_TRUE(streams.Cancel(handles[i]).has_value());
    PollBudget drain_budget{};
    drain_budget.frames = 4U;
    ASSERT_TRUE(streams.PollOnce(drain_budget, MonotonicTimestamp{10}).has_value());
    ASSERT_TRUE(streams.Release(handles[i]).has_value());
  }

  std::vector<std::uint32_t> indexes;
  streams.SetWireHandler(
      [&indexes](const StreamFrame& frame) noexcept -> std::expected<void, Status> {
        if (frame.kind == StreamFrameKind::kMessage && frame.message_end) {
          indexes.push_back(frame.stream.index);
        }
        return {};
      });
  for (int step = 0; step < 2; ++step) {
    ASSERT_TRUE(streams.Send(handles[1], kMessage).has_value());
    ASSERT_TRUE(streams.Send(handles[65], kMessage).has_value());
  }
  for (std::uint64_t step = 0U; step < 4U; ++step) {
    const auto res = streams.PollOnce(OneFrame(), 20U + step);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->frames, 1U);
  }
  ASSERT_EQ(indexes.size(), 4U);
  EXPECT_NE(indexes[0], indexes[1]);
  EXPECT_EQ(indexes[0], indexes[2]);
  EXPECT_EQ(indexes[1], indexes[3]);
}

}  // namespace
}  // namespace grpcxx
