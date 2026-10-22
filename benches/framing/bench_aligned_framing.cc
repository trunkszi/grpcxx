#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <span>
#include <vector>

#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"

using grpcxx::AlignedMessageBuf;
using grpcxx::LengthPrefixedFrame;

namespace {

constexpr std::size_t kMaxReceiveMessageSize = 4U * 1024U * 1024U;
constexpr std::size_t kFrameCount = 16U;
constexpr std::size_t kPayloadSize = 1024U;

/** Builds `frame_count` gRPC wire frames with deterministic payload bytes. */
[[nodiscard]] auto BuildWire(std::size_t frame_count,
                             std::size_t payload_size) -> std::vector<std::byte> {
  std::vector<std::byte> wire(frame_count * (AlignedMessageBuf::kGrpcHeaderBytes + payload_size));
  std::size_t offset = 0U;
  for (const auto frame_index : std::views::iota(std::size_t{0U}, frame_count)) {
    wire[offset] = std::byte{0x00};
    const auto length = static_cast<std::uint32_t>(payload_size);
    wire[offset + 1U] = static_cast<std::byte>((length >> 24U) & 0xFFU);
    wire[offset + 2U] = static_cast<std::byte>((length >> 16U) & 0xFFU);
    wire[offset + 3U] = static_cast<std::byte>((length >> 8U) & 0xFFU);
    wire[offset + 4U] = static_cast<std::byte>(length & 0xFFU);
    for (const auto byte_index : std::views::iota(std::size_t{0U}, payload_size)) {
      wire[offset + AlignedMessageBuf::kGrpcHeaderBytes + byte_index] =
          static_cast<std::byte>((frame_index + byte_index) & 0xFFU);
    }
    offset += AlignedMessageBuf::kGrpcHeaderBytes + payload_size;
  }
  return wire;
}

}  // namespace

/** Measures eight-byte-aligned frame construction for several payload sizes. */
void BM_AlignedMessageBuf_Construct(benchmark::State& state) {
  const auto payload_size = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    AlignedMessageBuf frame(payload_size);
    frame.SetCompressed(true);
    benchmark::DoNotOptimize(frame.PayloadAddress());
    benchmark::DoNotOptimize(frame.WireBytes().data());
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(payload_size));
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()));
}

BENCHMARK(BM_AlignedMessageBuf_Construct)->Arg(64)->Arg(1024)->Arg(65536)->Arg(4 * 1024 * 1024);

/** Measures incremental parsing when each chunk covers part of a frame. */
void BM_LengthPrefixedFrame_Fragmented(benchmark::State& state) {
  const auto chunk_bytes = static_cast<std::size_t>(state.range(0));
  const auto wire = BuildWire(kFrameCount, kPayloadSize);

  for (auto _ : state) {
    LengthPrefixedFrame parser(kMaxReceiveMessageSize);
    std::size_t completed = 0U;
    for (std::size_t offset = 0U; offset < wire.size(); offset += chunk_bytes) {
      const auto count = std::min(chunk_bytes, wire.size() - offset);
      auto frames = parser.Feed(std::span<const std::byte>(wire).subspan(offset, count));
      if (!frames.has_value()) {
        state.SkipWithError("fragmented feed rejected a valid frame");
        return;
      }
      completed += frames->size();
    }
    if (completed != kFrameCount) {
      state.SkipWithError("fragmented feed did not complete every frame");
      return;
    }
    benchmark::DoNotOptimize(completed);
  }
  state.counters["frames"] = static_cast<double>(kFrameCount);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kFrameCount);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(wire.size()));
}

BENCHMARK(BM_LengthPrefixedFrame_Fragmented)->Arg(64)->Arg(256)->Arg(1024);

/** Measures parsing when one chunk carries several complete frames. */
void BM_LengthPrefixedFrame_Coalesced(benchmark::State& state) {
  const auto wire = BuildWire(kFrameCount, kPayloadSize);

  for (auto _ : state) {
    LengthPrefixedFrame parser(kMaxReceiveMessageSize);
    auto frames = parser.Feed(wire);
    if (!frames.has_value()) {
      state.SkipWithError("coalesced feed rejected a valid frame");
      return;
    }
    if (frames->size() != kFrameCount) {
      state.SkipWithError("coalesced feed did not complete every frame");
      return;
    }
    benchmark::DoNotOptimize(frames);
  }
  state.counters["frames"] = static_cast<double>(kFrameCount);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kFrameCount);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(wire.size()));
}

BENCHMARK(BM_LengthPrefixedFrame_Coalesced);

BENCHMARK_MAIN();
