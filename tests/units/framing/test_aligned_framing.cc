#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <iterator>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"

namespace {

/**
 * Allocation counters owned by this test binary.
 *
 * GoogleTest allocates freely while registering and reporting tests, so scenarios that claim
 * zero allocation reset the probe immediately before the code under test and read it
 * immediately after, with no framework call in between.
 */
struct AllocationStats {
  std::uint64_t calls{0U};
  std::uint64_t bytes{0U};
  std::uint64_t largest{0U};
};

AllocationStats g_allocation_stats{};

void RecordAllocation(std::size_t size) noexcept {
  ++g_allocation_stats.calls;
  g_allocation_stats.bytes += size;
  g_allocation_stats.largest =
      std::max(g_allocation_stats.largest, static_cast<std::uint64_t>(size));
}

[[nodiscard]] auto AllocatePlain(std::size_t size) noexcept -> void* {
  void* memory = std::malloc(size == 0U ? 1U : size);
  if (memory != nullptr) {
    RecordAllocation(size);
  }
  return memory;
}

[[nodiscard]] auto AllocateAligned(std::size_t size, std::size_t alignment) noexcept -> void* {
  // aligned_alloc requires the size to be a multiple of the alignment.
  const std::size_t rounded = ((size + alignment - 1U) / alignment) * alignment;
  void* memory = std::aligned_alloc(alignment, rounded == 0U ? alignment : rounded);
  if (memory != nullptr) {
    RecordAllocation(size);
  }
  return memory;
}

void ResetAllocationProbe() noexcept { g_allocation_stats = AllocationStats{}; }

[[nodiscard]] auto ReadAllocationProbe() noexcept -> AllocationStats { return g_allocation_stats; }

}  // namespace

void* operator new(std::size_t size) {
  void* memory = AllocatePlain(size);
  if (memory == nullptr) {
    std::abort();
  }
  return memory;
}

void* operator new[](std::size_t size) {
  void* memory = AllocatePlain(size);
  if (memory == nullptr) {
    std::abort();
  }
  return memory;
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return AllocatePlain(size); }

void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return AllocatePlain(size);
}

void* operator new(std::size_t size, std::align_val_t alignment) {
  void* memory = AllocateAligned(size, static_cast<std::size_t>(alignment));
  if (memory == nullptr) {
    std::abort();
  }
  return memory;
}

void* operator new[](std::size_t size, std::align_val_t alignment) {
  return operator new(size, alignment);
}

void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
  return AllocateAligned(size, static_cast<std::size_t>(alignment));
}

void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
  return AllocateAligned(size, static_cast<std::size_t>(alignment));
}

void operator delete(void* memory) noexcept { std::free(memory); }

void operator delete[](void* memory) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

void operator delete(void* memory, std::align_val_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::align_val_t) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }

namespace grpcxx {
namespace {

/** Builds a gRPC wire frame, optionally overriding the declared length. */
[[nodiscard]] auto MakeWireFrame(std::span<const std::byte> payload, std::uint8_t flag,
                                 std::optional<std::uint32_t> declared_length = std::nullopt)
    -> std::vector<std::byte> {
  const auto length = declared_length.value_or(static_cast<std::uint32_t>(payload.size()));
  std::vector<std::byte> wire(AlignedMessageBuf::kGrpcHeaderBytes + payload.size());
  wire[0] = static_cast<std::byte>(flag);
  wire[1] = static_cast<std::byte>((length >> 24U) & 0xFFU);
  wire[2] = static_cast<std::byte>((length >> 16U) & 0xFFU);
  wire[3] = static_cast<std::byte>((length >> 8U) & 0xFFU);
  wire[4] = static_cast<std::byte>(length & 0xFFU);
  std::ranges::copy(payload,
                    wire.data() + static_cast<std::ptrdiff_t>(AlignedMessageBuf::kGrpcHeaderBytes));
  return wire;
}

/** Builds a deterministic payload whose bytes vary with the position. */
[[nodiscard]] auto MakePayload(std::size_t size, std::uint8_t seed) -> std::vector<std::byte> {
  std::vector<std::byte> payload(size);
  for (const auto index : std::views::iota(std::size_t{0U}, size)) {
    payload[index] = static_cast<std::byte>((seed + index) & 0xFFU);
  }
  return payload;
}

[[nodiscard]] auto AsBytes(std::span<const std::byte> bytes) -> std::span<const std::byte> {
  return bytes;
}

/** Asserts the complete frame layout, payload contents and compressed flag. */
void ExpectFrame(const AlignedMessageBuf& frame, std::span<const std::byte> expected_payload,
                 bool expected_compressed) {
  ASSERT_EQ(frame.PayloadSize(), expected_payload.size());
  ASSERT_EQ(frame.WireSize(), AlignedMessageBuf::kGrpcHeaderBytes + expected_payload.size());
  EXPECT_TRUE(std::ranges::equal(frame.Payload(), expected_payload));
  EXPECT_TRUE(std::ranges::equal(frame.WireBytes().subspan(AlignedMessageBuf::kGrpcHeaderBytes),
                                 expected_payload));
  EXPECT_EQ(frame.Compressed(), expected_compressed);
  EXPECT_EQ(frame.PayloadAddress() % 8U, 0U);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.Payload().data()), frame.PayloadAddress());
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.WireBytes().data()) +
                AlignedMessageBuf::kGrpcHeaderBytes,
            frame.PayloadAddress());

  const auto length_bytes = frame.WireBytes().subspan(1U, 4U);
  const auto decoded = (std::to_integer<std::uint32_t>(length_bytes[0]) << 24U) |
                       (std::to_integer<std::uint32_t>(length_bytes[1]) << 16U) |
                       (std::to_integer<std::uint32_t>(length_bytes[2]) << 8U) |
                       std::to_integer<std::uint32_t>(length_bytes[3]);
  EXPECT_EQ(decoded, expected_payload.size());
  EXPECT_EQ(frame.WireBytes()[0], expected_compressed ? std::byte{0x01} : std::byte{0x00});
}

/** Feeds one chunk and returns the completed frames, failing the test on error. */
auto FeedOk(LengthPrefixedFrame& parser,
            std::span<const std::byte> chunk) -> std::vector<AlignedMessageBuf> {
  auto result = parser.Feed(chunk);
  EXPECT_TRUE(result.has_value());
  if (!result.has_value()) {
    return {};
  }
  return std::move(*result);
}

// ---------------------------------------------------------------------------
// AlignedMessageBuf layout
// ---------------------------------------------------------------------------

TEST(AlignedMessageBufTest, DefaultConstructionExposesZeroLengthWireFrame) {
  AlignedMessageBuf frame;
  EXPECT_EQ(frame.PayloadSize(), 0U);
  EXPECT_EQ(frame.WireSize(), 5U);
  ASSERT_EQ(frame.Payload().size(), 0U);
  ASSERT_EQ(frame.WireBytes().size(), 5U);
  EXPECT_FALSE(frame.Compressed());
  EXPECT_EQ(frame.PayloadAddress() % 8U, 0U);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.WireBytes().data()) + 5U,
            frame.PayloadAddress());
  EXPECT_TRUE(std::ranges::equal(frame.WireBytes(),
                                 std::array{std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
                                            std::byte{0x00}, std::byte{0x00}}));
}

TEST(AlignedMessageBufTest, SharedContractAllocateMatchesConstructorLayout) {
  const auto allocated = AlignedMessageBuf::Allocate(37U);
  ASSERT_TRUE(allocated.has_value());
  const auto& frame = *allocated;
  AlignedMessageBuf constructed(37U);
  EXPECT_EQ(frame.Size(), constructed.Size());
  EXPECT_EQ(frame.PayloadSize(), constructed.PayloadSize());
  EXPECT_EQ(frame.WireSize(), constructed.WireSize());
  EXPECT_FALSE(frame.Compressed());
  EXPECT_EQ(frame.Compressed(), constructed.Compressed());
  EXPECT_EQ(frame.PayloadAddress() % 8U, 0U);
  EXPECT_TRUE(std::ranges::equal(frame.WireBytes(), constructed.WireBytes()));

  const auto compressed = AlignedMessageBuf::Allocate(37U, true);
  ASSERT_TRUE(compressed.has_value());
  EXPECT_TRUE(compressed->Compressed());
  EXPECT_EQ(compressed->Size(), 37U);
  EXPECT_EQ(std::to_integer<std::uint8_t>(compressed->WireBytes()[0]), 1U);

  const auto empty = AlignedMessageBuf::Allocate(0U);
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty->Size(), 0U);
  EXPECT_EQ(empty->WireSize(), 5U);
  EXPECT_FALSE(empty->Compressed());
}

TEST(AlignedMessageBufTest, PayloadBytesStartAtZero) {
  const auto all_zero = [](std::span<const std::byte> bytes) {
    return std::ranges::all_of(bytes, [](std::byte value) { return value == std::byte{0x00}; });
  };

  // Dirty a same-sized allocation first so a reused chunk cannot masquerade as zero-init.
  {
    AlignedMessageBuf dirty(16U);
    std::ranges::fill(dirty.Payload(), std::byte{0xAB});
  }

  AlignedMessageBuf frame(16U);
  EXPECT_TRUE(all_zero(frame.Payload()));

  const auto allocated = AlignedMessageBuf::Allocate(16U);
  ASSERT_TRUE(allocated.has_value());
  EXPECT_TRUE(all_zero(allocated->Payload()));
}

TEST(AlignedMessageBufTest, PayloadStartsAtOffsetEightForEveryTestedSize) {
  for (const auto size : std::array<std::size_t, 8>{0U, 1U, 2U, 7U, 8U, 9U, 1024U, 32768U}) {
    AlignedMessageBuf frame(size);
    EXPECT_EQ(frame.PayloadSize(), size);
    EXPECT_EQ(frame.WireSize(), size + 5U);
    EXPECT_EQ(frame.Payload().size(), size);
    EXPECT_EQ(frame.WireBytes().size(), size + 5U);
    EXPECT_EQ(frame.PayloadAddress() % 8U, 0U);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.Payload().data()), frame.PayloadAddress());
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.WireBytes().data()) + 5U,
              frame.PayloadAddress());
  }
}

TEST(AlignedMessageBufTest, HeaderEncodesLengthBigEndian) {
  AlignedMessageBuf frame(258U);
  const auto wire = frame.WireBytes();
  ASSERT_EQ(wire.size(), 263U);
  EXPECT_EQ(wire[0], std::byte{0x00});
  EXPECT_EQ(wire[1], std::byte{0x00});
  EXPECT_EQ(wire[2], std::byte{0x00});
  EXPECT_EQ(wire[3], std::byte{0x01});
  EXPECT_EQ(wire[4], std::byte{0x02});
}

TEST(AlignedMessageBufTest, CompressedFlagIsTheOnlyNonZeroFlagByte) {
  AlignedMessageBuf frame(4U);
  EXPECT_FALSE(frame.Compressed());
  EXPECT_EQ(frame.WireBytes()[0], std::byte{0x00});

  frame.SetCompressed(true);
  EXPECT_TRUE(frame.Compressed());
  EXPECT_EQ(frame.WireBytes()[0], std::byte{0x01});
  // Padding before the header must never be exposed through WireBytes().
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(frame.WireBytes().data()) + 5U,
            frame.PayloadAddress());

  frame.SetCompressed(false);
  EXPECT_FALSE(frame.Compressed());
  EXPECT_EQ(frame.WireBytes()[0], std::byte{0x00});
}

TEST(AlignedMessageBufTest, PayloadWritesAreVisibleThroughWireBytes) {
  auto payload = MakePayload(16U, 0x40U);
  AlignedMessageBuf frame(payload.size());
  std::ranges::copy(payload, frame.Payload().data());
  EXPECT_TRUE(std::ranges::equal(frame.Payload(), payload));
  EXPECT_TRUE(
      std::ranges::equal(frame.WireBytes().subspan(AlignedMessageBuf::kGrpcHeaderBytes), payload));
}

TEST(AlignedMessageBufTest, MoveConstructionTransfersOwnership) {
  auto payload = MakePayload(32U, 0x10U);
  AlignedMessageBuf source(payload.size());
  std::ranges::copy(payload, source.Payload().data());
  source.SetCompressed(true);
  const auto address = source.PayloadAddress();

  AlignedMessageBuf moved(std::move(source));
  EXPECT_EQ(moved.PayloadAddress(), address);
  EXPECT_TRUE(moved.Compressed());
  ExpectFrame(moved, payload, true);
}

TEST(AlignedMessageBufTest, MoveAssignmentTransfersOwnership) {
  auto payload = MakePayload(12U, 0x80U);
  AlignedMessageBuf source(payload.size());
  std::ranges::copy(payload, source.Payload().data());
  AlignedMessageBuf destination(1U);
  destination = std::move(source);
  ExpectFrame(destination, payload, false);
}

// ---------------------------------------------------------------------------
// LengthPrefixedFrame fragmentation
// ---------------------------------------------------------------------------

TEST(LengthPrefixedFrameTest, ByteByByteFeedCompletesExactlyOneFrame) {
  const auto payload = MakePayload(7U, 0x20U);
  const auto wire = MakeWireFrame(payload, 0U);
  ASSERT_EQ(wire.size(), 12U);

  LengthPrefixedFrame parser(1024U);
  std::size_t completed = 0U;
  for (const auto index : std::views::iota(std::size_t{0U}, wire.size())) {
    const auto frames = FeedOk(parser, std::span<const std::byte>(wire).subspan(index, 1U));
    const auto header_bytes_seen = index + 1U;
    if (header_bytes_seen < AlignedMessageBuf::kGrpcHeaderBytes) {
      EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
      EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);
    } else if (header_bytes_seen < wire.size()) {
      EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
      EXPECT_EQ(parser.BufferedPayloadBytes(),
                header_bytes_seen - AlignedMessageBuf::kGrpcHeaderBytes);
    } else {
      EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
      EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);
    }

    if (index + 1U < wire.size()) {
      EXPECT_TRUE(frames.empty());
    } else {
      ASSERT_EQ(frames.size(), 1U);
      ExpectFrame(frames.front(), payload, false);
      completed += frames.size();
    }
  }
  EXPECT_EQ(completed, 1U);
}

TEST(LengthPrefixedFrameTest, SplitAtEveryBoundaryCompletesOneExactFrame) {
  const auto payload = MakePayload(10U, 0x33U);
  const auto wire = MakeWireFrame(payload, 0U);

  for (const auto split : std::views::iota(std::size_t{0U}, wire.size() + 1U)) {
    LengthPrefixedFrame parser(1024U);
    auto first = parser.Feed(std::span<const std::byte>(wire).first(split));
    auto second = parser.Feed(std::span<const std::byte>(wire).subspan(split));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->size() + second->size(), 1U);
    if (!first->empty()) {
      ExpectFrame(first->front(), payload, false);
    }
    if (!second->empty()) {
      ExpectFrame(second->front(), payload, false);
    }
    EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  }
}

TEST(LengthPrefixedFrameTest, CoalescedFramesAreReturnedInWireOrder) {
  const auto first_payload = MakePayload(3U, 0x01U);
  const auto second_payload = MakePayload(5U, 0x40U);
  const auto third_payload = MakePayload(2U, 0x80U);

  std::vector<std::byte> chunk;
  for (const auto& [payload, compressed] :
       std::array<std::pair<std::span<const std::byte>, bool>, 3>{
           std::pair{AsBytes(first_payload), false}, std::pair{AsBytes(second_payload), true},
           std::pair{AsBytes(third_payload), false}}) {
    const auto wire = MakeWireFrame(payload, compressed ? 1U : 0U);
    chunk.append_range(wire);
  }

  LengthPrefixedFrame parser(1024U);
  const auto frames = FeedOk(parser, chunk);
  ASSERT_EQ(frames.size(), 3U);
  ExpectFrame(frames[0], first_payload, false);
  ExpectFrame(frames[1], second_payload, true);
  ExpectFrame(frames[2], third_payload, false);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
}

TEST(LengthPrefixedFrameTest, CoalescedFramesMaySpanChunkBoundaries) {
  const auto first_payload = MakePayload(4U, 0x11U);
  const auto second_payload = MakePayload(9U, 0x22U);
  const auto first_wire = MakeWireFrame(first_payload, 0U);
  const auto second_wire = MakeWireFrame(second_payload, 0U);

  std::vector<std::byte> chunk(first_wire.size());
  std::ranges::copy(first_wire, chunk.data());
  chunk.append_range(second_wire);

  LengthPrefixedFrame parser(1024U);
  // The first chunk completes the first frame and ends three bytes into the second payload.
  const std::size_t split = first_wire.size() + AlignedMessageBuf::kGrpcHeaderBytes + 3U;
  auto first = parser.Feed(std::span<const std::byte>(chunk).first(split));
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(first->size(), 1U);
  ExpectFrame(first->front(), first_payload, false);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
  EXPECT_EQ(parser.BufferedPayloadBytes(), 3U);

  const auto frames = FeedOk(parser, std::span<const std::byte>(chunk).subspan(split));
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), second_payload, false);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
}

TEST(LengthPrefixedFrameTest, ZeroLengthPayloadProducesAnEmptyFrame) {
  LengthPrefixedFrame parser(1024U);
  const auto wire = MakeWireFrame(std::span<const std::byte>{}, 0U);
  const auto frames = FeedOk(parser, wire);
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), std::span<const std::byte>{}, false);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
}

TEST(LengthPrefixedFrameTest, ZeroLengthCompressedPayloadPropagatesFlag) {
  LengthPrefixedFrame parser(1024U);
  const auto wire = MakeWireFrame(std::span<const std::byte>{}, 1U);
  const auto frames = FeedOk(parser, wire);
  ASSERT_EQ(frames.size(), 1U);
  EXPECT_TRUE(frames.front().Compressed());
  EXPECT_EQ(frames.front().WireBytes()[0], std::byte{0x01});
  EXPECT_EQ(frames.front().PayloadSize(), 0U);
}

TEST(LengthPrefixedFrameTest, OnlyTheLowFlagBitSelectsCompression) {
  for (const auto& [flag, compressed] : std::array<std::pair<std::uint8_t, bool>, 3>{
           {{0x80U, false}, {0x03U, true}, {0xFEU, false}}}) {
    LengthPrefixedFrame parser(1024U);
    const auto frames = FeedOk(parser, MakeWireFrame(std::span<const std::byte>{}, flag));
    ASSERT_EQ(frames.size(), 1U) << static_cast<int>(flag);
    EXPECT_EQ(frames.front().Compressed(), compressed) << static_cast<int>(flag);
  }
}

TEST(LengthPrefixedFrameTest, CompressedFlagIsPropagatedForNonEmptyPayload) {
  const auto payload = MakePayload(6U, 0x55U);
  for (const auto compressed : std::array{false, true}) {
    LengthPrefixedFrame parser(1024U);
    const auto wire = MakeWireFrame(payload, compressed ? 1U : 0U);
    const auto frames = FeedOk(parser, wire);
    ASSERT_EQ(frames.size(), 1U);
    ExpectFrame(frames.front(), payload, compressed);
  }
}

TEST(LengthPrefixedFrameTest, PayloadBytesMatchAcrossManySmallChunks) {
  const auto payload = MakePayload(1000U, 0x03U);
  const auto wire = MakeWireFrame(payload, 0U);

  LengthPrefixedFrame parser(2048U);
  std::vector<std::byte> collected;
  for (std::size_t offset = 0U; offset < wire.size(); offset += 7U) {
    const auto count = std::min<std::size_t>(7U, wire.size() - offset);
    const auto frames = FeedOk(parser, std::span<const std::byte>(wire).subspan(offset, count));
    for (const auto& frame : frames) {
      const auto bytes = frame.Payload();
      collected.append_range(bytes);
    }
  }
  ASSERT_EQ(collected.size(), payload.size());
  EXPECT_TRUE(std::ranges::equal(collected, payload));
}

// ---------------------------------------------------------------------------
// LengthPrefixedFrame limits and recovery
// ---------------------------------------------------------------------------

TEST(LengthPrefixedFrameTest, PayloadAtMaxReceiveMessageSizeIsAccepted) {
  constexpr std::size_t kMax = 1024U;
  const auto payload = MakePayload(kMax, 0xA0U);
  const auto wire = MakeWireFrame(payload, 0U);

  LengthPrefixedFrame parser(kMax);
  EXPECT_EQ(parser.MaxReceiveMessageSize(), kMax);
  const auto frames = FeedOk(parser, wire);
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), payload, false);
  EXPECT_EQ(parser.RejectedFrames(), 0U);
}

TEST(LengthPrefixedFrameTest, OversizedHeaderIsRejectedBeforePayloadAllocation) {
  constexpr std::size_t kMax = 1024U;
  constexpr std::uint32_t kLarge = 4096U;
  LengthPrefixedFrame parser(kMax);

  const auto wire = MakeWireFrame(std::span<const std::byte>{}, 0U, kLarge);
  ASSERT_EQ(wire.size(), 5U);

  ResetAllocationProbe();
  auto result = parser.Feed(wire);
  const auto stats = ReadAllocationProbe();

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kFailed);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);

  // Allocating the announced frame would require at least kLarge payload bytes, so no single
  // allocation may reach that size. A diagnostic message longer than the Status small-string
  // buffer could add one allocation for its own text, hence the call bound rather than zero.
  EXPECT_LE(stats.calls, 1U);
  EXPECT_LT(stats.largest, static_cast<std::uint64_t>(kLarge));
}

TEST(LengthPrefixedFrameTest, OversizedFrameIsRejectedWhenTheHeaderCompletes) {
  constexpr std::uint32_t kLarge = 5000U;
  LengthPrefixedFrame parser(1024U);
  const auto wire = MakeWireFrame(std::span<const std::byte>{}, 0U, kLarge);

  auto partial = parser.Feed(std::span<const std::byte>(wire).first(4U));
  ASSERT_TRUE(partial.has_value());
  EXPECT_TRUE(partial->empty());
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  EXPECT_EQ(parser.RejectedFrames(), 0U);

  auto rejected = parser.Feed(std::span<const std::byte>(wire).subspan(4U, 1U));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kFailed);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
}

TEST(LengthPrefixedFrameTest, FeedAfterFailureReturnsFailedPreconditionWithoutCounting) {
  LengthPrefixedFrame parser(64U);
  const auto oversized = MakeWireFrame(std::span<const std::byte>{}, 0U, 65U);
  const auto rejected = parser.Feed(oversized);
  ASSERT_FALSE(rejected.has_value());
  ASSERT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.RejectedTransitions(), 0U);

  const auto valid = MakeWireFrame(AsBytes(MakePayload(4U, 0x01U)), 0U);
  const auto after = parser.Feed(valid);
  ASSERT_FALSE(after.has_value());
  EXPECT_EQ(after.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kFailed);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.RejectedTransitions(), 1U);
  EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);

  // Reset clears the failed state but keeps both counters for the parser's lifetime.
  parser.Reset();
  EXPECT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.RejectedTransitions(), 1U);
}

TEST(LengthPrefixedFrameTest, ResetRestoresParsingAfterRejectionAndKeepsCounters) {
  LengthPrefixedFrame parser(64U);
  const auto oversized = MakeWireFrame(std::span<const std::byte>{}, 0U, 65U);
  ASSERT_FALSE(parser.Feed(oversized).has_value());
  ASSERT_EQ(parser.RejectedFrames(), 1U);

  parser.Reset();
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.MaxReceiveMessageSize(), 64U);

  const auto payload = MakePayload(8U, 0x77U);
  const auto frames = FeedOk(parser, MakeWireFrame(payload, 0U));
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), payload, false);
}

TEST(LengthPrefixedFrameTest, ResetDropsPartialPayloadState) {
  const auto partial_payload = MakePayload(10U, 0x99U);
  const auto partial_wire = MakeWireFrame(partial_payload, 0U);
  LengthPrefixedFrame parser(1024U);

  FeedOk(parser, std::span<const std::byte>(partial_wire).first(8U));
  ASSERT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
  ASSERT_EQ(parser.BufferedPayloadBytes(), 3U);

  parser.Reset();
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  EXPECT_EQ(parser.BufferedPayloadBytes(), 0U);

  const auto payload = MakePayload(2U, 0x05U);
  const auto frames = FeedOk(parser, MakeWireFrame(payload, 0U));
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), payload, false);
}

TEST(LengthPrefixedFrameTest, MoveTransfersStateAndLeavesTheSourceUsable) {
  const auto payload = MakePayload(6U, 0x42U);
  const auto wire = MakeWireFrame(payload, 0U);
  LengthPrefixedFrame source(1024U);
  FeedOk(source, std::span<const std::byte>(wire).first(8U));
  ASSERT_EQ(source.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
  ASSERT_EQ(source.BufferedPayloadBytes(), 3U);

  LengthPrefixedFrame moved(std::move(source));
  EXPECT_EQ(moved.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
  EXPECT_EQ(moved.BufferedPayloadBytes(), 3U);
  EXPECT_EQ(moved.MaxReceiveMessageSize(), 1024U);
  EXPECT_EQ(source.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  EXPECT_EQ(source.BufferedPayloadBytes(), 0U);

  const auto resumed = FeedOk(moved, std::span<const std::byte>(wire).subspan(8U));
  ASSERT_EQ(resumed.size(), 1U);
  ExpectFrame(resumed.front(), payload, false);

  // The moved-from parser must parse a fresh frame rather than touch moved-out storage.
  const auto fresh = FeedOk(source, wire);
  ASSERT_EQ(fresh.size(), 1U);
  ExpectFrame(fresh.front(), payload, false);
}

TEST(LengthPrefixedFrameTest, MoveAssignmentTransfersStateAndResetsTheSource) {
  const auto payload = MakePayload(4U, 0x21U);
  const auto wire = MakeWireFrame(payload, 0U);
  LengthPrefixedFrame source(1024U);
  FeedOk(source, std::span<const std::byte>(wire).first(7U));
  ASSERT_EQ(source.State(), LengthPrefixedFrame::ParseState::kReadingPayload);

  LengthPrefixedFrame assigned(64U);
  assigned = std::move(source);
  EXPECT_EQ(assigned.MaxReceiveMessageSize(), 1024U);
  EXPECT_EQ(assigned.State(), LengthPrefixedFrame::ParseState::kReadingPayload);
  EXPECT_EQ(assigned.BufferedPayloadBytes(), 2U);
  EXPECT_EQ(source.State(), LengthPrefixedFrame::ParseState::kReadingHeader);
  EXPECT_EQ(source.BufferedPayloadBytes(), 0U);

  const auto frames = FeedOk(assigned, std::span<const std::byte>(wire).subspan(7U));
  ASSERT_EQ(frames.size(), 1U);
  ExpectFrame(frames.front(), payload, false);
}

TEST(LengthPrefixedFrameTest, ZeroMaximumAcceptsOnlyEmptyFrames) {
  LengthPrefixedFrame parser(0U);
  const auto empty_frames = FeedOk(parser, MakeWireFrame(std::span<const std::byte>{}, 0U));
  ASSERT_EQ(empty_frames.size(), 1U);
  EXPECT_EQ(empty_frames.front().PayloadSize(), 0U);
  EXPECT_EQ(parser.RejectedFrames(), 0U);

  const auto rejected = parser.Feed(MakeWireFrame(AsBytes(MakePayload(1U, 0x00U)), 0U));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kFailed);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
}

TEST(LengthPrefixedFrameTest, FramesCompletedBeforeAnOversizedHeaderAreDiscardedOnError) {
  LengthPrefixedFrame parser(16U);
  const auto valid_payload = MakePayload(4U, 0x0AU);
  const auto valid_wire = MakeWireFrame(valid_payload, 0U);
  const auto oversized = MakeWireFrame(std::span<const std::byte>{}, 0U, 17U);

  std::vector<std::byte> chunk(valid_wire.size());
  std::ranges::copy(valid_wire, chunk.data());
  chunk.append_range(oversized);

  const auto result = parser.Feed(chunk);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().Code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.RejectedFrames(), 1U);
  EXPECT_EQ(parser.State(), LengthPrefixedFrame::ParseState::kFailed);
}

}  // namespace
}  // namespace grpcxx
