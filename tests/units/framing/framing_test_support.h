#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"

namespace framing_test_probe {

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

inline AllocationStats g_allocation_stats{};

inline void RecordAllocation(std::size_t size) noexcept {
  ++g_allocation_stats.calls;
  g_allocation_stats.bytes += size;
  g_allocation_stats.largest =
      std::max(g_allocation_stats.largest, static_cast<std::uint64_t>(size));
}

[[nodiscard]] inline auto AllocatePlain(std::size_t size) noexcept -> void* {
  void* memory = std::malloc(size == 0U ? 1U : size);
  if (memory != nullptr) {
    RecordAllocation(size);
  }
  return memory;
}

[[nodiscard]] inline auto AllocateAligned(std::size_t size,
                                          std::size_t alignment) noexcept -> void* {
  const std::size_t rounded = ((size + alignment - 1U) / alignment) * alignment;
  void* memory = std::aligned_alloc(alignment, rounded == 0U ? alignment : rounded);
  if (memory != nullptr) {
    RecordAllocation(size);
  }
  return memory;
}

inline void ResetAllocationProbe() noexcept { g_allocation_stats = AllocationStats{}; }

[[nodiscard]] inline auto ReadAllocationProbe() noexcept -> AllocationStats {
  return g_allocation_stats;
}

}  // namespace framing_test_probe

namespace grpcxx::framing_test_support {

/** Builds a gRPC wire frame, optionally overriding the declared length. */
[[nodiscard]] inline auto MakeWireFrame(std::span<const std::byte> payload, std::uint8_t flag,
                                        std::optional<std::uint32_t> declared_length = std::nullopt)
    -> std::vector<std::byte> {
  auto length = declared_length.value_or(static_cast<std::uint32_t>(payload.size()));
  if constexpr (std::endian::native == std::endian::little) {
    length = std::byteswap(length);
  }
  std::vector<std::byte> wire(AlignedMessageBuf::kGrpcHeaderBytes + payload.size());
  wire[0] = static_cast<std::byte>(flag);
  std::memcpy(wire.data() + 1U, &length, sizeof(length));
  std::ranges::copy(payload,
                    wire.data() + static_cast<std::ptrdiff_t>(AlignedMessageBuf::kGrpcHeaderBytes));
  return wire;
}

/** Builds a deterministic payload whose bytes vary with the position. */
[[nodiscard]] inline auto MakePayload(std::size_t size,
                                      std::uint8_t seed) -> std::vector<std::byte> {
  std::vector<std::byte> payload(size);
  for (const auto index : std::views::iota(std::size_t{0U}, size)) {
    payload[index] = static_cast<std::byte>((seed + index) & 0xFFU);
  }
  return payload;
}

[[nodiscard]] inline auto AsBytes(std::span<const std::byte> bytes) -> std::span<const std::byte> {
  return bytes;
}

/** Asserts the complete frame layout, payload contents and compressed flag. */
inline void ExpectFrame(const AlignedMessageBuf& frame, std::span<const std::byte> expected_payload,
                        bool expected_compressed) {
  ASSERT_EQ(frame.PayloadSize(), expected_payload.size());
  ASSERT_EQ(frame.WireSize(), AlignedMessageBuf::kGrpcHeaderBytes + expected_payload.size());
  EXPECT_TRUE(std::ranges::equal(frame.Payload(), expected_payload));
  EXPECT_TRUE(std::ranges::equal(frame.WireBytes().subspan(AlignedMessageBuf::kGrpcHeaderBytes),
                                 expected_payload));
  EXPECT_EQ(frame.Compressed(), expected_compressed);
  EXPECT_EQ(frame.PayloadAddress() % 8U, 0U);
  EXPECT_EQ(std::bit_cast<std::uintptr_t>(frame.Payload().data()), frame.PayloadAddress());
  EXPECT_EQ(
      std::bit_cast<std::uintptr_t>(frame.WireBytes().data()) + AlignedMessageBuf::kGrpcHeaderBytes,
      frame.PayloadAddress());

  std::uint32_t decoded = 0U;
  std::memcpy(&decoded, frame.WireBytes().data() + 1U, sizeof(decoded));
  if constexpr (std::endian::native == std::endian::little) {
    decoded = std::byteswap(decoded);
  }
  EXPECT_EQ(decoded, expected_payload.size());
  EXPECT_EQ(frame.WireBytes()[0], expected_compressed ? std::byte{0x01} : std::byte{0x00});
}

/** Feeds one chunk and returns the completed frames, failing the test on error. */
inline auto FeedOk(LengthPrefixedFrame& parser,
                   std::span<const std::byte> chunk) -> std::vector<AlignedMessageBuf> {
  auto result = parser.Feed(chunk);
  EXPECT_TRUE(result.has_value());
  if (!result.has_value()) {
    return {};
  }
  return std::move(*result);
}

}  // namespace grpcxx::framing_test_support
