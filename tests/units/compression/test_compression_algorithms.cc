#include <fmt/format.h>
#include <gtest/gtest.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/compression/deflate_compressor.h"
#include "grpcxx/compression/gzip_compressor.h"
#include "grpcxx/compression/lz4_compressor.h"
#include "grpcxx/compression/snappy_compressor.h"
#include "grpcxx/compression/zstd_compressor.h"

namespace grpcxx {
namespace {

auto Bytes(std::string_view text) -> std::span<const std::byte> {
  return std::as_bytes(std::span(text));
}

template <typename Compressor>
void CheckAlgorithm() {
  Compressor compressor;
  const std::string payload(65'536, 'x');
  auto compressed = compressor.Compress(Bytes(payload));
  ASSERT_TRUE(compressed.has_value());
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(compressed->Payload().data()) % 8, 0);
  auto restored = compressor.Decompress(compressed->Payload(), payload.size());
  ASSERT_TRUE(restored.has_value());
  EXPECT_TRUE(std::ranges::equal(restored->Payload(), Bytes(payload)));
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(restored->Payload().data()) % 8, 0);
  auto too_large = compressor.Decompress(compressed->Payload(), payload.size() - 1);
  ASSERT_FALSE(too_large.has_value());
  EXPECT_EQ(too_large.error().Code(), StatusCode::kResourceExhausted);
  auto truncated = compressor.Decompress(
      compressed->Payload().first(compressed->Payload().size() / 2), payload.size());
  ASSERT_FALSE(truncated.has_value());
  EXPECT_EQ(truncated.error().Code(), StatusCode::kInternal);
  std::vector<std::byte> appended(compressed->Payload().size() + 1);
  std::ranges::copy(compressed->Payload(), appended.data());
  appended.back() = std::byte{0x7f};
  auto trailing = compressor.Decompress(appended, payload.size());
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().Code(), StatusCode::kInternal);
  const std::array<std::byte, 8> malformed{std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                                           std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                                           std::byte{0xff}, std::byte{0xff}};
  auto invalid = compressor.Decompress(malformed, payload.size());
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().Code(), StatusCode::kInternal);
  auto missing = compressor.Decompress({}, payload.size());
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().Code(), StatusCode::kInternal);
  for (const auto size : std::array<std::size_t, 7>{1, 7, 8, 9, 255, 256, 257}) {
    std::vector<std::byte> boundary(size);
    for (const auto index : std::views::iota(std::size_t{0}, size)) {
      boundary[index] = static_cast<std::byte>(index % 251);
    }
    auto encoded = compressor.Compress(boundary);
    ASSERT_TRUE(encoded);
    auto decoded = compressor.Decompress(encoded->Payload(), size);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(std::ranges::equal(decoded->Payload(), boundary));
    auto zero_budget = compressor.Decompress(encoded->Payload(), 0);
    ASSERT_FALSE(zero_budget);
    EXPECT_EQ(zero_budget.error().Code(), StatusCode::kResourceExhausted);
  }
  auto empty = compressor.Compress({});
  ASSERT_TRUE(empty.has_value());
  auto empty_restored = compressor.Decompress(empty->Payload(), 0);
  ASSERT_TRUE(empty_restored.has_value());
  EXPECT_TRUE(empty_restored->Payload().empty());
}

TEST(CompressionAlgorithms, Zstd) { CheckAlgorithm<ZstdCompressor>(); }
TEST(CompressionAlgorithms, Lz4) { CheckAlgorithm<Lz4Compressor>(); }
TEST(CompressionAlgorithms, Gzip) { CheckAlgorithm<GzipCompressor>(); }
TEST(CompressionAlgorithms, Deflate) { CheckAlgorithm<DeflateCompressor>(); }
TEST(CompressionAlgorithms, Snappy) { CheckAlgorithm<SnappyCompressor>(); }

constexpr auto kAlgorithms = std::array{CompressionAlgorithm::kGzip, CompressionAlgorithm::kDeflate,
                                        CompressionAlgorithm::kZstd, CompressionAlgorithm::kLz4,
                                        CompressionAlgorithm::kSnappy};

TEST(CompressionRegistry, AdaptiveBypassAndAlignedFraming) {
  CompressionRegistry registry;
  for (const auto algorithm : kAlgorithms) {
    for (const auto size : std::array<std::size_t, 4>{0, 1, 64, 255}) {
      const std::string payload(size, 'a');
      auto encoded = registry.Compress(algorithm, Bytes(payload));
      ASSERT_TRUE(encoded.has_value());
      EXPECT_EQ(encoded->WireBytes()[0], std::byte{0});
      EXPECT_TRUE(std::ranges::equal(encoded->Payload(), Bytes(payload)));
      EXPECT_EQ(reinterpret_cast<std::uintptr_t>(encoded->Payload().data()) % 8, 0);
    }
    for (const auto size : std::array<std::size_t, 2>{256, 4096}) {
      const std::string payload(size, 'a');
      auto encoded = registry.Compress(algorithm, Bytes(payload));
      ASSERT_TRUE(encoded.has_value());
      EXPECT_EQ(encoded->WireBytes()[0], std::byte{1});
      auto decoded = registry.Decompress(algorithm, encoded->Payload(), payload.size());
      ASSERT_TRUE(decoded.has_value());
      EXPECT_TRUE(std::ranges::equal(decoded->Payload(), Bytes(payload)));
    }
  }
}

TEST(CompressionAlgorithms, ZstdUnknownContentSizeBombHonorsWindowBudget) {
  const std::string input(2 * 1024 * 1024, 'x');
  std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), ZSTD_freeCCtx);
  ASSERT_NE(context, nullptr);
  ASSERT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 0)));
  ASSERT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_windowLog, 21)));
  std::vector<std::byte> encoded(ZSTD_compressBound(input.size()));
  const auto size =
      ZSTD_compress2(context.get(), encoded.data(), encoded.size(), input.data(), input.size());
  ASSERT_FALSE(ZSTD_isError(size));
  encoded.resize(size);
  ASSERT_EQ(ZSTD_getFrameContentSize(encoded.data(), encoded.size()), ZSTD_CONTENTSIZE_UNKNOWN);
  ZstdCompressor compressor;
  auto decoded = compressor.Decompress(encoded, 4096);
  ASSERT_FALSE(decoded);
  EXPECT_EQ(decoded.error().Code(), StatusCode::kResourceExhausted);
}

TEST(CompressionAlgorithms, SnappyDeclaredLengthBombIsRejectedBeforeAllocation) {
  SnappyCompressor compressor;
  constexpr std::array<std::byte, 5> bomb{std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                                          std::byte{0xff}, std::byte{0x0f}};
  auto decoded = compressor.Decompress(bomb, 1024);
  ASSERT_FALSE(decoded);
  EXPECT_EQ(decoded.error().Code(), StatusCode::kResourceExhausted);
}

TEST(CompressionRegistry, EncodingNegotiation) {
  for (const auto algorithm : kAlgorithms) {
    auto parsed = CompressionRegistry::ParseEncoding(CompressionRegistry::EncodingName(algorithm));
    ASSERT_TRUE(parsed);
    EXPECT_EQ(*parsed, algorithm);
  }
  EXPECT_FALSE(CompressionRegistry::ParseEncoding("unknown"));
  EXPECT_FALSE(CompressionRegistry::ParseEncoding("gzip,deflate"));
  EXPECT_EQ(CompressionRegistry::Negotiate("unknown, zstd, gzip"), CompressionAlgorithm::kZstd);
  EXPECT_EQ(CompressionRegistry::Negotiate("unknown"), CompressionAlgorithm::kIdentity);
  EXPECT_NE(CompressionRegistry::AcceptEncoding().find("gzip"), std::string_view::npos);
}

TEST(CompressionRegistry, IncompressibleInputBypassesCompression) {
  CompressionRegistry registry;
  std::vector<std::byte> payload(256);
  std::uint32_t state = 0x12345678;
  for (auto& byte : payload) {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    byte = static_cast<std::byte>(state & 255);
  }
  for (const auto algorithm : kAlgorithms) {
    auto encoded = registry.Compress(algorithm, payload);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->WireBytes()[0], std::byte{0});
    EXPECT_TRUE(std::ranges::equal(encoded->Payload(), payload));
  }
}

TEST(CompressionRegistry, IdentityIsBoundedAndPreservesBytes) {
  CompressionRegistry registry;
  const auto payload = Bytes("identity payload");
  auto encoded = registry.Compress(CompressionAlgorithm::kIdentity, payload);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->WireBytes()[0], std::byte{0});
  auto too_large = registry.Decompress(CompressionAlgorithm::kIdentity, payload, 1);
  ASSERT_FALSE(too_large.has_value());
  EXPECT_EQ(too_large.error().Code(), StatusCode::kResourceExhausted);
}

}  // namespace
}  // namespace grpcxx
