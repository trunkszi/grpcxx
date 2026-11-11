#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"

namespace {

std::array<std::byte, grpcxx::AlignedMessageBuf::kGrpcHeaderBytes> PackedHeader(
    std::uint64_t packed) {
  std::array<std::byte, grpcxx::AlignedMessageBuf::kGrpcHeaderBytes> bytes{};
  std::memcpy(bytes.data(), &packed, bytes.size());
  return bytes;
}

void ExpectHeaderMatch(std::uint32_t length, bool compressed) {
  auto frame = grpcxx::AlignedMessageBuf::Allocate(length, compressed);
  ASSERT_TRUE(frame.has_value());
  const auto flag = static_cast<std::uint8_t>(compressed ? 1U : 0U);
  const auto packed = grpcxx::PackGrpcHeader(length, flag);
  const auto packed_bytes = PackedHeader(packed);
  ASSERT_GE(frame->WireBytes().size(), packed_bytes.size());
  EXPECT_TRUE(std::equal(packed_bytes.begin(), packed_bytes.end(), frame->WireBytes().begin()));
  EXPECT_EQ(grpcxx::UnpackGrpcHeaderLength(packed), length);

  grpcxx::LengthPrefixedFrame parser(static_cast<std::size_t>(length) + 1U);
  auto parsed = parser.Feed(frame->WireBytes());
  ASSERT_TRUE(parsed.has_value());
  ASSERT_EQ(parsed->size(), 1U);
  EXPECT_EQ(parsed->front().Payload().size(), length);
  EXPECT_EQ(parsed->front().Compressed(), compressed);
}

TEST(HeaderMirror, PackMatchesStoredLength) {
  for (const std::uint32_t length : {0U, 1U, 255U, 256U, 65535U, 1'000'000U}) {
    ExpectHeaderMatch(length, false);
    ExpectHeaderMatch(length, true);
  }

  constexpr std::uint32_t kOversized = 0xFFFFFFF0U;
  const auto packed = grpcxx::PackGrpcHeader(kOversized, 1U);
  const auto header = PackedHeader(packed);
  EXPECT_EQ(header[0], std::byte{1});
  EXPECT_EQ(grpcxx::UnpackGrpcHeaderLength(packed), kOversized);
  grpcxx::LengthPrefixedFrame parser(64);
  auto rejected = parser.Feed(header);
  EXPECT_FALSE(rejected.has_value());
  EXPECT_EQ(parser.State(), grpcxx::LengthPrefixedFrame::ParseState::kFailed);
}

}  // namespace
