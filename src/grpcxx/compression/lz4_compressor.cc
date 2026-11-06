#include "grpcxx/compression/lz4_compressor.h"

#include <lz4frame.h>

#include <algorithm>
#include <array>
#include <memory>
#include <vector>

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> Lz4Compressor::Compress(
    std::span<const std::byte> input) const noexcept {
  if (input.size() > compression_detail::WireLimit(input.size()))
    return compression_detail::Exhausted();
  /** LZ4 frames carry length and checksums, unlike untrusted raw LZ4 blocks. */
  LZ4F_preferences_t preferences{};
  preferences.frameInfo.contentSize = input.size();
  preferences.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
  preferences.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
  std::vector<std::byte> bytes(LZ4F_compressFrameBound(input.size(), &preferences));
  const auto size =
      LZ4F_compressFrame(bytes.data(), bytes.size(), input.data(), input.size(), &preferences);
  if (LZ4F_isError(size)) return compression_detail::Corrupt("lz4");
  bytes.resize(size);
  return compression_detail::Finish(bytes, true);
}
std::expected<AlignedMessageBuf, Status> Lz4Compressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  const auto limit = compression_detail::WireLimit(max_receive_message_size);
  LZ4F_dctx* raw = nullptr;
  if (LZ4F_isError(LZ4F_createDecompressionContext(&raw, LZ4F_VERSION)))
    return compression_detail::Exhausted();
  const std::unique_ptr<LZ4F_dctx, decltype(&LZ4F_freeDecompressionContext)> context(
      raw, LZ4F_freeDecompressionContext);
  LZ4F_frameInfo_t info{};
  auto consumed = input.size();
  const auto header = LZ4F_getFrameInfo(context.get(), &info, input.data(), &consumed);
  if (LZ4F_isError(header) || info.frameType != LZ4F_frame)
    return compression_detail::Corrupt("lz4");
  if (info.contentSize > limit) return compression_detail::Exhausted();
  std::vector<std::byte> bytes;
  std::array<std::byte, 65536> chunk{};
  std::size_t position = consumed;
  for (;;) {
    auto source_size = input.size() - position;
    auto target_size = std::min(chunk.size(), limit - bytes.size() + std::size_t{1});
    const auto remaining = LZ4F_decompress(context.get(), chunk.data(), &target_size,
                                           input.data() + position, &source_size, nullptr);
    if (LZ4F_isError(remaining)) return compression_detail::Corrupt("lz4");
    if (target_size > limit - bytes.size()) return compression_detail::Exhausted();
    bytes.insert_range(bytes.end(), std::span(chunk).first(target_size));
    position += source_size;
    if (remaining == 0) {
      if (position != input.size()) return compression_detail::Corrupt("lz4");
      return compression_detail::Finish(bytes, false);
    }
    if (source_size == 0 && target_size == 0) return compression_detail::Corrupt("lz4");
  }
}
}  // namespace grpcxx
