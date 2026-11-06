#include "grpcxx/compression/zstd_compressor.h"

#include <zstd.h>
#include <zstd_errors.h>

#include <algorithm>
#include <array>
#include <bit>
#include <memory>
#include <vector>

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> ZstdCompressor::Compress(
    std::span<const std::byte> input) const noexcept {
  if (input.size() > compression_detail::WireLimit(input.size()))
    return compression_detail::Exhausted();
  std::vector<std::byte> bytes(ZSTD_compressBound(input.size()));
  const std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(),
                                                                     ZSTD_freeCCtx);
  if (!context) return compression_detail::Exhausted();
  if (ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, level_)) ||
      ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1)))
    return compression_detail::Corrupt("zstd");
  const auto size =
      ZSTD_compress2(context.get(), bytes.data(), bytes.size(), input.data(), input.size());
  if (ZSTD_isError(size)) return compression_detail::Corrupt("zstd");
  bytes.resize(size);
  return compression_detail::Finish(bytes, true);
}
std::expected<AlignedMessageBuf, Status> ZstdCompressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  const auto limit = compression_detail::WireLimit(max_receive_message_size);
  const auto declared = ZSTD_getFrameContentSize(input.data(), input.size());
  if (declared == ZSTD_CONTENTSIZE_ERROR) return compression_detail::Corrupt("zstd");
  if (declared != ZSTD_CONTENTSIZE_UNKNOWN && declared > limit)
    return compression_detail::Exhausted();
  const std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context(ZSTD_createDCtx(),
                                                                     ZSTD_freeDCtx);
  if (!context) return compression_detail::Exhausted();
  /** Bound hostile frame window requirements independently of advertised size. */
  const auto window_log = std::clamp(std::bit_width(std::max(std::size_t{1}, limit) - 1), 10, 31);
  if (ZSTD_isError(
          ZSTD_DCtx_setParameter(context.get(), ZSTD_d_windowLogMax, static_cast<int>(window_log))))
    return compression_detail::Corrupt("zstd");
  ZSTD_inBuffer source{input.data(), input.size(), 0};
  std::vector<std::byte> bytes;
  std::array<std::byte, 65536> chunk{};
  for (;;) {
    const auto room = std::min(chunk.size(), limit - bytes.size() + std::size_t{1});
    ZSTD_outBuffer target{chunk.data(), room, 0};
    const auto prior = source.pos;
    const auto remaining = ZSTD_decompressStream(context.get(), &target, &source);
    if (ZSTD_isError(remaining)) {
      if (ZSTD_getErrorCode(remaining) == ZSTD_error_frameParameter_windowTooLarge ||
          ZSTD_getErrorCode(remaining) == ZSTD_error_memory_allocation)
        return compression_detail::Exhausted();
      return compression_detail::Corrupt("zstd");
    }
    if (target.pos > limit - bytes.size()) return compression_detail::Exhausted();
    bytes.insert_range(bytes.end(), std::span(chunk).first(target.pos));
    if (remaining == 0) {
      if (source.pos != source.size) return compression_detail::Corrupt("zstd");
      return compression_detail::Finish(bytes, false);
    }
    if (prior == source.pos && target.pos == 0) return compression_detail::Corrupt("zstd");
  }
}
}  // namespace grpcxx
