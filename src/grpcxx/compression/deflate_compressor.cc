#include "grpcxx/compression/deflate_compressor.h"

#include <zlib.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <vector>

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> DeflateCompressor::Compress(
    std::span<const std::byte> input) const noexcept {
  if (input.size() > std::numeric_limits<uInt>::max()) return compression_detail::Exhausted();
  z_stream stream{};
  if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15, 8, Z_DEFAULT_STRATEGY) != Z_OK)
    return compression_detail::Exhausted();
  const auto cleanup = [](z_stream* value) noexcept { deflateEnd(value); };
  const std::unique_ptr<z_stream, decltype(cleanup)> guard(&stream, cleanup);
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(input.data()));
  stream.avail_in = static_cast<uInt>(input.size());
  std::vector<std::byte> bytes;
  std::array<std::byte, 65536> chunk{};
  for (;;) {
    stream.next_out = reinterpret_cast<Bytef*>(chunk.data());
    stream.avail_out = static_cast<uInt>(chunk.size());
    const auto code = deflate(&stream, Z_FINISH);
    if (code != Z_OK && code != Z_STREAM_END) return compression_detail::Corrupt("deflate");
    bytes.insert_range(bytes.end(), std::span(chunk).first(chunk.size() - stream.avail_out));
    if (code == Z_STREAM_END) return compression_detail::Finish(bytes, true);
    if (bytes.size() > compression_detail::WireLimit(bytes.size()))
      return compression_detail::Exhausted();
  }
}
std::expected<AlignedMessageBuf, Status> DeflateCompressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  if (input.size() > std::numeric_limits<uInt>::max()) return compression_detail::Exhausted();
  const auto limit = compression_detail::WireLimit(max_receive_message_size);
  z_stream stream{};
  if (inflateInit2(&stream, 15) != Z_OK) return compression_detail::Exhausted();
  const auto cleanup = [](z_stream* value) noexcept { inflateEnd(value); };
  const std::unique_ptr<z_stream, decltype(cleanup)> guard(&stream, cleanup);
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(input.data()));
  stream.avail_in = static_cast<uInt>(input.size());
  std::vector<std::byte> bytes;
  std::array<std::byte, 65536> chunk{};
  for (;;) {
    /** A single sentinel byte distinguishes an exact-size stream from a bomb. */
    const auto room = std::min(chunk.size(), limit - bytes.size() + std::size_t{1});
    stream.next_out = reinterpret_cast<Bytef*>(chunk.data());
    stream.avail_out = static_cast<uInt>(room);
    const auto prior = stream.avail_in;
    const auto code = inflate(&stream, Z_NO_FLUSH);
    const auto produced = room - stream.avail_out;
    if (produced > limit - bytes.size()) return compression_detail::Exhausted();
    bytes.insert_range(bytes.end(), std::span(chunk).first(produced));
    if (code == Z_STREAM_END) {
      if (stream.avail_in != 0) return compression_detail::Corrupt("deflate");
      return compression_detail::Finish(bytes, false);
    }
    if (code != Z_OK || (prior == stream.avail_in && produced == 0))
      return compression_detail::Corrupt("deflate");
  }
}
}  // namespace grpcxx
