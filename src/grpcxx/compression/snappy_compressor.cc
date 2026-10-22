#include "grpcxx/compression/snappy_compressor.h"

#include <snappy-c.h>

#include <vector>

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> SnappyCompressor::Compress(
    std::span<const std::byte> input) const noexcept {
  if (input.size() > compression_detail::WireLimit(input.size()))
    return compression_detail::Exhausted();
  std::vector<std::byte> bytes(snappy_max_compressed_length(input.size()));
  auto size = bytes.size();
  if (snappy_compress(reinterpret_cast<const char*>(input.data()), input.size(),
                      reinterpret_cast<char*>(bytes.data()), &size) != SNAPPY_OK)
    return compression_detail::Corrupt("snappy");
  bytes.resize(size);
  return compression_detail::Finish(bytes, true);
}
std::expected<AlignedMessageBuf, Status> SnappyCompressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  std::size_t size = 0;
  if (snappy_uncompressed_length(reinterpret_cast<const char*>(input.data()), input.size(),
                                 &size) != SNAPPY_OK)
    return compression_detail::Corrupt("snappy");
  if (size > compression_detail::WireLimit(max_receive_message_size))
    return compression_detail::Exhausted();
  if (snappy_validate_compressed_buffer(reinterpret_cast<const char*>(input.data()),
                                        input.size()) != SNAPPY_OK)
    return compression_detail::Corrupt("snappy");
  auto result = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(size));
  if (!result) return std::unexpected(result.error());
  if (snappy_uncompress(reinterpret_cast<const char*>(input.data()), input.size(),
                        reinterpret_cast<char*>(result->Payload().data()), &size) != SNAPPY_OK)
    return compression_detail::Corrupt("snappy");
  return result;
}
}  // namespace grpcxx
