#include "grpcxx/compression/snappy_compressor.h"

#include <snappy-c.h>

#include <uarch/uarch.hpp>
#include <vector>

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> SnappyCompressor::Compress(
    std::span<const std::byte> input) const noexcept {
  if (uarch::unlikely(input.size() > compression_detail::WireLimit(input.size())))
    return compression_detail::Exhausted();
  std::vector<std::byte> bytes(snappy_max_compressed_length(input.size()));
  auto size = bytes.size();
  const auto* in_chars = static_cast<const char*>(static_cast<const void*>(input.data()));
  auto* out_chars = static_cast<char*>(static_cast<void*>(bytes.data()));
  if (uarch::unlikely(snappy_compress(in_chars, input.size(), out_chars, &size) != SNAPPY_OK))
    return compression_detail::Corrupt("snappy");
  bytes.resize(size);
  return compression_detail::Finish(bytes, true);
}
std::expected<AlignedMessageBuf, Status> SnappyCompressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  std::size_t size = 0;
  const auto* in_chars = static_cast<const char*>(static_cast<const void*>(input.data()));
  if (uarch::unlikely(snappy_uncompressed_length(in_chars, input.size(), &size) != SNAPPY_OK))
    return compression_detail::Corrupt("snappy");
  if (uarch::unlikely(size > compression_detail::WireLimit(max_receive_message_size)))
    return compression_detail::Exhausted();
  if (uarch::unlikely(snappy_validate_compressed_buffer(in_chars, input.size()) != SNAPPY_OK))
    return compression_detail::Corrupt("snappy");
  auto result = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(size));
  if (uarch::unlikely(!result)) return std::unexpected(result.error());
  auto* out_chars = static_cast<char*>(static_cast<void*>(result->Payload().data()));
  if (uarch::unlikely(snappy_uncompress(in_chars, input.size(), out_chars, &size) != SNAPPY_OK))
    return compression_detail::Corrupt("snappy");
  return result;
}
}  // namespace grpcxx
