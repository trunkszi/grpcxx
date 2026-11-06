#include "grpcxx/compression/identity_compressor.h"

#include "grpcxx/compression/compression_detail.h"

namespace grpcxx {
std::expected<AlignedMessageBuf, Status> IdentityCompressor::Compress(
    std::span<const std::byte> input) const noexcept {
  return compression_detail::Finish(input, false);
}
std::expected<AlignedMessageBuf, Status> IdentityCompressor::Decompress(
    std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept {
  if (input.size() > compression_detail::WireLimit(max_receive_message_size))
    return compression_detail::Exhausted();
  return compression_detail::Finish(input, false);
}
}  // namespace grpcxx
