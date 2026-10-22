#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded Deflate message compression with aligned payload ownership. */
class DeflateCompressor final {
 public:
  DeflateCompressor() noexcept = default;
  DeflateCompressor(const DeflateCompressor&) = delete;
  DeflateCompressor& operator=(const DeflateCompressor&) = delete;
  DeflateCompressor(DeflateCompressor&&) noexcept = default;
  DeflateCompressor& operator=(DeflateCompressor&&) noexcept = default;
  ~DeflateCompressor() noexcept = default;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;
};
}  // namespace grpcxx
