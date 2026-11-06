#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded Snappy message compression with aligned payload ownership. */
class SnappyCompressor final {
 public:
  SnappyCompressor() noexcept = default;
  SnappyCompressor(const SnappyCompressor&) = delete;
  SnappyCompressor& operator=(const SnappyCompressor&) = delete;
  SnappyCompressor(SnappyCompressor&&) noexcept = default;
  SnappyCompressor& operator=(SnappyCompressor&&) noexcept = default;
  ~SnappyCompressor() noexcept = default;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;
};
}  // namespace grpcxx
