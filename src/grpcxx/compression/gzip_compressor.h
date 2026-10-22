#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded Gzip message compression with aligned payload ownership. */
class GzipCompressor final {
 public:
  GzipCompressor() noexcept = default;
  GzipCompressor(const GzipCompressor&) = delete;
  GzipCompressor& operator=(const GzipCompressor&) = delete;
  GzipCompressor(GzipCompressor&&) noexcept = default;
  GzipCompressor& operator=(GzipCompressor&&) noexcept = default;
  ~GzipCompressor() noexcept = default;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;
};
}  // namespace grpcxx
