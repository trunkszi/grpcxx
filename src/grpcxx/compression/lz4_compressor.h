#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded LZ4 frame compression with aligned payload ownership.
 * Uses the standard LZ4 frame format with content and block checksums.
 * Raw LZ4 blocks are rejected because they cannot carry a verifiable size. */
class Lz4Compressor final {
 public:
  Lz4Compressor() noexcept = default;
  Lz4Compressor(const Lz4Compressor&) = delete;
  Lz4Compressor& operator=(const Lz4Compressor&) = delete;
  Lz4Compressor(Lz4Compressor&&) noexcept = default;
  Lz4Compressor& operator=(Lz4Compressor&&) noexcept = default;
  ~Lz4Compressor() noexcept = default;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;
};
}  // namespace grpcxx
