#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded Zstd message compression with aligned payload ownership. */
class ZstdCompressor final {
 public:
  ZstdCompressor(const ZstdCompressor&) = delete;
  ZstdCompressor& operator=(const ZstdCompressor&) = delete;
  ZstdCompressor(ZstdCompressor&&) noexcept = default;
  ZstdCompressor& operator=(ZstdCompressor&&) noexcept = default;
  ~ZstdCompressor() noexcept = default;
  explicit ZstdCompressor(int level = 1) noexcept : level_(level) {}
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;

 private:
  int level_;
};
}  // namespace grpcxx
