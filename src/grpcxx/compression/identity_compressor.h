#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** Bounded Identity message compression with aligned payload ownership. */
class IdentityCompressor final {
 public:
  IdentityCompressor() noexcept = default;
  IdentityCompressor(const IdentityCompressor&) = delete;
  IdentityCompressor& operator=(const IdentityCompressor&) = delete;
  IdentityCompressor(IdentityCompressor&&) noexcept = default;
  IdentityCompressor& operator=(IdentityCompressor&&) noexcept = default;
  ~IdentityCompressor() noexcept = default;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      std::span<const std::byte> input) const noexcept;
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      std::span<const std::byte> input, std::size_t max_receive_message_size) const noexcept;
};
}  // namespace grpcxx
