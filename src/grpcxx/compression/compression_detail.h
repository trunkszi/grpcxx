#pragma once

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <expected>
#include <limits>
#include <ranges>
#include <span>
#include <string_view>
#include <vector>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx::compression_detail {
/** Canonical decompression limit failure. */
inline std::unexpected<Status> Exhausted() noexcept {
  return std::unexpected(
      Status(StatusCode::kResourceExhausted, "Decompressed message exceeds receive limit"));
}
/** Canonical invalid compressed message failure. */
inline std::unexpected<Status> Corrupt(std::string_view algorithm) noexcept {
  return std::unexpected(
      Status(StatusCode::kInternal, fmt::format("Invalid {} compressed message", algorithm)));
}
/** Bound all wire lengths to the gRPC 32-bit framing domain. */
constexpr std::size_t WireLimit(std::size_t limit) noexcept {
  if consteval {
    return limit < std::numeric_limits<std::uint32_t>::max()
               ? limit
               : std::numeric_limits<std::uint32_t>::max();
  } else {
    return std::min(limit, std::size_t{std::numeric_limits<std::uint32_t>::max()});
  }
}
/** Copy a completed backend result into the wire-aligned owning buffer. */
inline std::expected<AlignedMessageBuf, Status> Finish(std::span<const std::byte> bytes,
                                                       bool compressed) noexcept {
  if (bytes.size() > WireLimit(bytes.size())) return Exhausted();
  auto result = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(bytes.size()), compressed);
  if (!result) return std::unexpected(result.error());
  std::ranges::copy(bytes, result->Payload().data());
  return result;
}
}  // namespace grpcxx::compression_detail
