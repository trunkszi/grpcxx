#pragma once

#include <cstddef>
#include <expected>
#include <span>
#include <string_view>

#include "grpcxx/compression/compression_algorithm.h"
#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {
/** gRPC compression negotiation and adaptive, aligned message encoding. */
class CompressionRegistry final {
 public:
  CompressionRegistry(const CompressionRegistry&) = delete;
  CompressionRegistry& operator=(const CompressionRegistry&) = delete;
  CompressionRegistry(CompressionRegistry&&) noexcept = default;
  CompressionRegistry& operator=(CompressionRegistry&&) noexcept = default;
  ~CompressionRegistry() noexcept = default;
  explicit CompressionRegistry(std::size_t min_compress_size_bytes = 256) noexcept;
  /** Return the canonical grpc-encoding token. */
  [[nodiscard]] static std::string_view EncodingName(CompressionAlgorithm algorithm) noexcept;
  /** Parse one grpc-encoding token; unsupported or malformed tokens fail closed. */
  [[nodiscard]] static std::expected<CompressionAlgorithm, Status> ParseEncoding(
      std::string_view encoding) noexcept;
  /** All algorithms available in this registry, suitable for grpc-accept-encoding. */
  [[nodiscard]] static std::string_view AcceptEncoding() noexcept;
  /** Choose the first recognized token in the peer's comma-separated preference order. */
  [[nodiscard]] static CompressionAlgorithm Negotiate(
      std::string_view peer_accept_encoding) noexcept;
  /** Bypass small or incompressible payloads and preserve the compressed flag. */
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Compress(
      CompressionAlgorithm algorithm, std::span<const std::byte> input) const noexcept;
  /** Decode the negotiated algorithm with a mandatory receive limit. */
  [[nodiscard]] std::expected<AlignedMessageBuf, Status> Decompress(
      CompressionAlgorithm algorithm, std::span<const std::byte> input,
      std::size_t max_receive_message_size) const noexcept;

 private:
  std::size_t min_compress_size_bytes_;
};
}  // namespace grpcxx
