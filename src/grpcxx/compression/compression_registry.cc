#include "grpcxx/compression/compression_registry.h"

#include <array>
#include <ranges>

#include "grpcxx/compression/deflate_compressor.h"
#include "grpcxx/compression/gzip_compressor.h"
#include "grpcxx/compression/identity_compressor.h"
#include "grpcxx/compression/lz4_compressor.h"
#include "grpcxx/compression/snappy_compressor.h"
#include "grpcxx/compression/zstd_compressor.h"

namespace grpcxx {
namespace {
constexpr std::array kAlgorithms{CompressionAlgorithm::kIdentity, CompressionAlgorithm::kGzip,
                                 CompressionAlgorithm::kDeflate,  CompressionAlgorithm::kZstd,
                                 CompressionAlgorithm::kLz4,      CompressionAlgorithm::kSnappy};
std::string_view Trim(std::string_view token) noexcept {
  const auto first = token.find_first_not_of(" \t");
  if (first == std::string_view::npos) return {};
  return token.substr(first, token.find_last_not_of(" \t") - first + 1);
}
}  // namespace
CompressionRegistry::CompressionRegistry(std::size_t min_compress_size_bytes) noexcept
    : min_compress_size_bytes_(min_compress_size_bytes) {}
std::string_view CompressionRegistry::EncodingName(CompressionAlgorithm algorithm) noexcept {
  switch (algorithm) {
    case CompressionAlgorithm::kIdentity:
      return "identity";
    case CompressionAlgorithm::kGzip:
      return "gzip";
    case CompressionAlgorithm::kDeflate:
      return "deflate";
    case CompressionAlgorithm::kZstd:
      return "zstd";
    case CompressionAlgorithm::kLz4:
      return "lz4";
    case CompressionAlgorithm::kSnappy:
      return "snappy";
  }
  return {};
}
std::expected<CompressionAlgorithm, Status> CompressionRegistry::ParseEncoding(
    std::string_view encoding) noexcept {
  const auto token = Trim(encoding);
  for (const auto algorithm : kAlgorithms)
    if (EncodingName(algorithm) == token) return algorithm;
  return std::unexpected(Status(StatusCode::kUnimplemented, "Unsupported grpc-encoding"));
}
std::string_view CompressionRegistry::AcceptEncoding() noexcept {
  return "identity,gzip,deflate,zstd,lz4,snappy";
}
CompressionAlgorithm CompressionRegistry::Negotiate(
    std::string_view peer_accept_encoding) noexcept {
  for (const auto part : peer_accept_encoding | std::views::split(',')) {
    const std::string_view token(std::ranges::data(part), std::ranges::size(part));
    if (auto algorithm = ParseEncoding(token); algorithm) return *algorithm;
  }
  return CompressionAlgorithm::kIdentity;
}
std::expected<AlignedMessageBuf, Status> CompressionRegistry::Compress(
    CompressionAlgorithm algorithm, std::span<const std::byte> input) const noexcept {
  if (EncodingName(algorithm).empty())
    return std::unexpected(Status(StatusCode::kUnimplemented, "Unsupported compression algorithm"));
  if (input.size() < min_compress_size_bytes_) return IdentityCompressor{}.Compress(input);
  auto result = [&]() -> std::expected<AlignedMessageBuf, Status> {
    switch (algorithm) {
      case CompressionAlgorithm::kIdentity:
        return IdentityCompressor{}.Compress(input);
      case CompressionAlgorithm::kGzip:
        return GzipCompressor{}.Compress(input);
      case CompressionAlgorithm::kDeflate:
        return DeflateCompressor{}.Compress(input);
      case CompressionAlgorithm::kZstd:
        return ZstdCompressor{}.Compress(input);
      case CompressionAlgorithm::kLz4:
        return Lz4Compressor{}.Compress(input);
      case CompressionAlgorithm::kSnappy:
        return SnappyCompressor{}.Compress(input);
    }
    return std::unexpected(Status(StatusCode::kUnimplemented, "Unsupported compression algorithm"));
  }();
  if (!result) return result;
  if (result->Payload().size() >= input.size()) return IdentityCompressor{}.Compress(input);
  return result;
}
std::expected<AlignedMessageBuf, Status> CompressionRegistry::Decompress(
    CompressionAlgorithm algorithm, std::span<const std::byte> input,
    std::size_t max_receive_message_size) const noexcept {
  switch (algorithm) {
    case CompressionAlgorithm::kIdentity:
      return IdentityCompressor{}.Decompress(input, max_receive_message_size);
    case CompressionAlgorithm::kGzip:
      return GzipCompressor{}.Decompress(input, max_receive_message_size);
    case CompressionAlgorithm::kDeflate:
      return DeflateCompressor{}.Decompress(input, max_receive_message_size);
    case CompressionAlgorithm::kZstd:
      return ZstdCompressor{}.Decompress(input, max_receive_message_size);
    case CompressionAlgorithm::kLz4:
      return Lz4Compressor{}.Decompress(input, max_receive_message_size);
    case CompressionAlgorithm::kSnappy:
      return SnappyCompressor{}.Decompress(input, max_receive_message_size);
  }
  return std::unexpected(Status(StatusCode::kUnimplemented, "Unsupported compression algorithm"));
}
}  // namespace grpcxx
