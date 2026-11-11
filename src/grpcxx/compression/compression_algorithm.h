#pragma once

#include <cstdint>

namespace grpcxx {
/** Message-level compression algorithms negotiated by gRPC metadata. */
enum class CompressionAlgorithm : std::uint8_t { kIdentity, kGzip, kDeflate, kZstd, kLz4, kSnappy };
}  // namespace grpcxx
