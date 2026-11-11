#pragma once

#include <cstdint>

namespace grpcxx {

/** A successful send owns the message even when the high-water mark is reached. */
enum class WriteStatus : std::uint8_t { kQueued, kHighWaterMarkReached };

/** Local and remote refer to the endpoint that owns the stream slot. */
enum class StreamState : std::uint8_t {
  kIdle,
  kOpen,
  kLocalHalfClosed,
  kRemoteHalfClosed,
  kClosed,
  kReset
};

}  // namespace grpcxx
