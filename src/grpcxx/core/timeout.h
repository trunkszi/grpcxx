#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "grpcxx/core/status.h"

namespace grpcxx {

/**
 * Codec for the `grpc-timeout` request header.
 *
 * The wire format is one to eight decimal digits followed by a case-sensitive unit: `H` hours,
 * `M` minutes, `S` seconds, `m` milliseconds, `u` microseconds or `n` nanoseconds. Formatting
 * rounds up so a client never receives a deadline shorter than the requested one.
 */
class GrpcTimeout final {
 public:
  /**
   * Formats `nanoseconds` into `buffer` and returns the written characters.
   *
   * The finest unit whose count still fits the eight-digit wire limit is chosen, and the
   * count is rounded up so the encoded timeout never elapses early. The result is at most
   * nine characters. Returns an empty view when `buffer` cannot hold the whole encoding.
   */
  [[nodiscard]] static std::string_view FormatTo(std::uint64_t nanoseconds,
                                                 std::span<char> buffer) noexcept;

  /**
   * Parses a `grpc-timeout` value into nanoseconds.
   *
   * Accepts one to eight decimal digits followed by one of `H`, `M`, `S`, `m`, `u` or `n`;
   * signs, whitespace, extra digits and unknown units are rejected. Returns kInvalidArgument
   * for malformed input and for values that do not fit in a `std::uint64_t` nanosecond count.
   */
  [[nodiscard]] static std::expected<std::uint64_t, Status> Parse(std::string_view value) noexcept;
};

}  // namespace grpcxx
