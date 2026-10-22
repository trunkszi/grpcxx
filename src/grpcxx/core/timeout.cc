#include "grpcxx/core/timeout.h"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <string_view>

namespace grpcxx {
namespace {

struct TimeoutUnit final {
  std::uint64_t nanos;
  char suffix;
};

/**
 * Ordered finest to coarsest. Formatting picks the first unit whose count fits the wire limit,
 * which maximizes precision while still encoding the deadline in at most eight digits.
 */
constexpr std::array<TimeoutUnit, 6> kUnits{{
    {1ULL, 'n'},
    {1'000ULL, 'u'},
    {1'000'000ULL, 'm'},
    {1'000'000'000ULL, 'S'},
    {60'000'000'000ULL, 'M'},
    {3'600'000'000'000ULL, 'H'},
}};

/** A `grpc-timeout` value carries at most eight decimal digits. */
constexpr std::uint64_t kMaxWireCount = 99'999'999ULL;

/** Ceiling division that cannot overflow for a `std::uint64_t` numerator. */
[[nodiscard]] constexpr std::uint64_t DivideCeil(std::uint64_t numerator,
                                                 std::uint64_t denominator) noexcept {
  const std::uint64_t quotient = numerator / denominator;
  return quotient + (numerator % denominator != 0U ? 1ULL : 0ULL);
}

[[nodiscard]] constexpr bool IsDecimalDigit(char character) noexcept {
  return character >= '0' && character <= '9';
}

}  // namespace

std::string_view GrpcTimeout::FormatTo(std::uint64_t nanoseconds, std::span<char> buffer) noexcept {
  if (buffer.empty()) {
    return {};
  }

  for (const TimeoutUnit& unit : kUnits) {
    if (const std::uint64_t count = DivideCeil(nanoseconds, unit.nanos); count <= kMaxWireCount) {
      const auto written =
          fmt::format_to_n(buffer.data(), buffer.size(), "{}{}", count, unit.suffix);
      if (written.size > buffer.size()) {
        return {};
      }
      return std::string_view{buffer.data(), written.size};
    }
  }

  return {};
}

std::expected<std::uint64_t, Status> GrpcTimeout::Parse(std::string_view value) noexcept {
  if (value.size() < 2U || value.size() > 9U) {
    return std::unexpected(
        Status(StatusCode::kInvalidArgument,
               fmt::format("grpc-timeout '{}' must be 1-8 digits followed by a unit", value)));
  }

  std::uint64_t nanos_per_unit = 0;
  switch (value.back()) {
    case 'H':
      nanos_per_unit = 3'600'000'000'000ULL;
      break;
    case 'M':
      nanos_per_unit = 60'000'000'000ULL;
      break;
    case 'S':
      nanos_per_unit = 1'000'000'000ULL;
      break;
    case 'm':
      nanos_per_unit = 1'000'000ULL;
      break;
    case 'u':
      nanos_per_unit = 1'000ULL;
      break;
    case 'n':
      nanos_per_unit = 1ULL;
      break;
    default:
      return std::unexpected(Status(StatusCode::kInvalidArgument,
                                    fmt::format("grpc-timeout '{}' has an unknown unit", value)));
  }

  const std::string_view digits = value.substr(0, value.size() - 1U);
  if (!std::ranges::all_of(digits, IsDecimalDigit)) {
    return std::unexpected(Status(
        StatusCode::kInvalidArgument,
        fmt::format("grpc-timeout '{}' must contain only decimal digits before the unit", value)));
  }

  std::uint64_t count = 0;
  for (const char digit : digits) {
    count = count * 10U + static_cast<std::uint64_t>(digit - '0');
  }

  if (const std::uint64_t limit = std::numeric_limits<std::uint64_t>::max() / nanos_per_unit;
      count > limit) {
    return std::unexpected(
        Status(StatusCode::kInvalidArgument,
               fmt::format("grpc-timeout '{}' exceeds the nanosecond range", value)));
  }

  return count * nanos_per_unit;
}

}  // namespace grpcxx
