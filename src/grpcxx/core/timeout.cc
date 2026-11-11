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
#include <uarch/uarch.hpp>

namespace grpcxx {
namespace {

struct TimeoutUnit final {
  uarch::FastDivisorU64 divisor;
  char suffix;
};

/**
 * Ordered finest to coarsest. Formatting picks the first unit whose count fits the wire limit,
 * which maximizes precision while still encoding the deadline in at most eight digits.
 */
constexpr std::array<TimeoutUnit, 6> kUnits{{
    {uarch::arithmetic::divisor_u64<1ULL>(), 'n'},
    {uarch::arithmetic::divisor_u64<1'000ULL>(), 'u'},
    {uarch::arithmetic::divisor_u64<1'000'000ULL>(), 'm'},
    {uarch::arithmetic::divisor_u64<1'000'000'000ULL>(), 'S'},
    {uarch::arithmetic::divisor_u64<60'000'000'000ULL>(), 'M'},
    {uarch::arithmetic::divisor_u64<3'600'000'000'000ULL>(), 'H'},
}};

/** A `grpc-timeout` value carries at most eight decimal digits. */
constexpr std::uint64_t kMaxWireCount = 99'999'999ULL;

/** Ceiling division using reciprocals that cannot overflow for a `std::uint64_t` numerator. */
[[nodiscard]] constexpr std::uint64_t DivideCeil(
    std::uint64_t numerator, const uarch::FastDivisorU64& denominator) noexcept {
  const auto [quotient, remainder] = denominator.div_rem(numerator);
  return quotient + (remainder != 0U ? 1ULL : 0ULL);
}

[[nodiscard]] constexpr bool IsDecimalDigit(char character) noexcept {
  return uarch::eager_all(character >= '0', character <= '9');
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeTimeoutLengthError(
    std::string_view value) noexcept {
  uarch::cold_path();
  return Status(StatusCode::kInvalidArgument,
                fmt::format("grpc-timeout '{}' must be 1-8 digits followed by a unit", value));
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeTimeoutUnknownUnitError(
    std::string_view value) noexcept {
  uarch::cold_path();
  return Status(StatusCode::kInvalidArgument,
                fmt::format("grpc-timeout '{}' has an unknown unit", value));
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeTimeoutNonDigitError(
    std::string_view value) noexcept {
  uarch::cold_path();
  return Status(
      StatusCode::kInvalidArgument,
      fmt::format("grpc-timeout '{}' must contain only decimal digits before the unit", value));
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeTimeoutOverflowError(
    std::string_view value) noexcept {
  uarch::cold_path();
  return Status(StatusCode::kInvalidArgument,
                fmt::format("grpc-timeout '{}' exceeds the nanosecond range", value));
}

}  // namespace

std::string_view GrpcTimeout::FormatTo(std::uint64_t nanoseconds, std::span<char> buffer) noexcept {
  if (uarch::unlikely(buffer.empty())) {
    return {};
  }

  for (const TimeoutUnit& unit : kUnits) {
    if (const std::uint64_t count = DivideCeil(nanoseconds, unit.divisor); count <= kMaxWireCount) {
      const auto written =
          fmt::format_to_n(buffer.data(), buffer.size(), "{}{}", count, unit.suffix);
      if (uarch::unlikely(written.size > buffer.size())) {
        return {};
      }
      return std::string_view{buffer.data(), written.size};
    }
  }

  return {};
}

std::expected<std::uint64_t, Status> GrpcTimeout::Parse(std::string_view value) noexcept {
  if (uarch::unlikely(value.size() < 2U || value.size() > 9U)) {
    return std::unexpected(MakeTimeoutLengthError(value));
  }

  std::uint64_t nanos_per_unit = 0;
  std::uint64_t max_count = 0;
  switch (value.back()) {
    case 'H':
      nanos_per_unit = 3'600'000'000'000ULL;
      max_count = std::numeric_limits<std::uint64_t>::max() / 3'600'000'000'000ULL;
      break;
    case 'M':
      nanos_per_unit = 60'000'000'000ULL;
      max_count = std::numeric_limits<std::uint64_t>::max() / 60'000'000'000ULL;
      break;
    case 'S':
      nanos_per_unit = 1'000'000'000ULL;
      max_count = std::numeric_limits<std::uint64_t>::max() / 1'000'000'000ULL;
      break;
    case 'm':
      nanos_per_unit = 1'000'000ULL;
      max_count = std::numeric_limits<std::uint64_t>::max() / 1'000'000ULL;
      break;
    case 'u':
      nanos_per_unit = 1'000ULL;
      max_count = std::numeric_limits<std::uint64_t>::max() / 1'000ULL;
      break;
    case 'n':
      nanos_per_unit = 1ULL;
      max_count = std::numeric_limits<std::uint64_t>::max();
      break;
    default:
      return std::unexpected(MakeTimeoutUnknownUnitError(value));
  }

  const std::string_view digits = value.substr(0, value.size() - 1U);
  if (uarch::unlikely(!std::ranges::all_of(digits, IsDecimalDigit))) {
    return std::unexpected(MakeTimeoutNonDigitError(value));
  }

  std::uint64_t count = 0;
  for (const char digit : digits) {
    count = count * 10U + static_cast<std::uint64_t>(digit - '0');
  }

  if (uarch::unlikely(count > max_count)) {
    return std::unexpected(MakeTimeoutOverflowError(value));
  }

  return count * nanos_per_unit;
}

}  // namespace grpcxx
