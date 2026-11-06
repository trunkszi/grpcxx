#include "grpcxx/core/status.h"

#include <fmt/format.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

/** RFC 3986 unreserved characters: ALPHA / DIGIT / "-" / "." / "_" / "~". */
[[nodiscard]] constexpr bool IsUnreserved(unsigned char byte) noexcept {
  return (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z') ||
         (byte >= '0' && byte <= '9') || byte == '-' || byte == '.' || byte == '_' || byte == '~';
}

constexpr std::string_view kHexDigits{"0123456789ABCDEF"};

/** Returns the value of one hexadecimal digit, or -1 when `digit` is not hexadecimal. */
[[nodiscard]] constexpr int HexValue(char digit) noexcept {
  if (digit >= '0' && digit <= '9') {
    return digit - '0';
  }
  if (digit >= 'A' && digit <= 'F') {
    return digit - 'A' + 10;
  }
  if (digit >= 'a' && digit <= 'f') {
    return digit - 'a' + 10;
  }
  return -1;
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeTruncatedPercentEncodingStatus(
    std::size_t index) noexcept {
  uarch::cold_path();
  return Status(StatusCode::kInvalidArgument,
                fmt::format("truncated percent-encoding at offset {}", index));
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeInvalidPercentEncodingStatus(
    std::size_t index) noexcept {
  uarch::cold_path();
  return Status(StatusCode::kInvalidArgument,
                fmt::format("invalid percent-encoding at offset {}", index));
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeNonOkHttpStatus(int http_status) noexcept {
  uarch::cold_path();
  StatusCode code = StatusCode::kUnknown;
  switch (http_status) {
    case 400:
      code = StatusCode::kInternal;
      break;
    case 401:
      code = StatusCode::kUnauthenticated;
      break;
    case 403:
      code = StatusCode::kPermissionDenied;
      break;
    case 404:
      code = StatusCode::kUnimplemented;
      break;
    case 429:
    case 502:
    case 503:
    case 504:
      code = StatusCode::kUnavailable;
      break;
    default:
      code = StatusCode::kUnknown;
      break;
  }
  return Status(code, fmt::format("HTTP status {}", http_status));
}

}  // namespace

std::string Status::PercentEncode(std::string_view input) noexcept {
  std::string encoded;
  encoded.reserve(input.size());

  for (const char raw : input) {
    const auto byte = static_cast<unsigned char>(raw);
    if (IsUnreserved(byte)) {
      encoded.push_back(raw);
      continue;
    }

    encoded.push_back('%');
    encoded.push_back(kHexDigits[byte >> 4U]);
    encoded.push_back(kHexDigits[byte & 0x0FU]);
  }

  return encoded;
}

std::expected<std::string, Status> Status::PercentDecode(std::string_view input) noexcept {
  std::string decoded;
  decoded.reserve(input.size());

  for (std::size_t index = 0; index < input.size(); ++index) {
    if (input[index] != '%') {
      decoded.push_back(input[index]);
      continue;
    }

    if (uarch::unlikely(input.size() - index < 3U)) {
      return std::unexpected(MakeTruncatedPercentEncodingStatus(index));
    }

    const int high = HexValue(input[index + 1U]);
    const int low = HexValue(input[index + 2U]);
    if (uarch::unlikely(high < 0 || low < 0)) {
      return std::unexpected(MakeInvalidPercentEncodingStatus(index));
    }

    decoded.push_back(static_cast<char>((high << 4U) | low));
    index += 2U;
  }

  return decoded;
}

Status Status::FromHttpStatus(int http_status) noexcept {
  if (uarch::likely(http_status >= 200 && http_status <= 299)) {
    return Status{};
  }
  return MakeNonOkHttpStatus(http_status);
}

}  // namespace grpcxx
