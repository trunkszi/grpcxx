#pragma once

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <string_view>

namespace grpcxx::h2_detail {
inline constexpr std::string_view kGenericContentType = "application/grpc";
inline constexpr std::size_t kMaxContentTypeBytes = 127;

/** Accept bounded gRPC media types without parameters or header delimiters. */
constexpr bool ValidContentType(std::string_view value) noexcept {
  if (value == kGenericContentType) return true;
  constexpr std::string_view kPrefix = "application/grpc+";
  if (!value.starts_with(kPrefix) || value.size() <= kPrefix.size() ||
      value.size() > kMaxContentTypeBytes)
    return false;
  return std::ranges::all_of(value.substr(kPrefix.size()), [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
           c == '_' || c == '.' || c == '+';
  });
}

/** Generic sessions accept any valid gRPC type; typed sessions require equality. */
constexpr bool AcceptsContentType(std::string_view expected, std::string_view received) noexcept {
  return ValidContentType(received) && (expected == kGenericContentType || received == expected);
}
}  // namespace grpcxx::h2_detail
