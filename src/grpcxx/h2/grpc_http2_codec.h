#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"

namespace grpcxx::h2 {

[[nodiscard, gnu::cold, gnu::noinline]] inline Status MakeWireStatus(
    StatusCode code, std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard]] inline std::optional<int> ParseDecimalInt(std::string_view text) noexcept {
  if (text.empty()) return std::nullopt;
  int parsed = 0;
  const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (ec != std::errc{} || ptr != text.data() + text.size()) return std::nullopt;
  return parsed;
}

[[nodiscard]] inline std::expected<StatusCode, Status> ParseGrpcStatusCode(
    std::string_view text) noexcept {
  const auto numeric = ParseDecimalInt(text);
  if (uarch::unlikely(!numeric.has_value() || *numeric < 0 || *numeric > 16)) {
    return std::unexpected(MakeWireStatus(StatusCode::kInternal, "invalid grpc-status trailer"));
  }
  return static_cast<StatusCode>(*numeric);
}

[[nodiscard]] inline std::string_view FormatGrpcStatusCode(StatusCode code,
                                                           std::span<char, 4> scratch) noexcept {
  const auto raw = static_cast<unsigned>(std::to_underlying(code));
  const auto [end, ec] = std::to_chars(scratch.data(), scratch.data() + scratch.size(), raw);
  if (uarch::unlikely(ec != std::errc{})) return "13";
  return std::string_view{scratch.data(), static_cast<std::size_t>(end - scratch.data())};
}

[[nodiscard]] constexpr bool IsAsciiVisibleToken(std::string_view text) noexcept {
  return std::ranges::all_of(text, [](char ch) noexcept {
    return static_cast<unsigned char>(ch) > 32U && static_cast<unsigned char>(ch) < 127U;
  });
}

[[nodiscard]] constexpr bool IsValidRpcPath(std::string_view path) noexcept {
  return path.starts_with('/') && path.size() <= 8192uz && IsAsciiVisibleToken(path) &&
         !path.contains('?') && !path.contains('#') &&
         path.find('/', 1uz) != std::string_view::npos;
}

[[nodiscard]] constexpr bool IsReservedClientMetadataKey(std::string_view key) noexcept {
  return key.starts_with(':') || key.starts_with("grpc-") || key == "content-type" || key == "te" ||
         key == "host" || key == "connection" || key == "transfer-encoding";
}

[[nodiscard]] constexpr bool IsReservedServerMetadataKey(std::string_view key) noexcept {
  return key.starts_with(':') || key == "content-type" || key == "grpc-encoding" ||
         key == "grpc-accept-encoding" || key == "grpc-status" || key == "grpc-message";
}

[[nodiscard]] constexpr std::string_view TrimOptionalWhitespace(std::string_view text) noexcept {
  while (text.starts_with(' ') || text.starts_with('\t')) text.remove_prefix(1uz);
  while (text.ends_with(' ') || text.ends_with('\t')) text.remove_suffix(1uz);
  return text;
}

/**
 * Decodes ASCII or comma-joined `-bin` Base64 header values into `map`.
 *
 * RFC 9113 allows intermediaries to join duplicate `-bin` header fields with commas, and gRPC
 * peers may omit trailing `=` padding on Base64 binary metadata.
 */
[[nodiscard]] inline std::expected<void, Status> AppendReceivedMetadata(
    MetadataMap& map, std::string_view key, std::string_view raw_value,
    StatusCode binary_error_code = StatusCode::kInternal) noexcept {
  if (!key.ends_with("-bin")) {
    return map.Add(key, raw_value);
  }
  while (true) {
    const auto comma = raw_value.find(',');
    const auto token = TrimOptionalWhitespace(raw_value.substr(0uz, comma));
    const auto pad_needed = (4uz - (token.size() & 3uz)) & 3uz;
    std::optional<std::vector<std::byte>> decoded;
    if (token.size() + pad_needed <= 256uz) {
      std::array<char, 256> stack_buf{};
      std::copy_n(token.data(), token.size(), stack_buf.data());
      std::fill_n(stack_buf.data() + token.size(), pad_needed, '=');
      decoded =
          MetadataMap::Base64Decode(std::string_view{stack_buf.data(), token.size() + pad_needed});
    } else {
      std::string heap_padded{};
      heap_padded.reserve(token.size() + pad_needed);
      heap_padded.append(token);
      heap_padded.append(pad_needed, '=');
      decoded = MetadataMap::Base64Decode(heap_padded);
    }
    if (uarch::unlikely(!decoded.has_value())) {
      return std::unexpected(MakeWireStatus(binary_error_code, "invalid binary metadata"));
    }
    if (auto added = map.AddBinary(key, *decoded); uarch::unlikely(!added.has_value())) {
      return added;
    }
    if (comma == std::string_view::npos) break;
    raw_value.remove_prefix(comma + 1uz);
  }
  return {};
}

[[nodiscard]] inline CompressionAlgorithm NegotiateAcceptedEncoding(
    std::string_view accept_encoding_header, CompressionAlgorithm preferred) noexcept {
  if (preferred == CompressionAlgorithm::kIdentity) return CompressionAlgorithm::kIdentity;
  const auto desired_token = CompressionRegistry::EncodingName(preferred);
  if (desired_token.empty()) return CompressionAlgorithm::kIdentity;
  std::size_t offset = 0uz;
  while (offset < accept_encoding_header.size()) {
    const auto comma = accept_encoding_header.find(',', offset);
    const auto len = (comma == std::string_view::npos) ? (accept_encoding_header.size() - offset)
                                                       : (comma - offset);
    const auto token = TrimOptionalWhitespace(accept_encoding_header.substr(offset, len));
    if (token == desired_token) return preferred;
    if (comma == std::string_view::npos) break;
    offset = comma + 1uz;
  }
  return CompressionAlgorithm::kIdentity;
}

}  // namespace grpcxx::h2
