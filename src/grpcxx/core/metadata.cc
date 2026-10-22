#include "grpcxx/core/metadata.h"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace grpcxx {
namespace {

constexpr std::string_view kBase64Alphabet{
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"};

/** Lowercases an ASCII letter and leaves every other octet unchanged. */
[[nodiscard]] constexpr char LowerAscii(char character) noexcept {
  return (character >= 'A' && character <= 'Z') ? static_cast<char>(character - 'A' + 'a')
                                                : character;
}

/** Metadata keys are visible ASCII tokens; spaces and controls are not legal. */
[[nodiscard]] constexpr bool IsValidKeyByte(char character) noexcept {
  const auto byte = static_cast<unsigned char>(character);
  return byte > 0x20U && byte <= 0x7EU;
}

/** ASCII metadata values are printable ASCII: 0x20 through 0x7E. */
[[nodiscard]] constexpr bool IsValidAsciiValueByte(char character) noexcept {
  const auto byte = static_cast<unsigned char>(character);
  return byte >= 0x20U && byte <= 0x7EU;
}

[[nodiscard]] std::string NormalizeKey(std::string_view key) noexcept {
  std::string normalized;
  normalized.reserve(key.size());
  for (const char character : key) {
    normalized.push_back(LowerAscii(character));
  }
  return normalized;
}

/** Builds the standard Base64 alphabet lookup once at compile time; -1 means invalid. */
[[nodiscard]] consteval std::array<std::int8_t, 256> MakeBase64DecodeTable() noexcept {
  std::array<std::int8_t, 256> table{};
  table.fill(static_cast<std::int8_t>(-1));
  for (std::size_t index = 0; index < kBase64Alphabet.size(); ++index) {
    const auto byte = static_cast<unsigned char>(kBase64Alphabet[index]);
    table[static_cast<std::size_t>(byte)] = static_cast<std::int8_t>(index);
  }
  return table;
}

constexpr std::array<std::int8_t, 256> kBase64DecodeTable = MakeBase64DecodeTable();

[[nodiscard]] constexpr int DecodeBase64Char(char character) noexcept {
  return kBase64DecodeTable[static_cast<std::size_t>(static_cast<unsigned char>(character))];
}

[[nodiscard]] bool MatchesNormalized(const MetadataMap::Entry& entry,
                                     const std::string& key) noexcept {
  return entry.key == key;
}

}  // namespace

std::string MetadataMap::Base64Encode(std::span<const std::byte> bytes) noexcept {
  std::string encoded;
  encoded.reserve(((bytes.size() + 2U) / 3U) * 4U);

  std::size_t index = 0;
  while (index + 3U <= bytes.size()) {
    const std::uint32_t first = std::to_integer<std::uint32_t>(bytes[index]);
    const std::uint32_t second = std::to_integer<std::uint32_t>(bytes[index + 1U]);
    const std::uint32_t third = std::to_integer<std::uint32_t>(bytes[index + 2U]);

    encoded.push_back(kBase64Alphabet[(first >> 2U) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[((first << 4U) | (second >> 4U)) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[((second << 2U) | (third >> 6U)) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[third & 0x3FU]);
    index += 3U;
  }

  const std::size_t remaining = bytes.size() - index;
  if (remaining == 1U) {
    const std::uint32_t first = std::to_integer<std::uint32_t>(bytes[index]);
    encoded.push_back(kBase64Alphabet[(first >> 2U) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[(first << 4U) & 0x3FU]);
    encoded.push_back('=');
    encoded.push_back('=');
  } else if (remaining == 2U) {
    const std::uint32_t first = std::to_integer<std::uint32_t>(bytes[index]);
    const std::uint32_t second = std::to_integer<std::uint32_t>(bytes[index + 1U]);
    encoded.push_back(kBase64Alphabet[(first >> 2U) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[((first << 4U) | (second >> 4U)) & 0x3FU]);
    encoded.push_back(kBase64Alphabet[(second << 2U) & 0x3FU]);
    encoded.push_back('=');
  }

  return encoded;
}

std::optional<std::vector<std::byte>> MetadataMap::Base64Decode(std::string_view encoded) noexcept {
  if (encoded.size() % 4U != 0U) {
    return std::nullopt;
  }

  std::vector<std::byte> decoded;
  decoded.reserve((encoded.size() / 4U) * 3U);

  for (std::size_t index = 0; index < encoded.size(); index += 4U) {
    const char third_char = encoded[index + 2U];
    const char fourth_char = encoded[index + 3U];

    const bool two_pads = third_char == '=';
    const bool one_pad = !two_pads && fourth_char == '=';
    if (two_pads && fourth_char != '=') {
      return std::nullopt;
    }
    if ((two_pads || one_pad) && index + 4U != encoded.size()) {
      return std::nullopt;
    }

    const int first = DecodeBase64Char(encoded[index]);
    const int second = DecodeBase64Char(encoded[index + 1U]);
    if (first < 0 || second < 0) {
      return std::nullopt;
    }

    std::uint32_t bits =
        (static_cast<std::uint32_t>(first) << 18U) | (static_cast<std::uint32_t>(second) << 12U);

    if (two_pads) {
      if ((static_cast<std::uint32_t>(second) & 0x0FU) != 0U) {
        return std::nullopt;
      }
      decoded.push_back(static_cast<std::byte>((bits >> 16U) & 0xFFU));
      continue;
    }

    const int third = DecodeBase64Char(third_char);
    if (third < 0) {
      return std::nullopt;
    }
    bits |= static_cast<std::uint32_t>(third) << 6U;

    if (one_pad) {
      if ((static_cast<std::uint32_t>(third) & 0x03U) != 0U) {
        return std::nullopt;
      }
      decoded.push_back(static_cast<std::byte>((bits >> 16U) & 0xFFU));
      decoded.push_back(static_cast<std::byte>((bits >> 8U) & 0xFFU));
      continue;
    }

    const int fourth = DecodeBase64Char(fourth_char);
    if (fourth < 0) {
      return std::nullopt;
    }
    bits |= static_cast<std::uint32_t>(fourth);

    decoded.push_back(static_cast<std::byte>((bits >> 16U) & 0xFFU));
    decoded.push_back(static_cast<std::byte>((bits >> 8U) & 0xFFU));
    decoded.push_back(static_cast<std::byte>(bits & 0xFFU));
  }

  return decoded;
}

std::expected<void, Status> MetadataMap::Add(std::string_view key,
                                             std::string_view value) noexcept {
  std::string normalized = NormalizeKey(key);
  if (normalized.empty() || !std::ranges::all_of(normalized, IsValidKeyByte)) {
    return std::unexpected(
        Status(StatusCode::kInvalidArgument, fmt::format("invalid metadata key '{}'", key)));
  }

  if (normalized.ends_with("-bin")) {
    const auto bytes = std::as_bytes(std::span<const char>{value.data(), value.size()});
    entries_.push_back(Entry{std::move(normalized), Base64Encode(bytes), true});
    return {};
  }

  if (!std::ranges::all_of(value, IsValidAsciiValueByte)) {
    return std::unexpected(Status(StatusCode::kInvalidArgument,
                                  fmt::format("invalid ASCII metadata value for key '{}'", key)));
  }

  entries_.push_back(Entry{std::move(normalized), std::string{value}, false});
  return {};
}

std::expected<void, Status> MetadataMap::AddBinary(std::string_view key,
                                                   std::span<const std::byte> value) noexcept {
  std::string normalized = NormalizeKey(key);
  if (normalized.empty() || !std::ranges::all_of(normalized, IsValidKeyByte)) {
    return std::unexpected(
        Status(StatusCode::kInvalidArgument, fmt::format("invalid metadata key '{}'", key)));
  }

  if (!normalized.ends_with("-bin")) {
    return std::unexpected(Status(StatusCode::kInvalidArgument,
                                  fmt::format("binary metadata key '{}' must end with -bin", key)));
  }

  entries_.push_back(Entry{std::move(normalized), Base64Encode(value), true});
  return {};
}

std::optional<std::string> MetadataMap::Get(std::string_view key) const noexcept {
  const std::string normalized = NormalizeKey(key);
  if (const auto found = std::ranges::find_if(entries_,
                                              [&normalized](const Entry& entry) noexcept {
                                                return MatchesNormalized(entry, normalized);
                                              });
      found != entries_.end()) {
    if (!found->binary) {
      return found->value;
    }

    const auto decoded = Base64Decode(found->value);
    if (!decoded.has_value()) {
      return std::nullopt;
    }

    std::string raw;
    raw.reserve(decoded->size());
    for (const std::byte byte : *decoded) {
      raw.push_back(static_cast<char>(byte));
    }
    return raw;
  }

  return std::nullopt;
}

std::optional<std::vector<std::byte>> MetadataMap::GetBinary(std::string_view key) const noexcept {
  const std::string normalized = NormalizeKey(key);
  const auto found = std::ranges::find_if(entries_, [&normalized](const Entry& entry) noexcept {
    return MatchesNormalized(entry, normalized);
  });
  if (found == entries_.end() || !found->binary) {
    return std::nullopt;
  }
  return Base64Decode(found->value);
}

std::optional<std::string_view> MetadataMap::WireValue(std::string_view key) const noexcept {
  const std::string normalized = NormalizeKey(key);
  const auto found = std::ranges::find_if(entries_, [&normalized](const Entry& entry) noexcept {
    return MatchesNormalized(entry, normalized);
  });
  if (found == entries_.end()) {
    return std::nullopt;
  }
  return std::string_view{found->value};
}

std::size_t MetadataMap::Size() const noexcept { return entries_.size(); }

std::span<const MetadataMap::Entry> MetadataMap::Entries() const noexcept {
  return std::span<const Entry>{entries_};
}

void MetadataMap::Clear() noexcept { entries_.clear(); }

}  // namespace grpcxx
