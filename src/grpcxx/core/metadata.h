#pragma once

#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "grpcxx/core/status.h"

namespace grpcxx {

/**
 * Ordered application metadata for gRPC headers and trailers.
 *
 * Entries live contiguously in insertion order, so repeated keys are preserved and lookups
 * return the first match. Keys are normalized to lowercase at insertion, which makes lookups
 * case-insensitive while keeping the wire spelling canonical.
 *
 * Keys ending in `-bin` (after lowercasing) carry raw binary values. Those values are stored
 * Base64-encoded internally: `WireValue` returns the encoded form ready for HTTP/2, while
 * `Get` and `GetBinary` return the original bytes. All other keys carry printable ASCII values
 * and are stored verbatim.
 */
class MetadataMap final {
 public:
  /** One stored metadata entry; `value` is Base64 for binary entries. */
  struct Entry final {
    std::string key{};
    std::string value{};
    bool binary{false};
  };

  /**
   * Appends an entry.
   *
   * A key ending in `-bin` stores `value` as raw bytes and Base64-encodes it; any other key
   * stores `value` verbatim after validating that it is printable ASCII. Rejects empty keys,
   * keys containing spaces, control characters or non-ASCII characters, and non-printable
   * ASCII values with kInvalidArgument. A rejected entry leaves the map unchanged.
   */
  [[nodiscard]] std::expected<void, Status> Add(std::string_view key,
                                                std::string_view value) noexcept;

  /**
   * Appends a binary entry.
   *
   * The key must end in `-bin`; `value` is stored as raw bytes and Base64-encoded for the
   * wire. Rejects malformed keys and keys without the `-bin` suffix with kInvalidArgument.
   */
  [[nodiscard]] std::expected<void, Status> AddBinary(std::string_view key,
                                                      std::span<const std::byte> value) noexcept;

  /**
   * Returns the first value stored under `key`, decoded.
   *
   * Binary entries are Base64-decoded back to their original bytes. Returns an empty optional
   * when the key is absent or when a stored binary value cannot be decoded.
   */
  [[nodiscard]] std::optional<std::string> Get(std::string_view key) const noexcept;

  /**
   * Returns the first binary value stored under `key`.
   *
   * Returns an empty optional when the key is absent or holds an ASCII value.
   */
  [[nodiscard]] std::optional<std::vector<std::byte>> GetBinary(
      std::string_view key) const noexcept;

  /**
   * Returns the first wire representation stored under `key`.
   *
   * ASCII entries are returned verbatim; binary entries are returned Base64-encoded.
   */
  [[nodiscard]] std::optional<std::string_view> WireValue(std::string_view key) const noexcept;

  /** Returns the number of stored entries, counting repeated keys separately. */
  [[nodiscard]] std::size_t Size() const noexcept;

  /** Returns all entries in insertion order. */
  [[nodiscard]] std::span<const Entry> Entries() const noexcept;

  /** Removes all entries. */
  void Clear() noexcept;

  /** Encodes `bytes` as standard padded Base64. */
  [[nodiscard]] static std::string Base64Encode(std::span<const std::byte> bytes) noexcept;

  /**
   * Decodes standard padded Base64.
   *
   * Returns an empty optional for input whose length is not a multiple of four, that contains
   * characters outside the standard alphabet, that has bad padding, or whose trailing bits
   * are not zero.
   */
  [[nodiscard]] static std::optional<std::vector<std::byte>> Base64Decode(
      std::string_view encoded) noexcept;

 private:
  std::vector<Entry> entries_{};
};

}  // namespace grpcxx
