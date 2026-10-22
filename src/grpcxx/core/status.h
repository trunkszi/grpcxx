#pragma once

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <utility>

namespace grpcxx {

/**
 * Canonical gRPC status codes.
 *
 * The numeric values are part of the gRPC wire contract and must never be reordered or
 * renumbered.
 */
enum class StatusCode : std::uint8_t {
  kOk = 0,
  kCancelled = 1,
  kUnknown = 2,
  kInvalidArgument = 3,
  kDeadlineExceeded = 4,
  kNotFound = 5,
  kAlreadyExists = 6,
  kPermissionDenied = 7,
  kResourceExhausted = 8,
  kFailedPrecondition = 9,
  kAborted = 10,
  kOutOfRange = 11,
  kUnimplemented = 12,
  kInternal = 13,
  kUnavailable = 14,
  kDataLoss = 15,
  kUnauthenticated = 16,
};

/**
 * A gRPC status: a canonical code plus an optional diagnostic message.
 *
 * The default-constructed value is OK with an empty message. A non-OK status carries a
 * human-readable message that is safe to send in the percent-encoded `grpc-message` header.
 * Status is a value type: copies own their message.
 */
class Status final {
 public:
  /** Creates an OK status with an empty message. */
  constexpr Status() noexcept = default;

  /** Creates a status with `code` and an empty message. */
  constexpr explicit Status(StatusCode code) noexcept : code_(code) {}

  /** Creates a status with `code` and a diagnostic message. */
  constexpr Status(StatusCode code, std::string message) noexcept
      : code_(code), message_(std::move(message)) {}

  /** Returns an OK status. */
  [[nodiscard]] static constexpr Status OkStatus() noexcept { return Status{}; }

  /** Returns the canonical status code. */
  [[nodiscard]] constexpr StatusCode Code() const noexcept { return code_; }

  /** Returns the diagnostic message; empty when none was supplied. */
  [[nodiscard]] constexpr std::string_view Message() const noexcept { return message_; }

  /** Reports whether the status code is kOk. */
  [[nodiscard]] constexpr bool Ok() const noexcept { return code_ == StatusCode::kOk; }

  /**
   * Percent-encodes `input` using the RFC 3986 unreserved set.
   *
   * Letters, digits, `-`, `.`, `_` and `~` are copied verbatim; every other octet becomes an
   * uppercase `%HH` triplet. This is the encoding used by the `grpc-message` header.
   */
  [[nodiscard]] static std::string PercentEncode(std::string_view input) noexcept;

  /**
   * Decodes an RFC 3986 percent-encoded string.
   *
   * Returns kInvalidArgument when a `%` is not followed by two hexadecimal digits; all other
   * octets are copied through unchanged.
   */
  [[nodiscard]] static std::expected<std::string, Status> PercentDecode(
      std::string_view input) noexcept;

  /**
   * Maps an HTTP/2 `:status` value to a gRPC status.
   *
   * 2xx is OK. The transport-level failures that gRPC reinterprets map as follows: 400 to
   * kInternal, 401 to kUnauthenticated, 403 to kPermissionDenied, 404 to kUnimplemented, and
   * 429/502/503/504 to kUnavailable. Every other value maps to kUnknown.
   */
  [[nodiscard]] static Status FromHttpStatus(int http_status) noexcept;

 private:
  StatusCode code_{StatusCode::kOk};
  std::string message_{};
};

}  // namespace grpcxx
