#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "grpcxx/core/status.h"

namespace grpcxx {

/**
 * Owns one gRPC message frame in an eight-byte-aligned backing allocation.
 *
 * The backing bytes are laid out as
 * `[0..2: private padding][3: compressed flag][4..7: big-endian payload length][8..8+N: payload]`.
 * The three padding bytes push the five-byte gRPC header to offset 3, so Payload() begins on an
 * eight-byte boundary while WireBytes() exposes the contiguous `[flag][length][payload]` slice
 * starting at that header. A transport can therefore send WireBytes() without prepending a
 * header, and a codec can read Payload() with eight-byte aligned loads.
 *
 * The class is move-only. Moving transfers ownership of the backing allocation; the moved-from
 * object must not be inspected.
 */
class AlignedMessageBuf final {
 public:
  static constexpr std::size_t kLeadingPadBytes = 3U;
  static constexpr std::size_t kGrpcHeaderBytes = 5U;
  static constexpr std::size_t kPayloadOffset = 8U;

  /**
   * Allocates one frame through the canonical shared-contract entry point.
   *
   * The payload size is a wire `uint32_t`; the result carries the same zero-initialized
   * payload and header as the constructor. The expected return type keeps the shared
   * cross-ticket contract while the constructor remains available for direct construction.
   */
  [[nodiscard]] static std::expected<AlignedMessageBuf, Status> Allocate(
      std::uint32_t payload_size, bool compressed = false) noexcept;

  /**
   * Constructs a frame with a zero-length payload.
   *
   * The frame still owns a valid eight-byte header, so WireBytes() returns the five-byte
   * zero-length prefix and Payload() returns an empty span.
   */
  AlignedMessageBuf() noexcept;

  /**
   * Allocates a frame of payload_size bytes and writes its header.
   *
   * The compressed flag starts clear and the big-endian header length is set to payload_size.
   * Payload bytes are value-initialized to zero and remain writable through Payload(). A size
   * above `UINT32_MAX` has no representable four-byte wire length and terminates the process;
   * `Allocate` cannot reach that case because it takes a `uint32_t`.
   */
  explicit AlignedMessageBuf(std::size_t payload_size) noexcept;

  AlignedMessageBuf(const AlignedMessageBuf&) = delete;
  AlignedMessageBuf& operator=(const AlignedMessageBuf&) = delete;
  AlignedMessageBuf(AlignedMessageBuf&&) noexcept = default;
  AlignedMessageBuf& operator=(AlignedMessageBuf&&) noexcept = default;
  ~AlignedMessageBuf() noexcept = default;

  /** Returns the payload region, starting at offset 8 with size PayloadSize(). */
  [[nodiscard]] std::span<std::byte> Payload() noexcept;

  /** Returns the payload region, starting at offset 8 with size PayloadSize(). */
  [[nodiscard]] std::span<const std::byte> Payload() const noexcept;

  /** Returns `[compressed flag][big-endian length][payload]`, starting at offset 3. */
  [[nodiscard]] std::span<const std::byte> WireBytes() const noexcept;

  [[nodiscard]] std::size_t PayloadSize() const noexcept;

  /** Returns the payload size as the canonical shared-contract `Size()` value. */
  [[nodiscard]] std::uint32_t Size() const noexcept;

  [[nodiscard]] std::size_t WireSize() const noexcept;

  [[nodiscard]] bool Compressed() const noexcept;

  /** Writes the gRPC compressed flag; all other flag bits stay zero. */
  void SetCompressed(bool compressed) noexcept;

  /** Returns the numeric address of Payload().data() so tests can verify alignment. */
  [[nodiscard]] std::uintptr_t PayloadAddress() const noexcept;

 private:
  std::unique_ptr<std::byte[]> data_{};
  std::size_t payload_size_{0U};
};

}  // namespace grpcxx
