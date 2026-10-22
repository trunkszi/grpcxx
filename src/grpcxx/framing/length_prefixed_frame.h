#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <vector>

#include "grpcxx/core/status.h"
#include "grpcxx/framing/aligned_message_buf.h"

namespace grpcxx {

/**
 * Stateful parser that reassembles gRPC length-prefixed frames from arbitrary chunks.
 *
 * Each frame is `[compressed flag][big-endian payload length][payload]`. Chunk boundaries may
 * fall anywhere in a frame, and one chunk may complete several frames. The parser rejects a frame
 * as soon as its header is complete when the declared length exceeds the configured maximum, so
 * an oversized frame never allocates payload storage.
 *
 * Copying is deleted because the buffered header and pending frame are parser state. Moving
 * transfers that state and resets the source to kReadingHeader, so a moved-from parser stays
 * usable and never retains a reference to a moved-out frame.
 */
class LengthPrefixedFrame final {
 public:
  enum class ParseState : std::uint8_t {
    kReadingHeader,
    kReadingPayload,
    kFailed,
  };

  explicit LengthPrefixedFrame(std::size_t max_receive_message_size) noexcept;

  LengthPrefixedFrame(const LengthPrefixedFrame&) = delete;
  LengthPrefixedFrame& operator=(const LengthPrefixedFrame&) = delete;

  /** Moves parser state out of `other`; `other` is reset to a usable empty parser. */
  LengthPrefixedFrame(LengthPrefixedFrame&& other) noexcept;
  LengthPrefixedFrame& operator=(LengthPrefixedFrame&& other) noexcept;

  ~LengthPrefixedFrame() noexcept = default;

  /**
   * Consumes chunk and returns every frame it completes.
   *
   * The returned vector is empty when chunk only covers part of a frame; it holds multiple
   * frames when chunk carries coalesced messages. A zero-length payload produces a frame with
   * an empty payload.
   *
   * If the header announces a payload larger than MaxReceiveMessageSize(), the parser enters
   * kFailed, increments RejectedFrames(), and returns StatusCode::kResourceExhausted without
   * allocating the payload frame. On any error the call returns no frames: frames completed
   * earlier in the same chunk are discarded and the parser stays failed until Reset(). Calling
   * Feed() while already failed is an illegal state transition: it increments
   * RejectedTransitions(), returns StatusCode::kFailedPrecondition, and does not increment
   * RejectedFrames().
   */
  [[nodiscard]] std::expected<std::vector<AlignedMessageBuf>, Status> Feed(
      std::span<const std::byte> chunk) noexcept;

  [[nodiscard]] ParseState State() const noexcept;

  [[nodiscard]] std::size_t RejectedFrames() const noexcept;

  /** Returns the number of calls refused because the parser was already in kFailed. */
  [[nodiscard]] std::size_t RejectedTransitions() const noexcept;

  [[nodiscard]] std::size_t MaxReceiveMessageSize() const noexcept;

  /** Returns the payload bytes received for the frame in progress; zero after a rejection. */
  [[nodiscard]] std::size_t BufferedPayloadBytes() const noexcept;

  /** Drops partial state and returns to kReadingHeader; maximum and rejection count persist. */
  void Reset() noexcept;

 private:
  [[nodiscard]] static std::uint32_t DecodeLength(
      const std::array<std::byte, AlignedMessageBuf::kGrpcHeaderBytes>& header) noexcept;

  std::size_t max_receive_message_size_{0U};
  ParseState state_{ParseState::kReadingHeader};
  std::array<std::byte, AlignedMessageBuf::kGrpcHeaderBytes> header_{};
  std::size_t header_received_{0U};
  std::optional<AlignedMessageBuf> pending_{};
  std::size_t payload_received_{0U};
  std::size_t rejected_frames_{0U};
  std::size_t rejected_transitions_{0U};
};

}  // namespace grpcxx
