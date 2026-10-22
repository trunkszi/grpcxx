#include "grpcxx/framing/length_prefixed_frame.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace grpcxx {

namespace {

constexpr std::byte kCompressedFlagMask{0x01};

}  // namespace

LengthPrefixedFrame::LengthPrefixedFrame(std::size_t max_receive_message_size) noexcept
    : max_receive_message_size_(max_receive_message_size) {}

LengthPrefixedFrame::LengthPrefixedFrame(LengthPrefixedFrame&& other) noexcept
    : max_receive_message_size_(other.max_receive_message_size_),
      state_(other.state_),
      header_(other.header_),
      header_received_(other.header_received_),
      pending_(std::move(other.pending_)),
      payload_received_(other.payload_received_),
      rejected_frames_(other.rejected_frames_),
      rejected_transitions_(other.rejected_transitions_) {
  other.Reset();
}

LengthPrefixedFrame& LengthPrefixedFrame::operator=(LengthPrefixedFrame&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  max_receive_message_size_ = other.max_receive_message_size_;
  state_ = other.state_;
  header_ = other.header_;
  header_received_ = other.header_received_;
  pending_ = std::move(other.pending_);
  payload_received_ = other.payload_received_;
  rejected_frames_ = other.rejected_frames_;
  rejected_transitions_ = other.rejected_transitions_;
  other.Reset();
  return *this;
}

std::expected<std::vector<AlignedMessageBuf>, Status> LengthPrefixedFrame::Feed(
    std::span<const std::byte> chunk) noexcept {
  if (state_ == ParseState::kFailed) {
    ++rejected_transitions_;
    return std::unexpected(Status(StatusCode::kFailedPrecondition, "parser failed"));
  }

  std::vector<AlignedMessageBuf> completed;
  std::size_t offset = 0U;

  while (offset < chunk.size()) {
    switch (state_) {
      case ParseState::kFailed:
        ++rejected_transitions_;
        return std::unexpected(Status(StatusCode::kFailedPrecondition, "parser failed"));

      case ParseState::kReadingHeader: {
        const std::size_t take =
            std::min(AlignedMessageBuf::kGrpcHeaderBytes - header_received_, chunk.size() - offset);
        std::ranges::copy(chunk.subspan(offset, take), header_.data() + header_received_);
        header_received_ += take;
        offset += take;

        if (header_received_ < AlignedMessageBuf::kGrpcHeaderBytes) {
          break;
        }

        const std::uint32_t payload_size = DecodeLength(header_);
        if (payload_size > max_receive_message_size_) {
          state_ = ParseState::kFailed;
          header_received_ = 0U;
          payload_received_ = 0U;
          pending_.reset();
          ++rejected_frames_;
          return std::unexpected(Status(StatusCode::kResourceExhausted, "frame too large"));
        }

        pending_.emplace(static_cast<std::size_t>(payload_size));
        pending_->SetCompressed((header_.front() & kCompressedFlagMask) != std::byte{0x00});
        header_received_ = 0U;
        payload_received_ = 0U;

        if (payload_size == 0U) {
          completed.push_back(std::move(*pending_));
          pending_.reset();
          state_ = ParseState::kReadingHeader;
        } else {
          state_ = ParseState::kReadingPayload;
        }
        break;
      }

      case ParseState::kReadingPayload: {
        const std::size_t payload_size = pending_->PayloadSize();
        const std::size_t take = std::min(payload_size - payload_received_, chunk.size() - offset);
        std::ranges::copy(chunk.subspan(offset, take),
                          pending_->Payload().data() + payload_received_);
        payload_received_ += take;
        offset += take;

        if (payload_received_ == payload_size) {
          completed.push_back(std::move(*pending_));
          pending_.reset();
          payload_received_ = 0U;
          state_ = ParseState::kReadingHeader;
        }
        break;
      }
    }
  }

  return completed;
}

LengthPrefixedFrame::ParseState LengthPrefixedFrame::State() const noexcept { return state_; }

std::size_t LengthPrefixedFrame::RejectedFrames() const noexcept { return rejected_frames_; }

std::size_t LengthPrefixedFrame::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

std::size_t LengthPrefixedFrame::MaxReceiveMessageSize() const noexcept {
  return max_receive_message_size_;
}

std::size_t LengthPrefixedFrame::BufferedPayloadBytes() const noexcept { return payload_received_; }

void LengthPrefixedFrame::Reset() noexcept {
  state_ = ParseState::kReadingHeader;
  header_ = {};
  header_received_ = 0U;
  payload_received_ = 0U;
  pending_.reset();
}

std::uint32_t LengthPrefixedFrame::DecodeLength(
    const std::array<std::byte, AlignedMessageBuf::kGrpcHeaderBytes>& header) noexcept {
  return (std::to_integer<std::uint32_t>(header[1]) << 24U) |
         (std::to_integer<std::uint32_t>(header[2]) << 16U) |
         (std::to_integer<std::uint32_t>(header[3]) << 8U) |
         std::to_integer<std::uint32_t>(header[4]);
}

}  // namespace grpcxx
