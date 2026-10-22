#include "grpcxx/framing/aligned_message_buf.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>

namespace grpcxx {

static_assert(alignof(std::max_align_t) >= AlignedMessageBuf::kPayloadOffset,
              "operator new must return storage aligned to at least eight bytes");

namespace {

void StoreBigEndianLength(std::byte* destination, std::uint32_t length) noexcept {
  destination[0] = static_cast<std::byte>((length >> 24U) & 0xFFU);
  destination[1] = static_cast<std::byte>((length >> 16U) & 0xFFU);
  destination[2] = static_cast<std::byte>((length >> 8U) & 0xFFU);
  destination[3] = static_cast<std::byte>(length & 0xFFU);
}

}  // namespace

std::expected<AlignedMessageBuf, Status> AlignedMessageBuf::Allocate(std::uint32_t payload_size,
                                                                     bool compressed) noexcept {
  AlignedMessageBuf buffer(static_cast<std::size_t>(payload_size));
  buffer.SetCompressed(compressed);
  return buffer;
}

AlignedMessageBuf::AlignedMessageBuf() noexcept : AlignedMessageBuf(0U) {}

AlignedMessageBuf::AlignedMessageBuf(std::size_t payload_size) noexcept {
  if (payload_size > std::numeric_limits<std::uint32_t>::max()) [[unlikely]] {
    // The wire length is four bytes; larger payloads have no representable header.
    std::terminate();
  }

  data_ = std::make_unique<std::byte[]>(kPayloadOffset + payload_size);
  payload_size_ = payload_size;
  // Bytes 0..2 and the compressed flag at byte 3 are zero from value-initialization.
  StoreBigEndianLength(data_.get() + kLeadingPadBytes + 1U,
                       static_cast<std::uint32_t>(payload_size));
}

std::span<std::byte> AlignedMessageBuf::Payload() noexcept {
  return std::span<std::byte>(data_.get() + kPayloadOffset, payload_size_);
}

std::span<const std::byte> AlignedMessageBuf::Payload() const noexcept {
  return std::span<const std::byte>(data_.get() + kPayloadOffset, payload_size_);
}

std::span<const std::byte> AlignedMessageBuf::WireBytes() const noexcept {
  return std::span<const std::byte>(data_.get() + kLeadingPadBytes,
                                    kGrpcHeaderBytes + payload_size_);
}

std::size_t AlignedMessageBuf::PayloadSize() const noexcept { return payload_size_; }

std::uint32_t AlignedMessageBuf::Size() const noexcept {
  return static_cast<std::uint32_t>(payload_size_);
}

std::size_t AlignedMessageBuf::WireSize() const noexcept {
  return kGrpcHeaderBytes + payload_size_;
}

bool AlignedMessageBuf::Compressed() const noexcept {
  return data_[kLeadingPadBytes] != std::byte{0x00};
}

void AlignedMessageBuf::SetCompressed(bool compressed) noexcept {
  data_[kLeadingPadBytes] = compressed ? std::byte{0x01} : std::byte{0x00};
}

std::uintptr_t AlignedMessageBuf::PayloadAddress() const noexcept {
  return reinterpret_cast<std::uintptr_t>(data_.get() + kPayloadOffset);
}

}  // namespace grpcxx
