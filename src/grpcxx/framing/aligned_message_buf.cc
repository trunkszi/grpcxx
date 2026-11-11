#include "grpcxx/framing/aligned_message_buf.h"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <type_traits>
#include <uarch/uarch.hpp>

namespace grpcxx {

static_assert(alignof(std::max_align_t) >= AlignedMessageBuf::kPayloadOffset,
              "operator new must return storage aligned to at least eight bytes");

namespace {

[[gnu::always_inline]] inline void StoreBigEndianLength(std::byte* __restrict destination,
                                                        std::uint32_t length) noexcept {
  if constexpr (std::endian::native == std::endian::little) {
    length = std::byteswap(length);
  }
  std::memcpy(destination, &length, sizeof(length));
}

[[noreturn, gnu::cold, gnu::noinline]] void TerminateOversizedFrame() noexcept {
  uarch::cold_path();
  std::terminate();
}

}  // namespace

std::expected<AlignedMessageBuf, Status> AlignedMessageBuf::Allocate(std::uint32_t payload_size,
                                                                     bool compressed) noexcept {
  if (uarch::unlikely(static_cast<std::size_t>(payload_size) >
                      std::numeric_limits<std::size_t>::max() - kPayloadOffset)) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "frame allocation size overflow"});
  }
  AlignedMessageBuf buffer(static_cast<std::size_t>(payload_size));
  buffer.SetCompressed(compressed);
  return buffer;
}

AlignedMessageBuf::AlignedMessageBuf() noexcept : AlignedMessageBuf(0U) {}

AlignedMessageBuf::AlignedMessageBuf(std::size_t payload_size) noexcept {
  if (uarch::unlikely(payload_size > std::numeric_limits<std::uint32_t>::max() ||
                      payload_size > std::numeric_limits<std::size_t>::max() - kPayloadOffset)) {
    // The wire length is four bytes; larger payloads have no representable header.
    TerminateOversizedFrame();
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
  return data_[kLeadingPadBytes] == std::byte{0x01};
}

void AlignedMessageBuf::SetCompressed(bool compressed) noexcept {
  data_[kLeadingPadBytes] =
      static_cast<std::byte>(uarch::predicate(compressed).select_register(0x01U, 0x00U));
}

std::uintptr_t AlignedMessageBuf::PayloadAddress() const noexcept {
  return std::bit_cast<std::uintptr_t>(data_.get() + kPayloadOffset);
}

}  // namespace grpcxx
