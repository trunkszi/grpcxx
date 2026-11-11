#include "grpcxx/framing/length_prefixed_frame.h"

namespace {

[[nodiscard]] std::uint64_t RejectedHeader() noexcept { return ~std::uint64_t{0}; }

}  // namespace

extern "C" std::uint64_t grpcxx_probe_frame_header_encode(std::uint32_t length,
                                                          std::uint32_t compressed) noexcept {
  const auto packed = grpcxx::PackGrpcHeader(length, static_cast<std::uint8_t>(compressed));
  return compressed <= 1U ? packed : RejectedHeader();
}

extern "C" std::uint64_t grpcxx_probe_frame_header_decode(std::uint64_t packed) noexcept {
  const auto flag = static_cast<std::uint32_t>(packed & 0xFFU);
  const auto length = grpcxx::UnpackGrpcHeaderLength(packed);
  const auto decoded = (static_cast<std::uint64_t>(flag) << 32U) | length;
  return grpcxx::LegalCompressedFlag(static_cast<std::uint8_t>(flag)) ? decoded : RejectedHeader();
}
