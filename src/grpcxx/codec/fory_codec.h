#pragma once

#if defined(GRPCXX_ENABLE_FORY)

#include <fmt/format.h>
#include <fory/serialization/fory.h>
#include <grpcxx/codec/codec_concept.h>

#include <algorithm>
#include <limits>
#include <ranges>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace codec_detail {

[[nodiscard, gnu::cold, gnu::noinline]] inline Status ForyError(StatusCode code,
                                                                std::string_view reason) noexcept {
  uarch::cold_path();
  return Status{code, std::string(reason)};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status ForyFormattedError(
    std::string_view prefix, std::string_view message) noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInvalidArgument, fmt::format("{}: {}", prefix, message)};
}

}  // namespace codec_detail

/** Caller-owned Apache Fory context; register application types before use. */
class ForyCodec {
 public:
  static constexpr std::string_view kContentType = "application/grpc+fory";

  ForyCodec() noexcept : engine_(fory::serialization::Fory::builder().build()) {}
  explicit ForyCodec(fory::serialization::ForyBuilder builder) noexcept
      : engine_(builder.build()) {}

  /** Configure type IDs before the first encode or decode operation. */
  [[nodiscard]] auto Engine() noexcept -> fory::serialization::Fory& { return engine_; }

  template <typename Message>
  [[nodiscard]] auto Encode(const Message& message,
                            AlignedMessageBuf& buffer) noexcept -> std::expected<void, Status> {
    auto encoded = engine_.serialize(message);
    if (uarch::unlikely(!encoded.ok())) {
      return std::unexpected(
          codec_detail::ForyFormattedError("Fory encode", encoded.error().message()));
    }
    if (uarch::unlikely(encoded->size() > std::numeric_limits<std::uint32_t>::max())) {
      return std::unexpected(codec_detail::ForyError(StatusCode::kResourceExhausted,
                                                     "Fory payload exceeds gRPC size limit"));
    }
    auto framed = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(encoded->size()));
    if (uarch::unlikely(!framed)) {
      return std::unexpected(std::move(framed.error()));
    }
    std::ranges::transform(*encoded, framed->Payload().data(),
                           [](std::uint8_t value) { return static_cast<std::byte>(value); });
    buffer = std::move(*framed);
    return {};
  }

  template <typename Message>
  [[nodiscard]] auto Decode(std::span<const std::byte> payload,
                            Message& message) noexcept -> std::expected<void, Status> {
    if (uarch::unlikely(payload.empty() ||
                        payload.size() > std::numeric_limits<std::uint32_t>::max())) {
      return std::unexpected(
          codec_detail::ForyError(StatusCode::kInvalidArgument, "Invalid Fory payload length"));
    }
    fory::Buffer input(const_cast<std::uint8_t*>(static_cast<const std::uint8_t*>(
                           static_cast<const void*>(payload.data()))),
                       static_cast<std::uint32_t>(payload.size()), false);
    auto decoded = engine_.template deserialize<Message>(input);
    if (uarch::unlikely(!decoded.ok())) {
      return std::unexpected(
          codec_detail::ForyFormattedError("Fory decode", decoded.error().message()));
    }
    if (uarch::unlikely(input.reader_index() != payload.size())) {
      return std::unexpected(
          codec_detail::ForyError(StatusCode::kInvalidArgument, "Trailing bytes in Fory payload"));
    }
    message = std::move(*decoded);
    return {};
  }

 private:
  fory::serialization::Fory engine_;
};

}  // namespace grpcxx

#endif
