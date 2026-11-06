#pragma once

#if defined(GRPCXX_ENABLE_FORY)

#include <fmt/format.h>
#include <fory/serialization/fory.h>
#include <grpcxx/codec/codec_concept.h>

#include <algorithm>
#include <limits>
#include <ranges>
#include <utility>

namespace grpcxx {

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
    if (!encoded.ok()) {
      return std::unexpected(Status{StatusCode::kInvalidArgument,
                                    fmt::format("Fory encode: {}", encoded.error().message())});
    }
    if (encoded->size() > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(
          Status{StatusCode::kResourceExhausted, "Fory payload exceeds gRPC size limit"});
    }
    auto framed = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(encoded->size()));
    if (!framed) {
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
    if (payload.empty() || payload.size() > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(Status{StatusCode::kInvalidArgument, "Invalid Fory payload length"});
    }
    fory::Buffer input(
        const_cast<std::uint8_t*>(reinterpret_cast<const std::uint8_t*>(payload.data())),
        static_cast<std::uint32_t>(payload.size()), false);
    auto decoded = engine_.template deserialize<Message>(input);
    if (!decoded.ok()) {
      return std::unexpected(Status{StatusCode::kInvalidArgument,
                                    fmt::format("Fory decode: {}", decoded.error().message())});
    }
    if (input.reader_index() != payload.size()) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Trailing bytes in Fory payload"});
    }
    message = std::move(*decoded);
    return {};
  }

 private:
  fory::serialization::Fory engine_;
};

}  // namespace grpcxx

#endif
