#include <grpcxx/codec/protobuf_codec.h>

#if defined(GRPCXX_ENABLE_PROTOBUF)

#include <fmt/format.h>

#include <limits>
#include <uarch/uarch.hpp>

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status ProtobufError(StatusCode code,
                                                             std::string_view reason) noexcept {
  uarch::cold_path();
  return Status{code, std::string(reason)};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status ProtobufSerializeError(
    std::string_view type_name) noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInternal, fmt::format("Cannot serialize {}", type_name)};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status ProtobufDecodeError(
    std::string_view type_name) noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInvalidArgument, fmt::format("Malformed {} payload", type_name)};
}

}  // namespace

auto ProtobufCodec::Encode(const google::protobuf::MessageLite& message,
                           AlignedMessageBuf& buffer) noexcept -> std::expected<void, Status> {
  const auto size = message.ByteSizeLong();
  if (uarch::unlikely(size > static_cast<std::size_t>(std::numeric_limits<int>::max()))) {
    return std::unexpected(ProtobufError(StatusCode::kResourceExhausted,
                                         "Protobuf payload exceeds serializer size limit"));
  }
  if (uarch::unlikely(!message.IsInitialized())) {
    return std::unexpected(ProtobufError(StatusCode::kInvalidArgument,
                                         "Protobuf message has missing required fields"));
  }
  auto encoded = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(size));
  if (uarch::unlikely(!encoded)) {
    return std::unexpected(std::move(encoded.error()));
  }
  if (uarch::unlikely(
          !message.SerializeToArray(encoded->Payload().data(), static_cast<int>(size)))) {
    return std::unexpected(ProtobufSerializeError(message.GetTypeName()));
  }
  buffer = std::move(*encoded);
  return {};
}

auto ProtobufCodec::Decode(std::span<const std::byte> payload,
                           google::protobuf::MessageLite& message) noexcept
    -> std::expected<void, Status> {
  if (uarch::unlikely(payload.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))) {
    return std::unexpected(ProtobufError(StatusCode::kResourceExhausted,
                                         "Protobuf payload exceeds parser size limit"));
  }
  // Protobuf's short-input path requires a nonnull source even for a zero-byte copy.
  const auto* data =
      payload.empty() ? "" : static_cast<const char*>(static_cast<const void*>(payload.data()));
  if (uarch::unlikely(!message.ParseFromArray(data, static_cast<int>(payload.size())))) {
    return std::unexpected(ProtobufDecodeError(message.GetTypeName()));
  }
  return {};
}

}  // namespace grpcxx

#endif
