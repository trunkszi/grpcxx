#include <grpcxx/codec/protobuf_codec.h>

#if defined(GRPCXX_ENABLE_PROTOBUF)

#include <fmt/format.h>

#include <limits>

namespace grpcxx {

auto ProtobufCodec::Encode(const google::protobuf::MessageLite& message,
                           AlignedMessageBuf& buffer) noexcept -> std::expected<void, Status> {
  const auto size = message.ByteSizeLong();
  if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "Protobuf payload exceeds serializer size limit"});
  }
  if (!message.IsInitialized()) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Protobuf message has missing required fields"});
  }
  auto encoded = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(size));
  if (!encoded) {
    return std::unexpected(std::move(encoded.error()));
  }
  if (!message.SerializeToArray(encoded->Payload().data(), static_cast<int>(size))) {
    return std::unexpected(
        Status{StatusCode::kInternal, fmt::format("Cannot serialize {}", message.GetTypeName())});
  }
  buffer = std::move(*encoded);
  return {};
}

auto ProtobufCodec::Decode(std::span<const std::byte> payload,
                           google::protobuf::MessageLite& message) noexcept
    -> std::expected<void, Status> {
  if (payload.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "Protobuf payload exceeds parser size limit"});
  }
  if (!message.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    return std::unexpected(Status{StatusCode::kInvalidArgument,
                                  fmt::format("Malformed {} payload", message.GetTypeName())});
  }
  return {};
}

}  // namespace grpcxx

#endif
