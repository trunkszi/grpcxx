#pragma once

#if defined(GRPCXX_ENABLE_PROTOBUF)

#include <google/protobuf/message_lite.h>
#include <grpcxx/codec/codec_concept.h>

namespace grpcxx {

/** Protobuf serializer writing directly into the framed payload allocation. */
class ProtobufCodec {
 public:
  static constexpr std::string_view kContentType = "application/grpc+proto";

  [[nodiscard]] static auto Encode(const google::protobuf::MessageLite& message,
                                   AlignedMessageBuf& buffer) noexcept
      -> std::expected<void, Status>;

  [[nodiscard]] static auto Decode(std::span<const std::byte> payload,
                                   google::protobuf::MessageLite& message) noexcept
      -> std::expected<void, Status>;
};

}  // namespace grpcxx

#endif
