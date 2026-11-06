#pragma once

#include <grpcxx/core/status.h>
#include <grpcxx/framing/aligned_message_buf.h>
#include <grpcxx/reflection/annotations.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <utility>

namespace grpcxx {
namespace reflection_detail {
inline constexpr std::size_t kMaxCodecPayloadBytes = 65536;

/** Span-based compatibility codecs use the base media type. */
template <typename Codec>
consteval std::string_view CodecContentType() {
  if constexpr (requires { Codec::kContentType; })
    return Codec::kContentType;
  else
    return "application/grpc";
}

/** Invokes the consumer while the codec's payload storage remains alive. */
template <typename Codec, typename Msg, typename Consumer>
auto EncodePayload(Codec* codec, const Msg& message,
                   Consumer&& consume) noexcept -> decltype(consume(std::span<const std::byte>{})) {
  auto canonical = [&](auto&& encode) noexcept -> decltype(consume(std::span<const std::byte>{})) {
    AlignedMessageBuf framed;
    auto encoded = encode(framed);
    if (!encoded) return std::unexpected(std::move(encoded.error()));
    if (framed.Size() > kMaxCodecPayloadBytes) {
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    }
    if (framed.Compressed()) return std::unexpected(Status{StatusCode::kInvalidArgument});
    return consume(std::as_const(framed).Payload());
  };
  if constexpr (requires(AlignedMessageBuf& buffer) {
                  {
                    Codec::Encode(message, buffer)
                  } noexcept -> std::same_as<std::expected<void, Status>>;
                }) {
    return canonical(
        [&](AlignedMessageBuf& buffer) noexcept { return Codec::Encode(message, buffer); });
  } else if constexpr (requires(AlignedMessageBuf& buffer) {
                         {
                           codec->Encode(message, buffer)
                         } noexcept -> std::same_as<std::expected<void, Status>>;
                       }) {
    if (codec == nullptr) return std::unexpected(Status{StatusCode::kFailedPrecondition});
    return canonical(
        [&](AlignedMessageBuf& buffer) noexcept { return codec->Encode(message, buffer); });
  } else {
    alignas(8) std::array<std::byte, kMaxCodecPayloadBytes> storage;
    auto encoded = [&]() noexcept -> std::expected<std::size_t, Status> {
      if constexpr (requires {
                      {
                        Codec::template Encode<Msg>(message, std::span{storage})
                      } noexcept -> std::same_as<std::expected<std::size_t, Status>>;
                    })
        return Codec::template Encode<Msg>(message, std::span{storage});
      else {
        static_assert(
            requires {
              {
                codec->template Encode<Msg>(message, std::span{storage})
              } noexcept -> std::same_as<std::expected<std::size_t, Status>>;
            }, "codec must provide canonical or span Encode");
        if (codec == nullptr) return std::unexpected(Status{StatusCode::kFailedPrecondition});
        return codec->template Encode<Msg>(message, std::span{storage});
      }
    }();
    if (!encoded) return std::unexpected(std::move(encoded.error()));
    if (*encoded > storage.size()) return std::unexpected(Status{StatusCode::kInternal});
    return consume(std::span<const std::byte>{storage}.first(*encoded));
  }
}

/**
 * Decodes an owning, value-initializable message with the supplied codec instance.
 * Arena-backed readers or builders require an owning adapter that preserves their storage.
 */
template <typename Codec, typename Msg>
std::expected<Msg, Status> DecodePayload(Codec* codec,
                                         std::span<const std::byte> payload) noexcept {
  if (payload.size() > kMaxCodecPayloadBytes) {
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  }
  if constexpr (requires(Msg& message) {
                  {
                    Codec::Decode(payload, message)
                  } noexcept -> std::same_as<std::expected<void, Status>>;
                }) {
    Msg message{};
    auto decoded = Codec::Decode(payload, message);
    if (!decoded) return std::unexpected(std::move(decoded.error()));
    return message;
  } else if constexpr (requires(Msg& message) {
                         {
                           codec->Decode(payload, message)
                         } noexcept -> std::same_as<std::expected<void, Status>>;
                       }) {
    if (codec == nullptr) return std::unexpected(Status{StatusCode::kFailedPrecondition});
    Msg message{};
    auto decoded = codec->Decode(payload, message);
    if (!decoded) return std::unexpected(std::move(decoded.error()));
    return message;
  } else if constexpr (requires {
                         {
                           Codec::template Decode<Msg>(payload)
                         } noexcept -> std::same_as<std::expected<Msg, Status>>;
                       })
    return Codec::template Decode<Msg>(payload);
  else {
    static_assert(
        requires {
          {
            codec->template Decode<Msg>(payload)
          } noexcept -> std::same_as<std::expected<Msg, Status>>;
        }, "codec must provide canonical or value Decode");
    if (codec == nullptr) return std::unexpected(Status{StatusCode::kFailedPrecondition});
    return codec->template Decode<Msg>(payload);
  }
}
}  // namespace reflection_detail

/** Non-owning session hooks; the session must outlive every issued responder. */
struct UnaryResponderSink {
  void* context{};
  bool (*is_current)(void*, std::uint64_t, std::int32_t, std::uint64_t) noexcept {};
  std::expected<void, Status> (*complete)(void*, std::uint64_t, std::int32_t, std::uint64_t,
                                          std::span<const std::byte>, MetadataMap&&,
                                          MetadataMap&&) noexcept {};
  std::expected<void, Status> (*fail)(void*, std::uint64_t, std::int32_t, std::uint64_t,
                                      Status) noexcept {};
};

/** A single-completion capability. A supplied codec and session outlive every deferred handle. */
template <typename Resp>
class UnaryResponder final {
 public:
  static constexpr std::size_t kMaxPayloadBytes = 65536;
  UnaryResponder() noexcept = default;
  UnaryResponder(const UnaryResponder&) = delete;
  UnaryResponder& operator=(const UnaryResponder&) = delete;
  UnaryResponder(UnaryResponder&& other) noexcept { MoveFrom(other); }
  UnaryResponder& operator=(UnaryResponder&& other) noexcept {
    if (this != &other) MoveFrom(other);
    return *this;
  }

  template <typename Codec>
  static UnaryResponder Bind(UnaryResponderSink sink, std::uint64_t session_id,
                             std::int32_t stream_id, std::uint64_t generation,
                             Codec* codec = nullptr) noexcept {
    UnaryResponder responder;
    responder.sink_ = sink;
    responder.codec_ = codec;
    responder.session_id_ = session_id;
    responder.stream_id_ = stream_id;
    responder.generation_ = generation;
    responder.encode_ = [](UnaryResponder& self,
                           Response<Resp>&& response) noexcept -> std::expected<void, Status> {
      return reflection_detail::EncodePayload<Codec>(
          static_cast<Codec*>(self.codec_), response.message,
          [&](std::span<const std::byte> payload) noexcept -> std::expected<void, Status> {
            return self.sink_.complete(
                self.sink_.context, self.session_id_, self.stream_id_, self.generation_, payload,
                std::move(response.initial_metadata), std::move(response.trailing_metadata));
          });
    };
    return responder;
  }

  [[nodiscard]] std::expected<void, Status> Complete(Response<Resp>&& response) noexcept {
    if (!Current() || sink_.complete == nullptr || encode_ == nullptr) return Stale();
    auto result = encode_(*this, std::move(response));
    if (result) consumed_ = true;
    return result;
  }

  [[nodiscard]] std::expected<void, Status> Fail(Status status) noexcept {
    if (status.Ok()) return std::unexpected(Status{StatusCode::kInvalidArgument});
    if (!Current() || sink_.fail == nullptr) return Stale();
    auto result =
        sink_.fail(sink_.context, session_id_, stream_id_, generation_, std::move(status));
    if (result) consumed_ = true;
    return result;
  }

  [[nodiscard]] bool Current() const noexcept {
    return !consumed_ && sink_.is_current != nullptr &&
           sink_.is_current(sink_.context, session_id_, stream_id_, generation_);
  }

 private:
  static std::expected<void, Status> Stale() noexcept {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  void MoveFrom(UnaryResponder& other) noexcept {
    session_id_ = other.session_id_;
    stream_id_ = other.stream_id_;
    generation_ = other.generation_;
    sink_ = other.sink_;
    encode_ = other.encode_;
    codec_ = other.codec_;
    consumed_ = other.consumed_;
    other.consumed_ = true;
  }
  std::uint64_t session_id_{};
  std::int32_t stream_id_{};
  std::uint64_t generation_{};
  UnaryResponderSink sink_{};
  std::expected<void, Status> (*encode_)(UnaryResponder&, Response<Resp>&&) noexcept {};
  void* codec_{};
  bool consumed_{false};
};

}  // namespace grpcxx
