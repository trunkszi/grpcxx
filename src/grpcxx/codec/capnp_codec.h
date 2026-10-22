#pragma once

#if defined(GRPCXX_ENABLE_CAPNP)

#include <capnp/any.h>
#include <capnp/serialize.h>
#include <grpcxx/codec/capnp_error_scope.h>
#include <grpcxx/codec/codec_concept.h>
#include <kj/exception.h>

#include <algorithm>
#include <memory>
#include <ranges>

namespace grpcxx {

/** Owns an RPC struct's arena; readers and builders borrow this movable owner. */
template <typename Schema>
class CapnpMessage final {
 public:
  CapnpMessage() noexcept = default;
  CapnpMessage(const CapnpMessage&) = delete;
  CapnpMessage& operator=(const CapnpMessage&) = delete;
  CapnpMessage(CapnpMessage&&) noexcept = default;
  CapnpMessage& operator=(CapnpMessage&&) noexcept = default;

  /** Views remain valid until this owner is destroyed or replaced by decoding. */
  [[nodiscard]] auto GetBuilder() noexcept -> typename Schema::Builder {
    return Storage().template getRoot<Schema>();
  }
  [[nodiscard]] auto GetReader() const noexcept -> typename Schema::Reader {
    return Storage().template getRoot<Schema>().asReader();
  }

 private:
  friend class CapnpCodec;
  auto Storage() const noexcept -> capnp::MallocMessageBuilder& {
    if (!storage_) {
      storage_ = std::make_unique<capnp::MallocMessageBuilder>();
      storage_->template initRoot<Schema>();
    }
    return *storage_;
  }
  mutable std::unique_ptr<capnp::MallocMessageBuilder> storage_;
};

/** Cap'n Proto framing and bounded, borrowed reads over aligned payloads. */
class CapnpCodec {
 public:
  static constexpr std::string_view kContentType = "application/grpc+capnp";
  static constexpr std::uint64_t kTraversalLimitWords = 8 * 1024 * 1024;
  static constexpr int kNestingLimit = 64;

  /** The reader borrows payload: keep its backing buffer alive and unchanged. */
  [[nodiscard]] static auto ReadAligned(std::span<const std::byte> payload) noexcept
      -> std::expected<std::unique_ptr<capnp::FlatArrayMessageReader>, Status>;

  /** Serialize a default-constructible RPC message without exposing its arena. */
  template <typename Schema>
  [[nodiscard]] static auto Encode(const CapnpMessage<Schema>& message,
                                   AlignedMessageBuf& buffer) noexcept
      -> std::expected<void, Status> {
    return EncodeBuilder(message.Storage(), buffer);
  }

  /** Publish an owning decoded struct only after typed root validation succeeds. */
  template <typename Schema>
  [[nodiscard]] static auto Decode(std::span<const std::byte> payload,
                                   CapnpMessage<Schema>& message) noexcept
      -> std::expected<void, Status> {
    auto reader = ReadAligned(payload);
    if (!reader) return std::unexpected(std::move(reader.error()));
    CapnpMessage<Schema> decoded;
    if (!codec_detail::RunCapnpOperation(
            [&] { decoded.Storage().setRoot((*reader)->template getRoot<Schema>()); })) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Cannot decode Cap'n Proto RPC struct"});
    }
    message = std::move(decoded);
    return {};
  }

  /** Serialize a generated schema Reader into the aligned gRPC payload. */
  template <typename Message>
    requires requires(const Message& message) { typename Message::Reads; }
  [[nodiscard]] static auto Encode(const Message& message, AlignedMessageBuf& buffer) noexcept
      -> std::expected<void, Status> {
    capnp::MallocMessageBuilder builder;
    if (auto succeeded = codec_detail::RunCapnpOperation([&] { builder.setRoot(message); });
        !succeeded) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Invalid Cap'n Proto source message"});
    }
    return EncodeBuilder(builder, buffer);
  }

  /** Generated schema Builders can satisfy Codec without borrowed reader state. */
  template <typename Message>
    requires requires(const Message& message) {
      typename Message::Builds;
      message.asReader();
    }
  [[nodiscard]] static auto Encode(const Message& message, AlignedMessageBuf& buffer) noexcept
      -> std::expected<void, Status> {
    return Encode(message.asReader(), buffer);
  }

  /** Deep-copy the decoded root into caller-owned message storage. */
  [[nodiscard]] static auto Decode(std::span<const std::byte> payload,
                                   capnp::MessageBuilder& message) noexcept
      -> std::expected<void, Status>;

  /** Decode into an initialized generated struct Builder without dangling views. */
  template <typename Message>
    requires requires { typename Message::Builds; }
  [[nodiscard]] static auto Decode(std::span<const std::byte> payload,
                                   Message& message) noexcept -> std::expected<void, Status> {
    auto reader = ReadAligned(payload);
    if (!reader) {
      return std::unexpected(std::move(reader.error()));
    }
    bool incompatible = false;
    if (auto succeeded = codec_detail::RunCapnpOperation([&] {
          auto source = (*reader)->getRoot<capnp::AnyStruct>();
          auto target = capnp::AnyStruct::Builder(Message(message));
          auto source_data = source.getDataSection();
          auto target_data = target.getDataSection();
          auto source_pointers = source.getPointerSection();
          auto target_pointers = target.getPointerSection();
          if (source_data.size() > target_data.size() ||
              source_pointers.size() > target_pointers.size()) {
            incompatible = true;
            return;
          }
          std::ranges::fill(target_data, 0);
          std::ranges::copy(source_data, target_data.begin());
          for (auto i : std::views::iota(0U, target_pointers.size())) {
            if (i < source_pointers.size()) {
              target_pointers[i].set(source_pointers[i]);
            } else {
              target_pointers[i].clear();
            }
          }
        });
        !succeeded) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Cannot decode Cap'n Proto struct"});
    }
    if (incompatible) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Cap'n Proto target struct is too small"});
    }
    return {};
  }

 private:
  [[nodiscard]] static auto EncodeBuilder(capnp::MessageBuilder& message,
                                          AlignedMessageBuf& buffer) noexcept
      -> std::expected<void, Status>;
};

}  // namespace grpcxx

#endif
