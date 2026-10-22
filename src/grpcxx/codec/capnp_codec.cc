#include <grpcxx/codec/capnp_codec.h>

#if defined(GRPCXX_ENABLE_CAPNP)

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <ranges>

namespace grpcxx {
namespace {

/** Read wire little-endian integers without assuming host endianness. */
auto ReadU32(std::span<const std::byte> bytes) noexcept -> std::uint32_t {
  std::uint32_t value = 0;
  for (auto i : std::views::iota(std::size_t{0}, std::size_t{4})) {
    value |= std::to_integer<std::uint32_t>(bytes[i]) << (i * 8);
  }
  return value;
}

/** Reject partial tables, overflow, empty roots, and trailing message data. */
auto ValidateFraming(std::span<const std::byte> payload) noexcept -> std::expected<void, Status> {
  if (payload.size() < 16 || payload.size() % sizeof(capnp::word) != 0) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Truncated Cap'n Proto word framing"});
  }
  if (reinterpret_cast<std::uintptr_t>(payload.data()) % 8 != 0) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Cap'n Proto payload must be eight-byte aligned"});
  }
  const auto count = static_cast<std::uint64_t>(ReadU32(payload.first(4))) + 1;
  if (count > 512 || count > (payload.size() / 4) - 1) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Invalid Cap'n Proto segment count"});
  }
  const auto table_words = (count + 2) / 2;
  std::uint64_t expected_words = table_words;
  for (auto i : std::views::iota(std::uint64_t{0}, count)) {
    const auto words = ReadU32(payload.subspan(static_cast<std::size_t>((i + 1) * 4), 4));
    if (i == 0 && words == 0) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Cap'n Proto root segment is empty"});
    }
    expected_words += words;
    if (expected_words > payload.size() / sizeof(capnp::word)) {
      return std::unexpected(
          Status{StatusCode::kInvalidArgument, "Cap'n Proto segment extends past payload"});
    }
  }
  if (expected_words != payload.size() / sizeof(capnp::word)) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Trailing bytes after Cap'n Proto segments"});
  }
  return {};
}

}  // namespace

auto CapnpCodec::ReadAligned(std::span<const std::byte> payload) noexcept
    -> std::expected<std::unique_ptr<capnp::FlatArrayMessageReader>, Status> {
  if (auto validated = ValidateFraming(payload); !validated) {
    return std::unexpected(std::move(validated.error()));
  }
  capnp::ReaderOptions options;
  options.traversalLimitInWords = kTraversalLimitWords;
  options.nestingLimit = kNestingLimit;
  const auto words = kj::ArrayPtr<const capnp::word>(
      reinterpret_cast<const capnp::word*>(payload.data()), payload.size() / sizeof(capnp::word));
  if (auto succeeded = codec_detail::RunCapnpOperation([&] {
        capnp::FlatArrayMessageReader validation(words, options);
        static_cast<void>(validation.getRoot<capnp::AnyPointer>().targetSize());
      });
      !succeeded) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Malformed Cap'n Proto pointer graph"});
  }
  /** Validation and caller traversal each receive their own bounded allowance. */
  auto reader = std::make_unique<capnp::FlatArrayMessageReader>(words, options);
  return reader;
}

auto CapnpCodec::EncodeBuilder(capnp::MessageBuilder& message,
                               AlignedMessageBuf& buffer) noexcept -> std::expected<void, Status> {
  kj::Array<capnp::word> encoded;
  if (auto succeeded =
          codec_detail::RunCapnpOperation([&] { encoded = capnp::messageToFlatArray(message); });
      !succeeded) {
    return std::unexpected(Status{StatusCode::kInternal, "Cannot serialize Cap'n Proto message"});
  }
  if (encoded.size() > std::numeric_limits<std::uint32_t>::max() / sizeof(capnp::word)) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "Cap'n Proto payload exceeds gRPC size limit"});
  }
  const auto size = encoded.size() * sizeof(capnp::word);
  auto framed = AlignedMessageBuf::Allocate(static_cast<std::uint32_t>(size));
  if (!framed) {
    return std::unexpected(std::move(framed.error()));
  }
  std::memcpy(framed->Payload().data(), encoded.begin(), size);
  buffer = std::move(*framed);
  return {};
}

auto CapnpCodec::Decode(std::span<const std::byte> payload,
                        capnp::MessageBuilder& message) noexcept -> std::expected<void, Status> {
  auto reader = ReadAligned(payload);
  if (!reader) {
    return std::unexpected(std::move(reader.error()));
  }
  if (auto succeeded = codec_detail::RunCapnpOperation(
          [&] { message.setRoot((*reader)->getRoot<capnp::AnyPointer>()); });
      !succeeded) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "Cannot decode Cap'n Proto message"});
  }
  return {};
}

}  // namespace grpcxx

#endif
