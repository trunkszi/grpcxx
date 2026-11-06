/** Deterministic codec seam tests; enabled engines exercise their real runtimes. */
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <ranges>
#include <string_view>
#include <vector>

#include "codec_workload.h"
#if defined(GRPCXX_ENABLE_PROTOBUF)
#include <google/protobuf/stubs/logging.h>

#include "required.pb.h"
#endif

namespace {

/** Record a failed observable invariant without relying on NDEBUG assertions. */
auto Check(bool condition, std::string_view description) noexcept -> void {
  EXPECT_TRUE(condition) << description;
}

/** Minimal application-defined codec proves the seam is independent of IDLs. */
struct UserCodec {
  static constexpr std::string_view kContentType = "application/grpc+user";
  auto Encode(const std::uint32_t& value,
              grpcxx::AlignedMessageBuf& buffer) noexcept -> std::expected<void, grpcxx::Status> {
    auto allocated = grpcxx::AlignedMessageBuf::Allocate(4);
    if (!allocated) return std::unexpected(allocated.error());
    buffer = std::move(*allocated);
    for (auto index : std::views::iota(0U, 4U)) {
      buffer.Payload()[index] = static_cast<std::byte>(value >> (index * 8));
    }
    return {};
  }
  auto Decode(std::span<const std::byte> payload,
              std::uint32_t& value) noexcept -> std::expected<void, grpcxx::Status> {
    if (payload.size() != 4) {
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument,
                                            "User payload must contain four bytes"});
    }
    value = 0;
    for (auto index : std::views::iota(0U, 4U)) {
      value |= std::to_integer<std::uint32_t>(payload[index]) << (index * 8);
    }
    return {};
  }
};
static_assert(grpcxx::Codec<UserCodec, std::uint32_t>);

struct ThrowingCodec {
  static constexpr std::string_view kContentType = "application/grpc+throwing";
  auto Encode(const std::uint32_t&,
              grpcxx::AlignedMessageBuf&) -> std::expected<void, grpcxx::Status>;
  auto Decode(std::span<const std::byte>, std::uint32_t&) -> std::expected<void, grpcxx::Status>;
};
static_assert(!grpcxx::Codec<ThrowingCodec, std::uint32_t>);

/** Compile-time branch verifies a custom codec can share constexpr utilities. */
constexpr auto TradeId() noexcept -> std::uint32_t {
  if consteval {
    return 73;
  } else {
    return 73;
  }
}
static_assert(TradeId() == 73);

auto TestCustomCodec() noexcept -> void {
  UserCodec codec;
  grpcxx::AlignedMessageBuf buffer;
  const auto input = TradeId();
  std::uint32_t output = 0;
  Check(codec.Encode(input, buffer).has_value(), "custom encode");
  Check(buffer.Size() == 4, "custom allocation follows the canonical payload size contract");
  Check(codec.Decode(buffer.Payload(), output).has_value() && output == input,
        "custom codec round trip");
  Check(!codec.Decode(buffer.Payload().first(3), output), "custom truncation rejected");
}

#if defined(GRPCXX_ENABLE_PROTOBUF)
static_assert(grpcxx::Codec<grpcxx::ProtobufCodec, codec_fixture::Request>);
auto TestProtobuf() noexcept -> void {
  grpcxx::ProtobufCodec codec;
  grpcxx::AlignedMessageBuf buffer;
  Check(grpcxx::codec_fixture_support::ProtobufRoundTrip(codec, buffer).has_value(),
        "Protobuf round trip");
  Check(reinterpret_cast<std::uintptr_t>(buffer.Payload().data()) % 8 == 0,
        "Protobuf payload remains eight-byte aligned");
  codec_fixture::Request output;
  const std::array invalid{std::byte{0x0a}, std::byte{0xff}};
  Check(!codec.Decode(invalid, output), "Protobuf truncated length-delimited field rejected");
  Check(codec.Decode({}, output).has_value(), "Protobuf valid empty message accepted");
  Check(codec.Encode(output, buffer).has_value() && buffer.Payload().empty(),
        "Protobuf empty message encodes to zero payload bytes");
  Check(buffer.WireBytes().size() == 5, "Protobuf empty message retains the gRPC prefix");
}

/** Required-field failures are reported before publishing a replacement buffer. */
auto TestProtobufRequiredFields() noexcept -> void {
  grpcxx::ProtobufCodec codec;
  codec_fixture::RequiredRequest input;
  grpcxx::AlignedMessageBuf buffer;
  auto allocated = grpcxx::AlignedMessageBuf::Allocate(8);
  ASSERT_TRUE(allocated);
  buffer = std::move(*allocated);
  std::ranges::fill(buffer.Payload(), std::byte{0x5a});
  const auto* original = buffer.Payload().data();
  auto missing = codec.Encode(input, buffer);
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error().Code(), grpcxx::StatusCode::kInvalidArgument);
  EXPECT_EQ(buffer.Payload().data(), original);
  EXPECT_TRUE(std::ranges::all_of(buffer.Payload(), [](auto b) { return b == std::byte{0x5a}; }));
  {
    google::protobuf::LogSilencer silence;
    EXPECT_FALSE(codec.Decode({}, input));
  }
  input.set_id(73);
  ASSERT_TRUE(codec.Encode(input, buffer));
  codec_fixture::RequiredRequest output;
  ASSERT_TRUE(codec.Decode(buffer.Payload(), output));
  EXPECT_EQ(output.id(), 73);
  const std::array truncated{std::byte{0x08}, std::byte{0x80}};
  EXPECT_FALSE(codec.Decode(truncated, output));
}

#endif

#if defined(GRPCXX_ENABLE_FORY)
static_assert(grpcxx::Codec<grpcxx::ForyCodec, grpcxx::codec_fixture_support::Trade>);
auto TestFory() noexcept -> void {
  grpcxx::ForyCodec codec;
  codec.Engine().register_struct<grpcxx::codec_fixture_support::Trade>(1);
  grpcxx::AlignedMessageBuf buffer;
  Check(grpcxx::codec_fixture_support::ForyRoundTrip(codec, buffer).has_value(),
        "Fory registered struct round trip");
  grpcxx::codec_fixture_support::Trade output;
  Check(!codec.Decode({}, output), "Fory empty payload rejected");
  Check(!codec.Decode(buffer.Payload().first(1), output), "Fory truncated payload rejected");
  auto bytes = std::vector<std::byte>(buffer.Payload().size() + 1);
  std::ranges::copy(buffer.Payload(), bytes.data());
  Check(!codec.Decode(bytes, output), "Fory trailing garbage rejected");
}

/** Every incomplete prefix fails without overwriting caller-owned state. */
auto TestForyFailureAtomicity() noexcept -> void {
  grpcxx::ForyCodec codec;
  codec.Engine().register_struct<grpcxx::codec_fixture_support::Trade>(1);
  const grpcxx::codec_fixture_support::Trade input{73, "immutable input"};
  grpcxx::AlignedMessageBuf buffer;
  ASSERT_TRUE(codec.Encode(input, buffer));
  auto snapshot = std::vector<std::byte>(buffer.Payload().size());
  std::ranges::copy(buffer.Payload(), snapshot.data());
  for (auto size : std::views::iota(std::size_t{0}, buffer.Payload().size())) {
    grpcxx::codec_fixture_support::Trade output{91, "keep caller value"};
    EXPECT_FALSE(codec.Decode(buffer.Payload().first(size), output)) << size;
    EXPECT_EQ(output.id, 91);
    EXPECT_EQ(output.symbol, "keep caller value");
  }
  grpcxx::ForyCodec unregistered;
  grpcxx::codec_fixture_support::Trade output{91, "keep caller value"};
  EXPECT_FALSE(unregistered.Decode(buffer.Payload(), output));
  EXPECT_EQ(output.id, 91);
  EXPECT_EQ(output.symbol, "keep caller value");
  EXPECT_TRUE(std::ranges::equal(buffer.Payload(), snapshot));
  EXPECT_EQ(input.id, 73);
  EXPECT_EQ(input.symbol, "immutable input");
}

#endif

#if defined(GRPCXX_ENABLE_CAPNP)
/** Validate both the borrowed reader address and flat-message framing guards. */
auto TestCapnp() noexcept -> void {
  grpcxx::CapnpCodec codec;
  grpcxx::AlignedMessageBuf buffer;
  ASSERT_TRUE(grpcxx::codec_fixture_support::CapnpRoundTrip(codec, buffer).has_value());
  capnp::MallocMessageBuilder destination;
  auto destination_root = destination.initRoot<capnp_fixture::nested::Request>();
  static_assert(grpcxx::Codec<grpcxx::CapnpCodec, capnp_fixture::nested::Request::Builder>);
  ASSERT_TRUE(codec.Decode(buffer.Payload(), destination_root));
  Check(destination_root.getId() == 73 &&
            std::string_view{destination_root.getSymbol().cStr()} == "EURUSD",
        "Capnp generated builder decode owns a deep copy");
  capnp::MallocMessageBuilder owned;
  ASSERT_TRUE(codec.Decode(buffer.Payload(), owned));
  Check(owned.getRoot<capnp_fixture::nested::Request>().getId() == 73,
        "Capnp message builder decode owns a deep copy");
  const auto payload = buffer.Payload();
  Check(reinterpret_cast<std::uintptr_t>(payload.data()) % 8 == 0,
        "Capnp payload address is eight-byte aligned");
  auto reader = codec.ReadAligned(payload);
  Check(reader.has_value(), "Capnp aligned reader created");
  if (reader) {
    const auto segment = (*reader)->getSegment(0);
    Check(reinterpret_cast<const std::byte*>(segment.begin()) == payload.data() + 8,
          "Capnp reader borrows original first segment without copying");
    const auto root = (*reader)->getRoot<capnp_fixture::nested::Request>();
    const auto text_address = reinterpret_cast<std::uintptr_t>(root.getSymbol().cStr());
    const auto start_address = reinterpret_cast<std::uintptr_t>(payload.data());
    Check(text_address >= start_address && text_address < start_address + payload.size(),
          "Capnp text reader points into original payload");
  }
  Check(!codec.ReadAligned({}), "Capnp empty framing rejected");
  std::vector<std::byte> unaligned_storage(payload.size() + 8);
  auto unaligned = std::span{unaligned_storage}.subspan(1, payload.size());
  std::ranges::copy(payload, unaligned.data());
  auto unaligned_result = codec.ReadAligned(unaligned);
  Check(
      !unaligned_result && unaligned_result.error().Code() == grpcxx::StatusCode::kInvalidArgument,
      "Capnp rejects unaligned input independently of valid word length");
  Check(!codec.ReadAligned(payload.first(payload.size() - 1)), "Capnp truncated word rejected");
  Check(!codec.ReadAligned(payload.first(8)), "Capnp missing segment rejected");
  std::vector<std::byte> trailing_storage(payload.size() + 8);
  std::ranges::copy(payload, trailing_storage.data());
  Check(!codec.ReadAligned(trailing_storage), "Capnp trailing undeclared words rejected");
  alignas(8) std::array<std::byte, 16> corrupt{};
  corrupt[0] = std::byte{0xff};
  corrupt[1] = std::byte{0xff};
  corrupt[2] = std::byte{0xff};
  corrupt[3] = std::byte{0xff};
  Check(!codec.ReadAligned(corrupt), "Capnp oversized segment count rejected");
  corrupt = {};
  corrupt[4] = std::byte{0xff};
  Check(!codec.ReadAligned(corrupt), "Capnp oversized segment body rejected");
  auto failure = codec.ReadAligned(corrupt);
  Check(!failure && (failure.error().Code() == grpcxx::StatusCode::kInvalidArgument ||
                     failure.error().Code() == grpcxx::StatusCode::kInternal),
        "Capnp malformed framing returns a canonical error status");
  corrupt = {};
  corrupt[4] = std::byte{1};
  corrupt[8] = std::byte{0xfc};
  corrupt[9] = std::byte{0xff};
  corrupt[10] = std::byte{0xff};
  corrupt[11] = std::byte{0x7f};
  corrupt[14] = std::byte{1};
  Check(!codec.ReadAligned(corrupt), "Capnp out-of-bounds root pointer rejected");
}

/** A valid far pointer crosses segments while all reader storage stays borrowed. */
auto TestCapnpMultipleSegments() noexcept -> void {
  alignas(8) const std::array<std::uint64_t, 5> words{0x0000000100000001ULL, 0x0000000000000002ULL,
                                                      0x0000000100000002ULL, 0x0000000100000000ULL,
                                                      73};
  const auto payload = std::as_bytes(std::span{words});
  auto reader = grpcxx::CapnpCodec::ReadAligned(payload);
  ASSERT_TRUE(reader);
  EXPECT_EQ((*reader)->getRoot<capnp_fixture::nested::Request>().getId(), 73);
  EXPECT_EQ(reinterpret_cast<const std::byte*>((*reader)->getSegment(0).begin()),
            payload.data() + 16);
  EXPECT_EQ(reinterpret_cast<const std::byte*>((*reader)->getSegment(1).begin()),
            payload.data() + 24);
  auto corrupt = words;
  corrupt[2] = 0x0000000900000002ULL;
  EXPECT_FALSE(grpcxx::CapnpCodec::ReadAligned(std::as_bytes(std::span{corrupt})));
  corrupt = words;
  corrupt[1] = 3;
  EXPECT_FALSE(grpcxx::CapnpCodec::ReadAligned(std::as_bytes(std::span{corrupt})));
}

/** Null roots are valid, while cycles and over-deep pointer graphs fail safely. */
auto TestCapnpPointerBounds() noexcept -> void {
  alignas(8) const std::array<std::uint64_t, 2> null_root{0x0000000100000000ULL, 0};
  auto empty = grpcxx::CapnpCodec::ReadAligned(std::as_bytes(std::span{null_root}));
  ASSERT_TRUE(empty);
  EXPECT_EQ((*empty)->getRoot<capnp_fixture::nested::Request>().getId(), 0);
  alignas(8) const std::array<std::uint64_t, 2> cycle{0x0000000100000000ULL, 0x00010000fffffffcULL};
  EXPECT_FALSE(grpcxx::CapnpCodec::ReadAligned(std::as_bytes(std::span{cycle})));
  alignas(8) std::array<std::uint64_t, 68> nested{};
  nested[0] = std::uint64_t{67} << 32;
  for (auto index : std::views::iota(std::size_t{1}, nested.size() - 1)) {
    nested[index] = std::uint64_t{1} << 48;
  }
  EXPECT_FALSE(grpcxx::CapnpCodec::ReadAligned(std::as_bytes(std::span{nested})));
}

/** Validation must not consume the traversal budget returned to the caller. */
auto TestCapnpTraversalBudget() noexcept -> void {
  constexpr auto kDataBytes = static_cast<unsigned int>(
      (grpcxx::CapnpCodec::kTraversalLimitWords / 2 + 1024) * sizeof(capnp::word));
  capnp::MallocMessageBuilder input;
  auto data = input.getRoot<capnp::AnyPointer>().initAs<capnp::Data>(kDataBytes);
  data[0] = 17;
  data[kDataBytes - 1] = 29;
  auto words = capnp::messageToFlatArray(input);
  auto allocated = grpcxx::AlignedMessageBuf::Allocate(
      static_cast<std::uint32_t>(words.size() * sizeof(capnp::word)));
  ASSERT_TRUE(allocated);
  std::memcpy(allocated->Payload().data(), words.begin(), allocated->Payload().size());
  auto reader = grpcxx::CapnpCodec::ReadAligned(allocated->Payload());
  ASSERT_TRUE(reader);
  auto result = (*reader)->getRoot<capnp::Data>();
  ASSERT_EQ(result.size(), kDataBytes);
  EXPECT_EQ(result[0], 17);
  EXPECT_EQ(result[kDataBytes - 1], 29);
  capnp::MallocMessageBuilder destination;
  ASSERT_TRUE(grpcxx::CapnpCodec::Decode(allocated->Payload(), destination));
  auto copy = destination.getRoot<capnp::Data>();
  EXPECT_EQ(copy.size(), kDataBytes);
  EXPECT_EQ(copy[kDataBytes - 1], 29);
  EXPECT_NE(copy.begin(), result.begin());
}

#endif
}  // namespace

TEST(MultiCodec, CustomCodec) { TestCustomCodec(); }
#if defined(GRPCXX_ENABLE_PROTOBUF)
TEST(MultiCodec, Protobuf) { TestProtobuf(); }
TEST(MultiCodec, ProtobufRequiredFields) { TestProtobufRequiredFields(); }
#endif
#if defined(GRPCXX_ENABLE_FORY)
TEST(MultiCodec, Fory) { TestFory(); }
TEST(MultiCodec, ForyFailureAtomicity) { TestForyFailureAtomicity(); }
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
TEST(MultiCodec, CapnpZeroCopyAndMalformedFraming) { TestCapnp(); }
TEST(MultiCodec, CapnpMultipleSegments) { TestCapnpMultipleSegments(); }
TEST(MultiCodec, CapnpPointerBounds) { TestCapnpPointerBounds(); }
TEST(MultiCodec, CapnpTraversalBudget) { TestCapnpTraversalBudget(); }
#endif
