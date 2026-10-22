#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"
#include "grpcxx/core/timeout.h"

namespace grpcxx {
namespace {

static_assert([] {
  const Status empty;
  const Status failure{StatusCode::kUnavailable, "temporarily unavailable"};
  return empty.Ok() && empty.Message().empty() && failure.Code() == StatusCode::kUnavailable &&
         failure.Message() == "temporarily unavailable";
}());

[[nodiscard]] auto AsBytes(std::string_view text) -> std::span<const std::byte> {
  return std::as_bytes(std::span{text.data(), text.size()});
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------
TEST(StatusTest, AllSeventeenCodesKeepTheirWireNumbers) {
  const std::array<std::pair<StatusCode, std::uint8_t>, 17> codes{{
      {StatusCode::kOk, 0U},
      {StatusCode::kCancelled, 1U},
      {StatusCode::kUnknown, 2U},
      {StatusCode::kInvalidArgument, 3U},
      {StatusCode::kDeadlineExceeded, 4U},
      {StatusCode::kNotFound, 5U},
      {StatusCode::kAlreadyExists, 6U},
      {StatusCode::kPermissionDenied, 7U},
      {StatusCode::kResourceExhausted, 8U},
      {StatusCode::kFailedPrecondition, 9U},
      {StatusCode::kAborted, 10U},
      {StatusCode::kOutOfRange, 11U},
      {StatusCode::kUnimplemented, 12U},
      {StatusCode::kInternal, 13U},
      {StatusCode::kUnavailable, 14U},
      {StatusCode::kDataLoss, 15U},
      {StatusCode::kUnauthenticated, 16U},
  }};
  for (const auto& [code, number] : codes) {
    EXPECT_EQ(static_cast<std::uint8_t>(code), number);
  }
  static_assert(std::is_same_v<std::underlying_type_t<StatusCode>, std::uint8_t>);
}
TEST(StatusTest, DefaultIsOkAndNonOkStatusOwnsItsMessage) {
  const Status ok;
  EXPECT_TRUE(ok.Ok());
  EXPECT_EQ(ok.Code(), StatusCode::kOk);
  EXPECT_EQ(ok.Message(), std::string_view{});
  EXPECT_TRUE(Status::OkStatus().Ok());

  std::string text = "diagnostic text that is longer than every small-string buffer";
  Status status(StatusCode::kNotFound, text);
  text.clear();
  EXPECT_FALSE(status.Ok());
  EXPECT_EQ(status.Code(), StatusCode::kNotFound);
  EXPECT_EQ(status.Message(), "diagnostic text that is longer than every small-string buffer");

  const Status copied = status;
  EXPECT_EQ(copied.Message(), status.Message());
  EXPECT_EQ(Status(StatusCode::kOk).Message(), std::string_view{});
}
TEST(StatusTest, PercentEncodeLeavesUnreservedAndEscapesEverythingElse) {
  constexpr std::string_view kUnreserved{
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"};
  EXPECT_EQ(Status::PercentEncode(kUnreserved), kUnreserved);
  EXPECT_EQ(Status::PercentEncode(""), "");
  EXPECT_EQ(Status::PercentEncode(" "), "%20");
  EXPECT_EQ(Status::PercentEncode("%"), "%25");
  EXPECT_EQ(Status::PercentEncode(":/?#[]@"), "%3A%2F%3F%23%5B%5D%40");
  EXPECT_EQ(Status::PercentEncode("!$&'()*+,;="), "%21%24%26%27%28%29%2A%2B%2C%3B%3D");
  EXPECT_EQ(Status::PercentEncode(std::string_view{"\x00\x01\x1F\x7F", 4}), "%00%01%1F%7F");
  EXPECT_EQ(Status::PercentEncode("\xC3\xA9"), "%C3%A9");
  EXPECT_EQ(Status::PercentEncode("\xE4\xB8\x96"), "%E4%B8%96");
  EXPECT_EQ(Status::PercentEncode("\xF0\x9F\x9A\x80"), "%F0%9F%9A%80");
  EXPECT_EQ(Status::PercentEncode("a b"), "a%20b");
}
TEST(StatusTest, PercentDecodeRoundTripsEveryByteValueAndLowercaseHex) {
  std::string all_bytes;
  for (const auto value : std::views::iota(0, 256)) {
    all_bytes.push_back(static_cast<char>(value));
  }
  const auto decoded_all = Status::PercentDecode(Status::PercentEncode(all_bytes));
  ASSERT_TRUE(decoded_all.has_value());
  EXPECT_EQ(*decoded_all, all_bytes);

  for (const std::string_view input : std::array<std::string_view, 4>{
           "héllo 世界 🚀", "100% sure", "a+b=c&d=e", std::string_view{"\x00\x01\x1F\x7F", 4}}) {
    const auto decoded = Status::PercentDecode(Status::PercentEncode(input));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, input);
  }

  const auto lowercase = Status::PercentDecode("%2f%c3%a9");
  ASSERT_TRUE(lowercase.has_value());
  EXPECT_EQ(*lowercase, "/\xC3\xA9");
}
TEST(StatusTest, PercentDecodeRejectsMalformedEscapes) {
  for (const std::string_view input : std::array<std::string_view, 10>{
           "%", "%2", "%2G", "%G2", "%GG", "abc%", "abc%4", "100%", "%%20", "%2f%4"}) {
    const auto decoded = Status::PercentDecode(input);
    ASSERT_FALSE(decoded.has_value()) << input;
    EXPECT_EQ(decoded.error().Code(), StatusCode::kInvalidArgument) << input;
  }
}
TEST(StatusTest, FromHttpStatusMapsSuccessFailuresAndEverythingElse) {
  for (const int http_status : std::array{200, 201, 204, 250, 299}) {
    const Status status = Status::FromHttpStatus(http_status);
    EXPECT_TRUE(status.Ok()) << "HTTP " << http_status;
    EXPECT_EQ(status.Message(), std::string_view{}) << "HTTP " << http_status;
  }

  const std::array<std::pair<int, StatusCode>, 8> failures{{
      {400, StatusCode::kInternal},
      {401, StatusCode::kUnauthenticated},
      {403, StatusCode::kPermissionDenied},
      {404, StatusCode::kUnimplemented},
      {429, StatusCode::kUnavailable},
      {502, StatusCode::kUnavailable},
      {503, StatusCode::kUnavailable},
      {504, StatusCode::kUnavailable},
  }};
  for (const auto& [http_status, expected] : failures) {
    const Status status = Status::FromHttpStatus(http_status);
    EXPECT_EQ(status.Code(), expected) << "HTTP " << http_status;
    EXPECT_FALSE(status.Ok()) << "HTTP " << http_status;
  }

  for (const int http_status : std::array{0, 100, 199, 300, 301, 418, 500, 501, 505, -1}) {
    const Status status = Status::FromHttpStatus(http_status);
    EXPECT_EQ(status.Code(), StatusCode::kUnknown) << "HTTP " << http_status;
    EXPECT_FALSE(status.Ok()) << "HTTP " << http_status;
  }
}

// ---------------------------------------------------------------------------
// MetadataMap
// ---------------------------------------------------------------------------
TEST(MetadataMapTest, EmptyMapAndClearExposeNoEntries) {
  MetadataMap map;
  EXPECT_EQ(map.Size(), 0U);
  EXPECT_TRUE(map.Entries().empty());
  EXPECT_FALSE(map.Get("missing").has_value());
  EXPECT_FALSE(map.GetBinary("missing-bin").has_value());
  EXPECT_FALSE(map.WireValue("missing").has_value());

  ASSERT_TRUE(map.Add("x-a", "1").has_value());
  ASSERT_TRUE(map.AddBinary("x-b-bin", std::array<std::byte, 1>{std::byte{0x01}}).has_value());
  ASSERT_EQ(map.Size(), 2U);
  map.Clear();
  EXPECT_EQ(map.Size(), 0U);
  EXPECT_TRUE(map.Entries().empty());
  EXPECT_FALSE(map.Get("x-a").has_value());
}
TEST(MetadataMapTest, AddStoresAsciiAndLooksUpCaseInsensitively) {
  MetadataMap map;
  ASSERT_TRUE(map.Add("X-Request-Id", "abc-123").has_value());
  ASSERT_EQ(map.Size(), 1U);

  for (const std::string_view key :
       std::array<std::string_view, 3>{"x-request-id", "X-REQUEST-ID", "x-ReQuEsT-Id"}) {
    const auto value = map.Get(key);
    ASSERT_TRUE(value.has_value()) << key;
    EXPECT_EQ(*value, "abc-123") << key;
  }
  const auto wire = map.WireValue("X-Request-Id");
  ASSERT_TRUE(wire.has_value());
  EXPECT_EQ(*wire, "abc-123");

  ASSERT_EQ(map.Entries().size(), 1U);
  EXPECT_EQ(map.Entries().front().key, "x-request-id");
  EXPECT_EQ(map.Entries().front().value, "abc-123");
  EXPECT_FALSE(map.Entries().front().binary);
  EXPECT_FALSE(map.GetBinary("x-request-id").has_value());
}
TEST(MetadataMapTest, DuplicateKeysAppendAndFirstValueWinsLookups) {
  MetadataMap map;
  ASSERT_TRUE(map.Add("x-key", "first").has_value());
  ASSERT_TRUE(map.Add("X-Key", "second").has_value());
  EXPECT_EQ(map.Size(), 2U);
  const auto value = map.Get("x-key");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "first");
  EXPECT_EQ(map.Entries()[0].value, "first");
  EXPECT_EQ(map.Entries()[1].value, "second");
}
TEST(MetadataMapTest, AddTreatsBinarySuffixValueAsRawBytes) {
  MetadataMap map;
  const std::string_view raw{"a\0b", 3U};
  ASSERT_TRUE(map.Add("Trace-Bin", raw).has_value());
  ASSERT_EQ(map.Size(), 1U);
  EXPECT_TRUE(map.Entries().front().binary);
  EXPECT_EQ(map.Entries().front().key, "trace-bin");
  EXPECT_EQ(map.Entries().front().value, "YQBi");

  const auto wire = map.WireValue("trace-bin");
  ASSERT_TRUE(wire.has_value());
  EXPECT_EQ(*wire, "YQBi");

  const auto decoded = map.Get("trace-bin");
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, std::string(raw));

  const auto binary = map.GetBinary("trace-bin");
  ASSERT_TRUE(binary.has_value());
  ASSERT_EQ(binary->size(), 3U);
  EXPECT_EQ((*binary)[0], std::byte{0x61});
  EXPECT_EQ((*binary)[1], std::byte{0x00});
  EXPECT_EQ((*binary)[2], std::byte{0x62});
}
TEST(MetadataMapTest, AddBinaryRoundTripsHighBytesAndRejectsAsciiKeys) {
  MetadataMap map;
  const std::array<std::byte, 4> raw{std::byte{0x00}, std::byte{0xFF}, std::byte{0x10},
                                     std::byte{0x80}};
  ASSERT_TRUE(map.AddBinary("Payload-Bin", raw).has_value());
  ASSERT_EQ(map.Size(), 1U);
  EXPECT_TRUE(map.Entries().front().binary);
  EXPECT_EQ(map.Entries().front().key, "payload-bin");
  EXPECT_EQ(map.Entries().front().value, "AP8QgA==");

  const auto binary = map.GetBinary("payload-bin");
  ASSERT_TRUE(binary.has_value());
  EXPECT_TRUE(std::ranges::equal(*binary, raw));

  const auto text = map.Get("payload-bin");
  ASSERT_TRUE(text.has_value());
  ASSERT_EQ(text->size(), raw.size());
  for (const auto index : std::views::iota(std::size_t{0U}, raw.size())) {
    EXPECT_EQ(static_cast<std::byte>((*text)[index]), raw[index]);
  }

  const auto rejected = map.AddBinary("plain", raw);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(map.Size(), 1U);
}
TEST(MetadataMapTest, RejectsInvalidKeysAndAsciiValuesWithoutMutatingTheMap) {
  MetadataMap map;
  ASSERT_TRUE(map.Add("x-key", "kept").has_value());
  for (const std::string_view key :
       std::array<std::string_view, 5>{"", "bad key", "bad\x01key", "bad\x7Fkey", "bad\xFFkey"}) {
    const auto rejected = map.Add(key, "value");
    ASSERT_FALSE(rejected.has_value()) << key;
    EXPECT_EQ(rejected.error().Code(), StatusCode::kInvalidArgument) << key;
  }
  for (const std::string_view value :
       std::array<std::string_view, 4>{"bad\x01", "bad\x1F", "bad\x7F", "bad\xFF"}) {
    const auto rejected = map.Add("x-key", value);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().Code(), StatusCode::kInvalidArgument);
  }
  EXPECT_EQ(map.Size(), 1U);
  const auto kept = map.Get("x-key");
  ASSERT_TRUE(kept.has_value());
  EXPECT_EQ(*kept, "kept");
}
TEST(MetadataMapTest, AcceptsTheVisibleAsciiBoundaries) {
  MetadataMap map;
  ASSERT_TRUE(map.Add("x-~key", " ").has_value());
  const auto space = map.Get("x-~key");
  ASSERT_TRUE(space.has_value());
  EXPECT_EQ(*space, " ");
  ASSERT_TRUE(map.Add("x-tilde", "~").has_value());
  const auto tilde = map.Get("x-tilde");
  ASSERT_TRUE(tilde.has_value());
  EXPECT_EQ(*tilde, "~");
  EXPECT_EQ(map.Size(), 2U);
}
TEST(Base64Test, EncodesRfc4648Vectors) {
  const std::array<std::pair<std::string_view, std::string_view>, 7> vectors{{
      {"", ""},
      {"f", "Zg=="},
      {"fo", "Zm8="},
      {"foo", "Zm9v"},
      {"foob", "Zm9vYg=="},
      {"fooba", "Zm9vYmE="},
      {"foobar", "Zm9vYmFy"},
  }};
  for (const auto& [plain, encoded] : vectors) {
    EXPECT_EQ(MetadataMap::Base64Encode(AsBytes(plain)), encoded) << plain;
  }

  const std::array<std::pair<std::string_view, std::string_view>, 8> decode_vectors{{
      {"", ""},
      {"Zg==", "f"},
      {"Zm8=", "fo"},
      {"Zm9v", "foo"},
      {"Zm9vYg==", "foob"},
      {"Zm9vYmE=", "fooba"},
      {"Zm9vYmFy", "foobar"},
      {"AAA=", std::string_view{"\0\0", 2U}},
  }};
  for (const auto& [encoded, plain] : decode_vectors) {
    const auto decoded = MetadataMap::Base64Decode(encoded);
    ASSERT_TRUE(decoded.has_value()) << encoded;
    EXPECT_TRUE(std::ranges::equal(*decoded, AsBytes(plain))) << encoded;
  }

  for (const std::string_view encoded :
       std::array<std::string_view, 10>{"A", "AAAA=A", "====", "A===", "AB==", "AAB=", "Zm9v!",
                                        "Zm9vYmFy\n", "Zg==Zg==", "=AAA"}) {
    EXPECT_FALSE(MetadataMap::Base64Decode(encoded).has_value()) << encoded;
  }
}

// ---------------------------------------------------------------------------
// GrpcTimeout
// ---------------------------------------------------------------------------
TEST(GrpcTimeoutTest, ParsesAllSixUnitsZeroAndLeadingZeros) {
  const std::array<std::pair<std::string_view, std::uint64_t>, 10> cases{{
      {"1H", 3'600'000'000'000ULL},
      {"2M", 120'000'000'000ULL},
      {"3S", 3'000'000'000ULL},
      {"4m", 4'000'000ULL},
      {"5u", 5'000ULL},
      {"6n", 6ULL},
      {"0n", 0ULL},
      {"0S", 0ULL},
      {"00000000n", 0ULL},
      {"00000001S", 1'000'000'000ULL},
  }};
  for (const auto& [wire, nanos] : cases) {
    const auto parsed = GrpcTimeout::Parse(wire);
    ASSERT_TRUE(parsed.has_value()) << wire;
    EXPECT_EQ(*parsed, nanos) << wire;
  }
}
TEST(GrpcTimeoutTest, AcceptsEightDigitsAndRejectsNineOrMalformedValues) {
  const auto eight = GrpcTimeout::Parse("99999999n");
  ASSERT_TRUE(eight.has_value());
  EXPECT_EQ(*eight, 99'999'999ULL);

  const auto eight_seconds = GrpcTimeout::Parse("12345678S");
  ASSERT_TRUE(eight_seconds.has_value());
  EXPECT_EQ(*eight_seconds, 12'345'678'000'000'000ULL);

  for (const std::string_view wire :
       std::array<std::string_view, 16>{"999999999n", "", "S", "n", "12", "1s", "1T", " 1S", "+1S",
                                        "-1S", "1.5S", "1S ", "1SS", "1 S", "0x10n", "1e9n"}) {
    const auto parsed = GrpcTimeout::Parse(wire);
    ASSERT_FALSE(parsed.has_value()) << wire;
    EXPECT_EQ(parsed.error().Code(), StatusCode::kInvalidArgument) << wire;
  }
}
TEST(GrpcTimeoutTest, RejectsValuesThatOverflowNanoseconds) {
  const auto overflow = GrpcTimeout::Parse("5124096H");
  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().Code(), StatusCode::kInvalidArgument);

  const auto edge = GrpcTimeout::Parse("5124095H");
  ASSERT_TRUE(edge.has_value());
  EXPECT_EQ(*edge, 18'446'742'000'000'000'000ULL);

  const auto huge_count = GrpcTimeout::Parse("99999999H");
  ASSERT_FALSE(huge_count.has_value());
  EXPECT_EQ(huge_count.error().Code(), StatusCode::kInvalidArgument);
}
TEST(GrpcTimeoutTest, FormatToPicksTheFinestUnitThatFitsEightDigits) {
  std::array<char, 16> buffer{};
  const auto format = [&buffer](std::uint64_t nanos) {
    return std::string{GrpcTimeout::FormatTo(nanos, buffer)};
  };

  EXPECT_EQ(format(0U), "0n");
  EXPECT_EQ(format(1U), "1n");
  EXPECT_EQ(format(999U), "999n");
  EXPECT_EQ(format(1'000U), "1000n");
  EXPECT_EQ(format(1'000'000U), "1000000n");
  EXPECT_EQ(format(99'999'999U), "99999999n");
  EXPECT_EQ(format(100'000'000U), "100000u");
  EXPECT_EQ(format(1'000'000'000U), "1000000u");
  EXPECT_EQ(format(99'999'999'000'000'000ULL), "99999999S");
  EXPECT_EQ(format(100'000'000'000'000ULL), "100000S");
  EXPECT_EQ(format(1'000'000'000'000U), "1000000m");
  EXPECT_EQ(format(3'600'000'000'000U), "3600000m");
  EXPECT_EQ(format(100'000'000'000'000'000ULL), "1666667M");
  EXPECT_EQ(format(10'000'000'000'000'000'000ULL), "2777778H");
}
TEST(GrpcTimeoutTest, FormatToRoundsUpAndRoundTripsRepresentableValues) {
  std::array<char, 16> buffer{};
  const auto rounded = GrpcTimeout::FormatTo(1'000'000'001ULL, buffer);
  EXPECT_EQ(rounded, "1000001u");
  const auto parsed = GrpcTimeout::Parse(rounded);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_GE(*parsed, 1'000'000'001ULL);
  EXPECT_LT(*parsed - 1'000'000'001ULL, 1'000ULL);

  const auto nearly_second = GrpcTimeout::FormatTo(999'999'999ULL, buffer);
  EXPECT_EQ(nearly_second, "1000000u");
  const auto nearly_parsed = GrpcTimeout::Parse(nearly_second);
  ASSERT_TRUE(nearly_parsed.has_value());
  EXPECT_EQ(*nearly_parsed, 1'000'000'000ULL);

  std::array<char, 16> round_trip_buffer{};
  for (const auto nanos : std::array<std::uint64_t, 7>{0ULL, 1ULL, 999ULL, 1'000ULL, 1'000'000ULL,
                                                       1'000'000'000ULL, 3'600'000'000'000ULL}) {
    const auto wire = GrpcTimeout::FormatTo(nanos, round_trip_buffer);
    const auto round_trip = GrpcTimeout::Parse(wire);
    ASSERT_TRUE(round_trip.has_value()) << wire;
    EXPECT_EQ(*round_trip, nanos) << wire;
  }
}
TEST(GrpcTimeoutTest, FormatToRequiresEnoughBufferSpace) {
  EXPECT_TRUE(GrpcTimeout::FormatTo(1U, std::span<char>{}).empty());

  std::array<char, 1> one_byte{};
  EXPECT_TRUE(GrpcTimeout::FormatTo(1U, one_byte).empty());

  std::array<char, 2> exact{};
  const auto written = GrpcTimeout::FormatTo(1U, exact);
  EXPECT_EQ(written, "1n");
  EXPECT_EQ(written.data(), exact.data());
  EXPECT_EQ(written.size(), 2U);

  std::array<char, 16> roomy{};
  EXPECT_EQ(GrpcTimeout::FormatTo(42U, roomy), "42n");
}
TEST(GrpcTimeoutTest, Uint64MaximumFormatsToACoarseCountThatParseRejects) {
  // No eight-digit grpc-timeout value represents UINT64_MAX nanoseconds exactly: only the
  // nanosecond unit divides it, and that count needs twenty digits. FormatTo rounds up to
  // whole hours, one hour beyond the uint64 nanosecond range; Parse reports that overflow
  // rather than wrapping. Both halves of the boundary are pinned here.
  std::array<char, 16> buffer{};
  const auto written = GrpcTimeout::FormatTo(std::numeric_limits<std::uint64_t>::max(), buffer);
  EXPECT_EQ(written, "5124096H");

  const auto parsed = GrpcTimeout::Parse(written);
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error().Code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace grpcxx
