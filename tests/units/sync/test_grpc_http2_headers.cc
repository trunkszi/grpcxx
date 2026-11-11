#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "grpcxx/h2/grpc_http2_headers.h"
#include "grpcxx/sync/call_options.h"

namespace grpcxx {
namespace {

nexora::HeaderField MakeField(std::string_view name, std::string_view value) {
  nexora::HeaderField f{};
  EXPECT_TRUE(f.name.TryAssign(name));
  EXPECT_TRUE(f.value.TryAssign(value));
  return f;
}

TEST(GrpcHttp2HeadersTest, RequestHeadersRoundtrip) {
  MetadataMap req_meta{};
  ASSERT_TRUE(req_meta.Add("x-db-name", "public").has_value());
  const std::array<std::byte, 5> raw_bin{std::byte{0x00}, std::byte{0x01}, std::byte{0xfe},
                                         std::byte{0xff}, std::byte{0x42}};
  ASSERT_TRUE(req_meta.AddBinary("x-trace-bin", raw_bin).has_value());

  const h2::GrpcRequestHeaderSpec spec{
      .scheme = "https",
      .authority = "db.example.internal:4001",
      .path = "/test.v1.EchoService/Unary",
      .content_type = "application/grpc+proto",
      .encoding = CompressionAlgorithm::kZstd,
      .accept_encoding = CompressionRegistry::AcceptEncoding(),
      .timeout_ns = 250'000'000ULL,
      .metadata = &req_meta,
      .max_metadata_bytes = 65536uz,
  };
  auto built = h2::BuildGrpcRequestHeaders(spec);
  ASSERT_TRUE(built.has_value());
  EXPECT_EQ(built->Get("te"), "trailers");
  EXPECT_EQ(built->Get("content-type"), "application/grpc+proto");
  EXPECT_EQ(built->Get("grpc-encoding"), "zstd");
  EXPECT_EQ(built->Get("grpc-timeout"), "250000u");

  auto parsed = h2::ParseGrpcRequestHeaders(
      nexora::Method::kPost, "https", "db.example.internal:4001", "/test.v1.EchoService/Unary",
      built->Fields(), CompressionAlgorithm::kLz4);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->scheme, "https");
  EXPECT_EQ(parsed->authority, "db.example.internal:4001");
  EXPECT_EQ(parsed->path, "/test.v1.EchoService/Unary");
  EXPECT_EQ(parsed->request_encoding, CompressionAlgorithm::kZstd);
  EXPECT_EQ(parsed->negotiated_response_encoding, CompressionAlgorithm::kLz4);
  EXPECT_EQ(parsed->timeout_ns, 250'000'000ULL);
  EXPECT_EQ(parsed->metadata.Get("x-db-name"), "public");
  const auto decoded_bin = parsed->metadata.GetBinary("x-trace-bin");
  ASSERT_TRUE(decoded_bin.has_value());
  ASSERT_EQ(decoded_bin->size(), raw_bin.size());
  EXPECT_TRUE(std::equal(decoded_bin->begin(), decoded_bin->end(), raw_bin.begin()));
}

TEST(GrpcHttp2HeadersTest, CommaJoinedBinaryMetadata) {
  std::vector<nexora::HeaderField> raw_fields{
      MakeField(":method", "POST"),
      MakeField(":scheme", "http"),
      MakeField(":authority", "localhost"),
      MakeField(":path", "/svc.Test/Unary"),
      MakeField("content-type", "application/grpc"),
      MakeField("x-multi-bin", "AQID, BAUG"),
  };
  auto parsed = h2::ParseGrpcRequestHeaders(raw_fields);
  ASSERT_TRUE(parsed.has_value());
  std::vector<std::vector<std::byte>> all_bins{};
  for (const auto& entry : parsed->metadata.Entries()) {
    if (entry.key == "x-multi-bin") {
      auto decoded = MetadataMap::Base64Decode(entry.value);
      ASSERT_TRUE(decoded.has_value());
      all_bins.push_back(std::move(*decoded));
    }
  }
  ASSERT_EQ(all_bins.size(), 2uz);
  EXPECT_EQ(all_bins[0], (std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}}));
  EXPECT_EQ(all_bins[1], (std::vector<std::byte>{std::byte{4}, std::byte{5}, std::byte{6}}));
}

TEST(GrpcHttp2HeadersTest, RejectsInvalidHeadersAndOversize) {
  MetadataMap bad_meta{};
  ASSERT_TRUE(bad_meta.Add("grpc-timeout", "5S").has_value());
  auto reserved = h2::BuildGrpcRequestHeaders(h2::GrpcRequestHeaderSpec{
      .path = "/svc.Test/Call",
      .metadata = &bad_meta,
  });
  ASSERT_FALSE(reserved.has_value());
  EXPECT_EQ(reserved.error().Code(), StatusCode::kInvalidArgument);

  auto bad_path =
      h2::BuildGrpcRequestHeaders(h2::GrpcRequestHeaderSpec{.path = "no_leading_slash"});
  ASSERT_FALSE(bad_path.has_value());
  EXPECT_EQ(bad_path.error().Code(), StatusCode::kInvalidArgument);

  MetadataMap huge_meta{};
  const std::string chunk(7000uz, 'a');
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(huge_meta.Add("x-large-" + std::to_string(i), chunk).has_value());
  }
  auto oversize = h2::BuildGrpcRequestHeaders(h2::GrpcRequestHeaderSpec{
      .path = "/svc.Test/Call",
      .metadata = &huge_meta,
      .max_metadata_bytes = 65536uz,
  });
  ASSERT_FALSE(oversize.has_value());
  EXPECT_EQ(oversize.error().Code(), StatusCode::kResourceExhausted);
}

TEST(GrpcHttp2HeadersTest, ResponseHeadersAndTrailers) {
  MetadataMap initial{};
  ASSERT_TRUE(initial.Add("x-server-node", "n1").has_value());
  auto resp_hdrs = h2::BuildGrpcResponseHeaders(CompressionAlgorithm::kGzip, &initial);
  ASSERT_TRUE(resp_hdrs.has_value());

  auto parsed_initial =
      h2::ParseGrpcResponseHeaders(200, resp_hdrs->Fields(), false, false, 65536uz);
  ASSERT_TRUE(parsed_initial.has_value());
  EXPECT_FALSE(parsed_initial->has_grpc_status);
  EXPECT_EQ(parsed_initial->encoding, CompressionAlgorithm::kGzip);
  EXPECT_EQ(parsed_initial->metadata.Get("x-server-node"), "n1");

  MetadataMap trailing{};
  ASSERT_TRUE(trailing.Add("x-cost-us", "42").has_value());
  const Status err_status{StatusCode::kInvalidArgument, "bad column: ts%1 != UTF-8 \xC3\xA9"};
  auto trailers = h2::BuildGrpcTrailers(err_status, &trailing);
  ASSERT_TRUE(trailers.has_value());

  auto parsed_trailers = h2::ParseGrpcResponseHeaders(200, trailers->Fields(), true, true, 65536uz);
  ASSERT_TRUE(parsed_trailers.has_value());
  EXPECT_TRUE(parsed_trailers->has_grpc_status);
  EXPECT_EQ(parsed_trailers->grpc_status.Code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(parsed_trailers->grpc_status.Message(), err_status.Message());
  EXPECT_EQ(parsed_trailers->metadata.WireValue("grpc-status"), "3");
  EXPECT_EQ(parsed_trailers->metadata.Get("x-cost-us"), "42");
}

TEST(GrpcHttp2HeadersTest, TrailersOnlyAndStatusMap) {
  std::vector<nexora::HeaderField> trailers_only{
      MakeField(":status", "200"),
      MakeField("content-type", "application/grpc"),
      MakeField("grpc-status", "12"),
      MakeField("grpc-message", "not%20implemented"),
  };
  auto parsed_to = h2::ParseGrpcResponseHeaders(trailers_only, false, true);
  ASSERT_TRUE(parsed_to.has_value());
  EXPECT_TRUE(parsed_to->has_grpc_status);
  EXPECT_EQ(parsed_to->grpc_status.Code(), StatusCode::kUnimplemented);
  EXPECT_EQ(parsed_to->grpc_status.Message(), "not implemented");

  std::vector<nexora::HeaderField> bad_trailers{
      MakeField("grpc-encoding", "gzip"),
      MakeField("grpc-status", "0"),
  };
  auto enc_in_trailers = h2::ParseGrpcResponseHeaders(200, bad_trailers, true, true);
  ASSERT_FALSE(enc_in_trailers.has_value());
  EXPECT_EQ(enc_in_trailers.error().Code(), StatusCode::kInternal);

  struct HttpCase {
    int http_code;
    StatusCode expected;
  };
  constexpr std::array<HttpCase, 8> kCases{{
      {400, StatusCode::kInternal},
      {401, StatusCode::kUnauthenticated},
      {403, StatusCode::kPermissionDenied},
      {404, StatusCode::kUnimplemented},
      {429, StatusCode::kUnavailable},
      {502, StatusCode::kUnavailable},
      {503, StatusCode::kUnavailable},
      {418, StatusCode::kUnknown},
  }};
  for (const auto& tc : kCases) {
    auto res = h2::ParseGrpcResponseHeaders(tc.http_code, {}, false, true);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().Code(), tc.expected);
  }
}

TEST(GrpcHttp2HeadersTest, EndpointSchemeParsing) {
  auto plain = SyncEndpoint::Parse("127.0.0.1:4001");
  ASSERT_TRUE(plain.has_value());
  EXPECT_EQ(plain->Scheme(), EndpointScheme::kGrpc);
  EXPECT_EQ(plain->Host(), "127.0.0.1");
  EXPECT_EQ(plain->Port(), 4001U);

  auto tls = SyncEndpoint::Parse("grpcs://[::1]:4433");
  ASSERT_TRUE(tls.has_value());
  EXPECT_EQ(tls->Scheme(), EndpointScheme::kGrpcs);
  EXPECT_EQ(tls->Host(), "::1");
  EXPECT_EQ(tls->Port(), 4433U);
  EXPECT_EQ(tls->Authority(), "[::1]:4433");

  auto uds = SyncEndpoint::Parse("unix:///tmp/grpcxx_test.sock");
  ASSERT_TRUE(uds.has_value());
  EXPECT_EQ(uds->Scheme(), EndpointScheme::kUnix);
  EXPECT_EQ(uds->UnixPath(), "/tmp/grpcxx_test.sock");

  EXPECT_FALSE(SyncEndpoint::Parse("http://127.0.0.1:4001").has_value());
  EXPECT_FALSE(SyncEndpoint::Parse("grpc://127.0.0.1:99999").has_value());
  EXPECT_FALSE(SyncEndpoint::Parse("grpc://127.0.0.1:4001/extra").has_value());
}

}  // namespace
}  // namespace grpcxx
