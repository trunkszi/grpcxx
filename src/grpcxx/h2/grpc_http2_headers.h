#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <nexora/http/headers.hpp>
#include <nexora/http/message.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"
#include "grpcxx/core/timeout.h"
#include "grpcxx/h2/content_type.h"
#include "grpcxx/h2/grpc_http2_codec.h"

namespace grpcxx::h2 {

struct GrpcRequestHeaderSpec final {
  std::string_view scheme{"http"};
  std::string_view authority{"localhost"};
  std::string_view path{};
  std::string_view content_type{"application/grpc"};
  CompressionAlgorithm encoding{CompressionAlgorithm::kIdentity};
  std::string_view accept_encoding{CompressionRegistry::AcceptEncoding()};
  std::optional<std::uint64_t> timeout_ns{};
  const MetadataMap* metadata{nullptr};
  std::size_t max_metadata_bytes{65536uz};
};

struct ParsedGrpcRequestHeaders final {
  std::string_view scheme{};
  std::string_view authority{};
  std::string_view path{};
  std::string_view content_type{};
  CompressionAlgorithm request_encoding{CompressionAlgorithm::kIdentity};
  CompressionAlgorithm negotiated_response_encoding{CompressionAlgorithm::kIdentity};
  std::optional<std::uint64_t> timeout_ns{};
  MetadataMap metadata{};
};

struct ParsedGrpcResponseHeaders final {
  int http_status{0};
  CompressionAlgorithm encoding{CompressionAlgorithm::kIdentity};
  MetadataMap metadata{};
  bool has_grpc_status{false};
  Status grpc_status{Status::OkStatus()};
};

[[nodiscard]] inline std::expected<void, Status> AppendHeaderChecked(
    nexora::Headers<>& out, std::string_view name, std::string_view value) noexcept {
  if (auto res = out.Append(name, value); uarch::unlikely(!res.has_value())) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kResourceExhausted, "HTTP/2 header block capacity exceeded"));
  }
  return {};
}

[[nodiscard]] inline std::expected<nexora::Headers<>, Status> BuildGrpcRequestHeaders(
    const GrpcRequestHeaderSpec& spec) noexcept {
  if (uarch::unlikely(!IsValidRpcPath(spec.path))) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "RPC path must be /service/method"));
  }
  if (uarch::unlikely(spec.authority.empty() || !IsAsciiVisibleToken(spec.authority))) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "RPC authority must be printable ASCII"));
  }
  if (uarch::unlikely(spec.scheme != "http" && spec.scheme != "https")) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "RPC scheme must be http or https"));
  }
  if (uarch::unlikely(!h2_detail::ValidContentType(spec.content_type))) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "invalid gRPC content-type"));
  }
  const auto encoding_name = CompressionRegistry::EncodingName(spec.encoding);
  if (uarch::unlikely(encoding_name.empty())) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "unsupported request compression algorithm"));
  }
  if (uarch::unlikely(spec.timeout_ns.has_value() && *spec.timeout_ns == 0U)) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "RPC timeout must be positive"));
  }

  std::size_t metadata_bytes = 0uz;
  if (spec.metadata != nullptr) {
    for (const auto& entry : spec.metadata->Entries()) {
      if (uarch::unlikely(IsReservedClientMetadataKey(entry.key))) {
        return std::unexpected(
            Status{StatusCode::kInvalidArgument, "reserved request metadata key: " + entry.key});
      }
      metadata_bytes += entry.key.size() + entry.value.size();
    }
    if (uarch::unlikely(metadata_bytes > spec.max_metadata_bytes)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kResourceExhausted, "request metadata exceeds limit"));
    }
  }

  nexora::Headers<> headers{};
  if (auto ok = AppendHeaderChecked(headers, "content-type", spec.content_type); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (auto ok = AppendHeaderChecked(headers, "te", "trailers"); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (auto ok = AppendHeaderChecked(headers, "grpc-encoding", encoding_name); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (!spec.accept_encoding.empty()) {
    if (auto ok = AppendHeaderChecked(headers, "grpc-accept-encoding", spec.accept_encoding); !ok) {
      return std::unexpected(std::move(ok.error()));
    }
  }
  if (spec.timeout_ns.has_value()) {
    std::array<char, 16> timeout_buf{};
    const auto formatted = GrpcTimeout::FormatTo(*spec.timeout_ns, timeout_buf);
    if (uarch::unlikely(formatted.empty())) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kInvalidArgument, "cannot format grpc-timeout"));
    }
    if (auto ok = AppendHeaderChecked(headers, "grpc-timeout", formatted); !ok) {
      return std::unexpected(std::move(ok.error()));
    }
  }
  if (spec.metadata != nullptr) {
    for (const auto& entry : spec.metadata->Entries()) {
      if (auto ok = AppendHeaderChecked(headers, entry.key, entry.value); !ok) {
        return std::unexpected(std::move(ok.error()));
      }
    }
  }
  return headers;
}

[[nodiscard]] inline std::expected<ParsedGrpcRequestHeaders, Status> ParseGrpcRequestHeaders(
    nexora::Method method, std::string_view scheme, std::string_view authority,
    std::string_view path, std::span<const nexora::HeaderField> fields,
    CompressionAlgorithm preferred_response_encoding = CompressionAlgorithm::kIdentity,
    std::size_t max_metadata_bytes = 65536uz) noexcept {
  ParsedGrpcRequestHeaders out{};
  out.scheme = scheme;
  out.authority = authority;
  out.path = path;
  std::size_t header_bytes = scheme.size() + authority.size() + path.size();
  if (uarch::unlikely(header_bytes > max_metadata_bytes)) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kResourceExhausted, "request metadata exceeds limit"));
  }
  bool valid_method = (method == nexora::Method::kPost);
  bool valid_content_type = false;

  for (const auto& field : fields) {
    const auto key = field.name.View();
    const auto value = field.value.View();
    header_bytes += key.size() + value.size();
    if (uarch::unlikely(header_bytes > max_metadata_bytes)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kResourceExhausted, "request metadata exceeds limit"));
    }
    if (key == ":method") {
      valid_method = (value == "POST");
      continue;
    }
    if (key == ":scheme") {
      out.scheme = value;
      continue;
    }
    if (key == ":authority") {
      out.authority = value;
      continue;
    }
    if (key == ":path") {
      out.path = value;
      continue;
    }
    if (key.starts_with(':')) continue;

    if (key == "content-type") {
      out.content_type = value;
      valid_content_type = h2_detail::ValidContentType(value);
    } else if (key == "grpc-encoding") {
      auto parsed_enc = CompressionRegistry::ParseEncoding(value);
      if (uarch::unlikely(!parsed_enc.has_value())) {
        return std::unexpected(std::move(parsed_enc.error()));
      }
      out.request_encoding = *parsed_enc;
    } else if (key == "grpc-accept-encoding") {
      out.negotiated_response_encoding =
          NegotiateAcceptedEncoding(value, preferred_response_encoding);
    } else if (key == "grpc-timeout") {
      auto parsed_timeout = GrpcTimeout::Parse(value);
      if (uarch::unlikely(!parsed_timeout.has_value())) {
        return std::unexpected(std::move(parsed_timeout.error()));
      }
      out.timeout_ns = *parsed_timeout;
    }
    if (auto added = AppendReceivedMetadata(out.metadata, key, value);
        uarch::unlikely(!added.has_value())) {
      return std::unexpected(std::move(added.error()));
    }
  }

  if (uarch::unlikely(!valid_method || !valid_content_type || !IsValidRpcPath(out.path))) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInternal, "invalid gRPC HTTP/2 request headers"));
  }
  return out;
}

[[nodiscard]] inline std::expected<ParsedGrpcRequestHeaders, Status> ParseGrpcRequestHeaders(
    std::span<const nexora::HeaderField> fields,
    CompressionAlgorithm preferred_response_encoding = CompressionAlgorithm::kIdentity,
    std::size_t max_metadata_bytes = 65536uz) noexcept {
  return ParseGrpcRequestHeaders(nexora::Method::kGet, {}, {}, {}, fields,
                                 preferred_response_encoding, max_metadata_bytes);
}

[[nodiscard]] inline std::expected<nexora::Headers<>, Status> BuildGrpcResponseHeaders(
    CompressionAlgorithm response_encoding = CompressionAlgorithm::kIdentity,
    const MetadataMap* initial_metadata = nullptr,
    std::string_view default_content_type = "application/grpc",
    std::size_t max_metadata_bytes = 65536uz) noexcept {
  const std::string_view content_type = default_content_type;
  std::size_t metadata_bytes = 0uz;
  if (initial_metadata != nullptr) {
    for (const auto& entry : initial_metadata->Entries()) {
      if (uarch::unlikely(IsReservedServerMetadataKey(entry.key))) {
        return std::unexpected(
            MakeWireStatus(StatusCode::kInvalidArgument, "reserved response header"));
      }
      metadata_bytes += entry.key.size() + entry.value.size();
    }
    if (uarch::unlikely(metadata_bytes > max_metadata_bytes)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kResourceExhausted, "response metadata exceeds limit"));
    }
  }
  const auto encoding_name = CompressionRegistry::EncodingName(response_encoding);
  if (uarch::unlikely(encoding_name.empty())) {
    return std::unexpected(
        MakeWireStatus(StatusCode::kInvalidArgument, "unsupported response compression"));
  }

  nexora::Headers<> headers{};
  if (auto ok = AppendHeaderChecked(headers, "content-type", content_type); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (auto ok = AppendHeaderChecked(headers, "grpc-accept-encoding",
                                    CompressionRegistry::AcceptEncoding());
      !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (auto ok = AppendHeaderChecked(headers, "grpc-encoding", encoding_name); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (initial_metadata != nullptr) {
    for (const auto& entry : initial_metadata->Entries()) {
      if (auto ok = AppendHeaderChecked(headers, entry.key, entry.value); !ok) {
        return std::unexpected(std::move(ok.error()));
      }
    }
  }
  return headers;
}

[[nodiscard]] inline std::expected<nexora::Headers<>, Status> BuildGrpcTrailers(
    const Status& status, const MetadataMap* trailing_metadata = nullptr,
    std::size_t max_metadata_bytes = 65536uz) noexcept {
  std::size_t metadata_bytes = 0uz;
  if (trailing_metadata != nullptr) {
    for (const auto& entry : trailing_metadata->Entries()) {
      if (uarch::unlikely(IsReservedServerMetadataKey(entry.key))) {
        return std::unexpected(
            MakeWireStatus(StatusCode::kInvalidArgument, "reserved response trailer"));
      }
      metadata_bytes += entry.key.size() + entry.value.size();
    }
    if (uarch::unlikely(metadata_bytes > max_metadata_bytes)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kResourceExhausted, "response trailers exceed limit"));
    }
  }

  nexora::Headers<> trailers{};
  std::array<char, 4> code_scratch{};
  const auto code_str = FormatGrpcStatusCode(status.Code(), code_scratch);
  if (auto ok = AppendHeaderChecked(trailers, "grpc-status", code_str); !ok) {
    return std::unexpected(std::move(ok.error()));
  }
  if (!status.Message().empty()) {
    const auto encoded_msg = Status::PercentEncode(status.Message());
    if (auto ok = AppendHeaderChecked(trailers, "grpc-message", encoded_msg); !ok) {
      return std::unexpected(std::move(ok.error()));
    }
  }
  if (trailing_metadata != nullptr) {
    for (const auto& entry : trailing_metadata->Entries()) {
      if (auto ok = AppendHeaderChecked(trailers, entry.key, entry.value); !ok) {
        return std::unexpected(std::move(ok.error()));
      }
    }
  }
  return trailers;
}

[[nodiscard]] inline std::expected<ParsedGrpcResponseHeaders, Status> ParseGrpcResponseHeaders(
    int pre_parsed_http_status, std::span<const nexora::HeaderField> fields, bool is_trailers_block,
    bool end_stream, std::size_t max_metadata_bytes = 65536uz) noexcept {
  ParsedGrpcResponseHeaders out{};
  out.http_status = pre_parsed_http_status;
  std::size_t header_bytes = 0uz;
  bool saw_status = (!is_trailers_block && pre_parsed_http_status > 0);
  bool valid_content_type = false;

  for (const auto& field : fields) {
    const auto key = field.name.View();
    const auto value = field.value.View();
    header_bytes += key.size() + value.size();
    if (uarch::unlikely(header_bytes > max_metadata_bytes)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kResourceExhausted, "response metadata exceeds limit"));
    }
    if (key == ":status") {
      if (uarch::unlikely(is_trailers_block || saw_status)) {
        return std::unexpected(MakeWireStatus(StatusCode::kInternal, "invalid HTTP status"));
      }
      const auto parsed_http = ParseDecimalInt(value);
      if (uarch::unlikely(!parsed_http.has_value())) {
        return std::unexpected(MakeWireStatus(StatusCode::kInternal, "invalid HTTP status"));
      }
      out.http_status = *parsed_http;
      saw_status = true;
      continue;
    }
    if (key.starts_with(':')) continue;

    if (uarch::unlikely((key == "grpc-status" || key == "grpc-message" || key == "grpc-encoding" ||
                         key == "content-type") &&
                        out.metadata.WireValue(key).has_value())) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kInternal, "duplicate reserved response metadata"));
    }
    if (key == "content-type") {
      valid_content_type = h2_detail::ValidContentType(value);
    } else if (key == "grpc-encoding") {
      if (uarch::unlikely(is_trailers_block)) {
        return std::unexpected(
            MakeWireStatus(StatusCode::kInternal, "grpc-encoding is not allowed in trailers"));
      }
      auto parsed_enc = CompressionRegistry::ParseEncoding(value);
      if (uarch::unlikely(!parsed_enc.has_value())) {
        return std::unexpected(std::move(parsed_enc.error()));
      }
      out.encoding = *parsed_enc;
    }

    if (auto added = AppendReceivedMetadata(out.metadata, key, value);
        uarch::unlikely(!added.has_value())) {
      return std::unexpected(std::move(added.error()));
    }
  }

  if (!is_trailers_block) {
    if (uarch::unlikely(!saw_status || out.http_status != 200)) {
      return std::unexpected(saw_status ? Status::FromHttpStatus(out.http_status)
                                        : MakeWireStatus(StatusCode::kInternal, "missing :status"));
    }
    if (uarch::unlikely(!valid_content_type)) {
      return std::unexpected(MakeWireStatus(StatusCode::kInternal, "response is not gRPC"));
    }
  }

  if (const auto status_wire = out.metadata.Get("grpc-status")) {
    auto parsed_code = ParseGrpcStatusCode(*status_wire);
    if (uarch::unlikely(!parsed_code.has_value())) {
      return std::unexpected(std::move(parsed_code.error()));
    }
    if (uarch::unlikely(!end_stream)) {
      return std::unexpected(
          MakeWireStatus(StatusCode::kInternal, "grpc-status without END_STREAM"));
    }
    std::string decoded_message{};
    if (const auto msg_wire = out.metadata.Get("grpc-message")) {
      auto decoded = Status::PercentDecode(*msg_wire);
      if (uarch::unlikely(!decoded.has_value())) {
        return std::unexpected(std::move(decoded.error()));
      }
      decoded_message = std::move(*decoded);
    }
    out.has_grpc_status = true;
    out.grpc_status = Status{*parsed_code, std::move(decoded_message)};
  }

  return out;
}

[[nodiscard]] inline std::expected<ParsedGrpcResponseHeaders, Status> ParseGrpcResponseHeaders(
    std::span<const nexora::HeaderField> fields, bool is_trailers_block, bool end_stream,
    std::size_t max_metadata_bytes = 65536uz) noexcept {
  return ParseGrpcResponseHeaders(0, fields, is_trailers_block, end_stream, max_metadata_bytes);
}

}  // namespace grpcxx::h2
