#pragma once

#include <fmt/format.h>
#include <grpcxx/codec/capnp_codec.h>
#include <grpcxx/codec/codec_concept.h>
#include <grpcxx/codec/fory_codec.h>
#include <grpcxx/codec/protobuf_codec.h>

#include <cstdint>
#include <expected>
#include <string>

#if defined(GRPCXX_ENABLE_PROTOBUF)
#include "showcase.pb.h"
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
#include "showcase.capnp.h"
#endif

namespace grpcxx::codec_fixture_support {

#if defined(GRPCXX_ENABLE_FORY)
/** Small registered trade message shared by the benchmark and showcase. */
struct Trade {
  std::int64_t id{};
  std::string symbol;
  FORY_STRUCT(Trade, id, symbol);
};
#endif

/** Encode and decode a real backend message; return its wire payload size. */
#if defined(GRPCXX_ENABLE_PROTOBUF)
inline auto ProtobufRoundTrip(ProtobufCodec& codec, AlignedMessageBuf& buffer) noexcept
    -> std::expected<std::size_t, Status> {
  codec_fixture::Request input;
  input.set_id(73);
  input.set_symbol("EURUSD");
  if (auto result = codec.Encode(input, buffer); !result) {
    return std::unexpected(result.error());
  }
  codec_fixture::Request output;
  if (auto result = codec.Decode(buffer.Payload(), output); !result) {
    return std::unexpected(result.error());
  }
  if (output.id() != input.id() || output.symbol() != input.symbol()) {
    return std::unexpected(Status{StatusCode::kInternal, "Protobuf round trip mismatch"});
  }
  return buffer.Payload().size();
}
#endif

#if defined(GRPCXX_ENABLE_FORY)
inline auto ForyRoundTrip(ForyCodec& codec, AlignedMessageBuf& buffer) noexcept
    -> std::expected<std::size_t, Status> {
  const Trade input{73, "EURUSD"};
  if (auto result = codec.Encode(input, buffer); !result) {
    return std::unexpected(result.error());
  }
  Trade output;
  if (auto result = codec.Decode(buffer.Payload(), output); !result) {
    return std::unexpected(result.error());
  }
  if (output.id != input.id || output.symbol != input.symbol) {
    return std::unexpected(Status{StatusCode::kInternal, "Fory round trip mismatch"});
  }
  return buffer.Payload().size();
}
#endif

#if defined(GRPCXX_ENABLE_CAPNP)
inline auto CapnpRoundTrip(CapnpCodec& codec, AlignedMessageBuf& buffer) noexcept
    -> std::expected<std::size_t, Status> {
  capnp::MallocMessageBuilder input;
  auto root = input.initRoot<capnp_fixture::nested::Request>();
  root.setId(73);
  root.setSymbol("EURUSD");
  if (auto result = codec.Encode(root.asReader(), buffer); !result) {
    return std::unexpected(result.error());
  }
  auto decoded = codec.ReadAligned(buffer.Payload());
  if (!decoded) {
    return std::unexpected(decoded.error());
  }
  const auto output = (*decoded)->getRoot<capnp_fixture::nested::Request>();
  if (output.getId() != 73 || output.getSymbol() != "EURUSD") {
    return std::unexpected(Status{StatusCode::kInternal, "Capnp round trip mismatch"});
  }
  return buffer.Payload().size();
}
#endif

}  // namespace grpcxx::codec_fixture_support
