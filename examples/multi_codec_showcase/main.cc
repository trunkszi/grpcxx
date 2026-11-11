/**
 * Generated RecordStore contracts use real codecs over two crossed HTTP/2 connections.
 * Each peer acts as a client and server. Get is immediate; Put is deferred.
 * See README.md for the GCC reflection build and codec selection commands.
 */
#include <fmt/format.h>

#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <cstdio>
#include <utility>

#include "record_batch_peer.h"
#include "record_batch_traits.h"

namespace examples::record_batch {

/** Preserve the aligned serialization round trip using this example's own generated IDLs. */
template <typename Traits>
auto AlignedRoundTrip() noexcept -> Result {
  typename Traits::Codec codec;
  Traits::Configure(codec);
  auto input = Traits::MakeGet(73);
  grpcxx::AlignedMessageBuf buffer;
  if (auto encoded = codec.Encode(input, buffer); !encoded) {
    return std::unexpected(std::move(encoded.error()));
  }
  const auto address = std::bit_cast<std::uintptr_t>(buffer.Payload().data());
  if (buffer.Payload().empty() || address % 8 != 0) return Failure("payload is not 8-byte aligned");
  typename Traits::GetRequest decoded;
  if (auto result = codec.Decode(buffer.Payload(), decoded); !result) {
    return std::unexpected(std::move(result.error()));
  }
  if (Traits::RecordId(decoded) != 73 || !Traits::ValidGet(decoded))
    return Failure("codec round trip mismatch");
#if defined(GRPCXX_ENABLE_CAPNP)
  if constexpr (std::same_as<typename Traits::Codec, grpcxx::CapnpCodec>) {
    auto reader = codec.ReadAligned(buffer.Payload());
    if (!reader) return std::unexpected(std::move(reader.error()));
    const auto tag =
        (*reader)->template getRoot<typename Traits::GetSchema>().getRequest().getTag();
    const auto borrowed = std::bit_cast<std::uintptr_t>(tag.cStr());
    if (borrowed < address || borrowed - address >= buffer.Payload().size() || tag != kTag.data()) {
      return Failure("Cap'n Proto text does not borrow aligned payload");
    }
  }
#endif
  fmt::print("{}: aligned round trip, {} payload bytes\n", Traits::Codec::kContentType,
             buffer.Payload().size());
  return {};
}

/** Initiate both RPC shapes from each side and validate routing, status, deferral, and teardown. */
template <typename Traits>
auto CrossCall() noexcept -> Result {
  Peer<Traits> left(1), right(2);
  if (!left.client_.Ready() || !left.server_.Ready() || !right.client_.Ready() ||
      !right.server_.Ready()) {
    return Failure("HTTP/2 session initialization failed");
  }
  Completion<typename Traits::GetReply> left_get, right_get;
  Completion<typename Traits::PutReply> left_put, right_put;
  std::array<grpcxx::ClientContext, 4> contexts;
  /** Stubs detach callbacks before the response storage, contexts, codecs, or sessions are
   * destroyed. */
  grpcxx::ClientStub<typename Traits::Contract, typename Traits::Codec> left_stub(
      left.client_, left.client_codec_);
  grpcxx::ClientStub<typename Traits::Contract, typename Traits::Codec> right_stub(
      right.client_, right.client_codec_);
  if (auto call =
          left_stub.template CallUnary<0>(contexts[0], Traits::MakeGet(73), left_get.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call =
          left_stub.template CallUnary<1>(contexts[1], Traits::MakePut(74, 3), left_put.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call =
          right_stub.template CallUnary<0>(contexts[2], Traits::MakeGet(173), right_get.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call = right_stub.template CallUnary<1>(contexts[3], Traits::MakePut(174, 5),
                                                   right_put.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left_get.Ok() || !right_get.Ok() || left_put.count_ != 0 || right_put.count_ != 0 ||
      !left.service_.pending_ || !right.service_.pending_ || !left.service_.pending_->Current() ||
      !right.service_.pending_->Current()) {
    return Failure("immediate/deferred completion boundary violated");
  }
  if (auto completed = left.service_.CompletePending(); !completed)
    return std::unexpected(std::move(completed.error()));
  if (auto completed = right.service_.CompletePending(); !completed)
    return std::unexpected(std::move(completed.error()));
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left_put.Ok() || !right_put.Ok() || !Traits::MatchesGet(*left_get.response_, 73, 2) ||
      !Traits::MatchesGet(*right_get.response_, 173, 1) ||
      !Traits::MatchesPut(*left_put.response_, 74, 3, 2) ||
      !Traits::MatchesPut(*right_put.response_, 174, 5, 1)) {
    return Failure("cross-call response or grpc-status mismatch");
  }
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left.Idle() || !right.Idle() || left.service_.gets_ != 1 || right.service_.gets_ != 1 ||
      left.service_.puts_ != 1 || right.service_.puts_ != 1 || !left_get.Ok() || !right_get.Ok() ||
      !left_put.Ok() || !right_put.Ok()) {
    return Failure("duplicate callback, dispatch failure, or live stream after completion");
  }
  fmt::print("{}: peer 1 <-> peer 2, immediate Get + deferred Put, 4 callbacks, grpc-status=0\n",
             Traits::Codec::kContentType);
  return {};
}

/** Every enabled backend must pass its aligned payload and generated contract scenarios. */
template <typename Traits>
bool Run() noexcept {
  auto result = AlignedRoundTrip<Traits>();
  if (result) result = CrossCall<Traits>();
  if (!result) {
    fmt::print(stderr, "{} failed: status={} {}\n", Traits::Codec::kContentType,
               static_cast<int>(std::to_underlying(result.error().Code())),
               result.error().Message());
  }
  return result.has_value();
}

}  // namespace examples::record_batch

int main() {
  bool success = true;
#if defined(GRPCXX_ENABLE_PROTOBUF)
  success &= examples::record_batch::Run<examples::record_batch::ProtobufTraits>();
#endif
#if defined(GRPCXX_ENABLE_FORY)
  success &= examples::record_batch::Run<examples::record_batch::ForyTraits>();
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
  success &= examples::record_batch::Run<examples::record_batch::CapnpTraits>();
#endif
  return success ? 0 : 1;
}
