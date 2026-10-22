/**
 * Generated OrderBook contracts use real codecs over two crossed HTTP/2 connections.
 * Each peer acts as a client and server. Quote is immediate; PlaceOrder is deferred.
 * See README.md for the GCC reflection build and codec selection commands.
 */
#include <fmt/format.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <cstdio>
#include <utility>

#include "order_book_peer.h"
#include "order_book_traits.h"

namespace examples::order_book {

/** Preserve the aligned serialization round trip using this example's own generated IDLs. */
template <typename Traits>
auto AlignedRoundTrip() noexcept -> Result {
  typename Traits::Codec codec;
  Traits::Configure(codec);
  auto input = Traits::MakeQuote(73);
  grpcxx::AlignedMessageBuf buffer;
  if (auto encoded = codec.Encode(input, buffer); !encoded) {
    return std::unexpected(std::move(encoded.error()));
  }
  const auto address = reinterpret_cast<std::uintptr_t>(buffer.Payload().data());
  if (buffer.Payload().empty() || address % 8 != 0) return Failure("payload is not 8-byte aligned");
  typename Traits::QuoteRequest decoded;
  if (auto result = codec.Decode(buffer.Payload(), decoded); !result) {
    return std::unexpected(std::move(result.error()));
  }
  if (Traits::QuoteId(decoded) != 73 || !Traits::ValidQuote(decoded))
    return Failure("codec round trip mismatch");
#if defined(GRPCXX_ENABLE_CAPNP)
  if constexpr (std::same_as<typename Traits::Codec, grpcxx::CapnpCodec>) {
    auto reader = codec.ReadAligned(buffer.Payload());
    if (!reader) return std::unexpected(std::move(reader.error()));
    const auto symbol =
        (*reader)->template getRoot<typename Traits::QuoteSchema>().getRequest().getSymbol();
    const auto borrowed = reinterpret_cast<std::uintptr_t>(symbol.cStr());
    if (borrowed < address || borrowed - address >= buffer.Payload().size() ||
        symbol != kSymbol.data()) {
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
  Completion<typename Traits::QuoteResponse> left_quote, right_quote;
  Completion<typename Traits::OrderResponse> left_order, right_order;
  std::array<grpcxx::ClientContext, 4> contexts;
  /** Stubs detach callbacks before the response storage, contexts, codecs, or sessions are
   * destroyed. */
  grpcxx::ClientStub<typename Traits::Contract, typename Traits::Codec> left_stub(
      left.client_, left.client_codec_);
  grpcxx::ClientStub<typename Traits::Contract, typename Traits::Codec> right_stub(
      right.client_, right.client_codec_);
  if (auto call = left_stub.template CallUnary<0>(contexts[0], Traits::MakeQuote(73),
                                                  left_quote.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call = left_stub.template CallUnary<1>(contexts[1], Traits::MakeOrder(74, 3),
                                                  left_order.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call = right_stub.template CallUnary<0>(contexts[2], Traits::MakeQuote(173),
                                                   right_quote.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto call = right_stub.template CallUnary<1>(contexts[3], Traits::MakeOrder(174, 5),
                                                   right_order.Callback());
      !call) {
    return std::unexpected(std::move(call.error()));
  }
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left_quote.Ok() || !right_quote.Ok() || left_order.count_ != 0 || right_order.count_ != 0 ||
      !left.service_.pending_ || !right.service_.pending_ || !left.service_.pending_->Current() ||
      !right.service_.pending_->Current()) {
    return Failure("immediate/deferred completion boundary violated");
  }
  if (auto completed = left.service_.CompletePending(); !completed)
    return std::unexpected(std::move(completed.error()));
  if (auto completed = right.service_.CompletePending(); !completed)
    return std::unexpected(std::move(completed.error()));
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left_order.Ok() || !right_order.Ok() ||
      !Traits::MatchesQuote(*left_quote.response_, 73, 2) ||
      !Traits::MatchesQuote(*right_quote.response_, 173, 1) ||
      !Traits::MatchesOrder(*left_order.response_, 74, 3, 2) ||
      !Traits::MatchesOrder(*right_order.response_, 174, 5, 1)) {
    return Failure("cross-call response or grpc-status mismatch");
  }
  if (auto driven = Drive(left, right); !driven) return std::unexpected(std::move(driven.error()));
  if (!left.Idle() || !right.Idle() || left.service_.quotes_ != 1 || right.service_.quotes_ != 1 ||
      left.service_.orders_ != 1 || right.service_.orders_ != 1 || !left_quote.Ok() ||
      !right_quote.Ok() || !left_order.Ok() || !right_order.Ok()) {
    return Failure("duplicate callback, dispatch failure, or live stream after completion");
  }
  fmt::print(
      "{}: peer 1 <-> peer 2, immediate Quote + deferred PlaceOrder, 4 callbacks, grpc-status=0\n",
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
               static_cast<int>(result.error().Code()), result.error().Message());
  }
  return result.has_value();
}

}  // namespace examples::order_book

int main() {
  bool success = true;
#if defined(GRPCXX_ENABLE_PROTOBUF)
  success &= examples::order_book::Run<examples::order_book::ProtobufTraits>();
#endif
#if defined(GRPCXX_ENABLE_FORY)
  success &= examples::order_book::Run<examples::order_book::ForyTraits>();
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
  success &= examples::order_book::Run<examples::order_book::CapnpTraits>();
#endif
  return success ? 0 : 1;
}
