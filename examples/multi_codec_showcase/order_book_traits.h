#pragma once

#include <grpcxx/codec/capnp_codec.h>
#include <grpcxx/codec/fory_codec.h>
#include <grpcxx/codec/protobuf_codec.h>

#include <cstdint>
#include <string_view>

#if defined(GRPCXX_ENABLE_PROTOBUF)
#include "proto/order_book.grpcxx.h"
#endif
#if defined(GRPCXX_ENABLE_FORY)
#include "fory/order_book.grpcxx.h"
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
#include "capnp/order_book.grpcxx.h"
#endif

namespace examples::order_book {

inline constexpr std::string_view kSymbol = "BTCUSDT";
inline constexpr std::int64_t kPrice = 10000;

/** Adapt generated Protobuf and Fory value messages to the same order-book operations. */
template <typename Service, typename Engine, typename QuoteIn, typename QuoteOut, typename OrderIn,
          typename OrderOut>
struct ValueTraits {
  using Contract = Service;
  using Codec = Engine;
  using QuoteRequest = QuoteIn;
  using QuoteResponse = QuoteOut;
  using OrderRequest = OrderIn;
  using OrderResponse = OrderOut;

  static void Configure(Codec& codec) noexcept {
    if constexpr (requires { codec.Engine(); }) {
      codec.Engine().template register_struct<QuoteRequest>(1);
      codec.Engine().template register_struct<QuoteResponse>(2);
      codec.Engine().template register_struct<OrderRequest>(3);
      codec.Engine().template register_struct<OrderResponse>(4);
    }
  }
  static auto MakeQuote(std::int64_t id) noexcept -> QuoteRequest {
    QuoteRequest request;
    request.set_id(id);
    request.set_symbol(kSymbol.data());
    return request;
  }
  static auto MakeOrder(std::int64_t id, std::int64_t quantity) noexcept -> OrderRequest {
    OrderRequest request;
    request.set_id(id);
    request.set_symbol(kSymbol.data());
    request.set_quantity(quantity);
    return request;
  }
  static auto QuoteId(const QuoteRequest& request) noexcept -> std::int64_t { return request.id(); }
  static auto OrderId(const OrderRequest& request) noexcept -> std::int64_t { return request.id(); }
  static auto Quantity(const OrderRequest& request) noexcept -> std::int64_t {
    return request.quantity();
  }
  static bool ValidQuote(const QuoteRequest& request) noexcept {
    return request.id() > 0 && request.symbol() == kSymbol;
  }
  static bool ValidOrder(const OrderRequest& request) noexcept {
    return request.id() > 0 && request.symbol() == kSymbol && request.quantity() > 0;
  }
  static auto QuoteReply(std::int64_t id, std::int64_t server) noexcept -> QuoteResponse {
    QuoteResponse response;
    response.set_id(id);
    response.set_price(kPrice + server);
    response.set_server(server);
    return response;
  }
  static auto OrderReply(std::int64_t id, std::int64_t quantity,
                         std::int64_t server) noexcept -> OrderResponse {
    OrderResponse response;
    response.set_id(id);
    response.set_quantity(quantity);
    response.set_server(server);
    return response;
  }
  static bool MatchesQuote(const QuoteResponse& response, std::int64_t id,
                           std::int64_t server) noexcept {
    return response.id() == id && response.price() == kPrice + server &&
           response.server() == server;
  }
  static bool MatchesOrder(const OrderResponse& response, std::int64_t id, std::int64_t quantity,
                           std::int64_t server) noexcept {
    return response.id() == id && response.quantity() == quantity && response.server() == server;
  }
};

#if defined(GRPCXX_ENABLE_PROTOBUF)
using ProtobufTraits =
    ValueTraits<proto::OrderBookContract, grpcxx::ProtobufCodec, proto::QuoteRequest,
                proto::QuoteResponse, proto::OrderRequest, proto::OrderResponse>;
#endif
#if defined(GRPCXX_ENABLE_FORY)
using ForyTraits = ValueTraits<fory::OrderBookContract, grpcxx::ForyCodec, fory::QuoteRequest,
                               fory::QuoteResponse, fory::OrderRequest, fory::OrderResponse>;
#endif

#if defined(GRPCXX_ENABLE_CAPNP)
/** Cap'n Proto RPC parameters/results own their arenas across deferred completion. */
struct CapnpTraits {
  using Contract = capnp::OrderBookContract;
  using Codec = grpcxx::CapnpCodec;
  using QuoteSchema = capnp::OrderBook::QuoteParams;
  using QuoteRequest = grpcxx::CapnpMessage<QuoteSchema>;
  using QuoteResponse = grpcxx::CapnpMessage<capnp::OrderBook::QuoteResults>;
  using OrderRequest = grpcxx::CapnpMessage<capnp::OrderBook::PlaceOrderParams>;
  using OrderResponse = grpcxx::CapnpMessage<capnp::OrderBook::PlaceOrderResults>;
  static void Configure(Codec&) noexcept {}
  static auto MakeQuote(std::int64_t id) noexcept -> QuoteRequest {
    QuoteRequest request;
    auto root = request.GetBuilder().initRequest();
    root.setId(id);
    root.setSymbol(kSymbol.data());
    return request;
  }
  static auto MakeOrder(std::int64_t id, std::int64_t quantity) noexcept -> OrderRequest {
    OrderRequest request;
    auto root = request.GetBuilder().initRequest();
    root.setId(id);
    root.setSymbol(kSymbol.data());
    root.setQuantity(quantity);
    return request;
  }
  static auto QuoteId(const QuoteRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getId();
  }
  static auto OrderId(const OrderRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getId();
  }
  static auto Quantity(const OrderRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getQuantity();
  }
  static bool ValidQuote(const QuoteRequest& request) noexcept {
    const auto root = request.GetReader().getRequest();
    return root.getId() > 0 && root.getSymbol() == kSymbol.data();
  }
  static bool ValidOrder(const OrderRequest& request) noexcept {
    const auto root = request.GetReader().getRequest();
    return root.getId() > 0 && root.getSymbol() == kSymbol.data() && root.getQuantity() > 0;
  }
  static auto QuoteReply(std::int64_t id, std::int64_t server) noexcept -> QuoteResponse {
    QuoteResponse response;
    auto root = response.GetBuilder().initResponse();
    root.setId(id);
    root.setPrice(kPrice + server);
    root.setServer(server);
    return response;
  }
  static auto OrderReply(std::int64_t id, std::int64_t quantity,
                         std::int64_t server) noexcept -> OrderResponse {
    OrderResponse response;
    auto root = response.GetBuilder().initResponse();
    root.setId(id);
    root.setQuantity(quantity);
    root.setServer(server);
    return response;
  }
  static bool MatchesQuote(const QuoteResponse& response, std::int64_t id,
                           std::int64_t server) noexcept {
    const auto root = response.GetReader().getResponse();
    return root.getId() == id && root.getPrice() == kPrice + server && root.getServer() == server;
  }
  static bool MatchesOrder(const OrderResponse& response, std::int64_t id, std::int64_t quantity,
                           std::int64_t server) noexcept {
    const auto root = response.GetReader().getResponse();
    return root.getId() == id && root.getQuantity() == quantity && root.getServer() == server;
  }
};
#endif

}  // namespace examples::order_book
