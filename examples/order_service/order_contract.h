#pragma once

#include "grpcxx/core/service.h"
#include "grpcxx/core/status.h"
#include "grpcxx/framing/length_prefixed_frame.h"
#include "grpcxx/reflection/annotations.h"
#include "grpcxx/streaming/write_status.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <meta>
#include <ranges>
#include <span>
#include <string_view>
#include <type_traits>

namespace order_gateway {

/** New-order request. Prices and quantities are integer ticks. */
struct NewOrderSingle {
  std::uint64_t client_order_id{0};
  std::int64_t price_ticks{0};
  std::int64_t quantity{0};
  char symbol[16]{};
};

/** Execution report produced after the deferred risk tick. */
struct ExecutionReport {
  std::uint64_t client_order_id{0};
  std::int64_t price_ticks{0};
  std::int64_t quantity{0};
  char status[16]{};
};

/** Cancel request for one client order id. */
struct CancelOrder {
  std::uint64_t client_order_id{0};
};

/** Immediate cancel acknowledgement. */
struct CancelAck {
  std::uint64_t client_order_id{0};
  bool canceled{false};
};

/** One L1 book tick on the market-data stream. */
struct MarketDataTick {
  std::uint64_t sequence{0};
  std::int64_t bid_ticks{0};
  std::int64_t ask_ticks{0};
};

/** Client message on the duplex order-entry stream. */
struct OrderEntryRequest {
  std::uint64_t client_order_id{0};
  std::int64_t quantity{0};
};

/** Reflected gateway methods; unary routes use the canonical deferred responder. */
struct[[= grpcxx::service{.name = "order_gateway.v1.OrderGateway"}]] OrderGatewayContract {
  [[= grpcxx::rpc{
      .name = "NewOrderSingle"}]] ExecutionReport NewOrderSingle(order_gateway::NewOrderSingle);
  [[= grpcxx::rpc{.name = "CancelOrder"}]] CancelAck CancelOrder(order_gateway::CancelOrder);
  [[= grpcxx::rpc{.name = "SubscribeMarketData",
                  .kind = grpcxx::RpcKind::kServerStreaming}]] MarketDataTick
      SubscribeMarketData(std::uint32_t);
  [[= grpcxx::rpc{.name = "StreamOrderEntry",
                  .kind = grpcxx::RpcKind::kBidiStreaming}]] ExecutionReport
      StreamOrderEntry(OrderEntryRequest);
};

/** Local loopback codec for trivially-copyable example messages, not a portable wire ABI. */
struct LoopbackCodec {
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T& value,
                                                           std::span<std::byte> output) noexcept {
    static_assert(std::is_trivially_copyable_v<T>);
    if (output.size() < sizeof(T))
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kResourceExhausted});
    std::memcpy(output.data(), &value, sizeof(T));
    return sizeof(T);
  }
  template <typename T>
  static std::expected<T, grpcxx::Status> Decode(std::span<const std::byte> input) noexcept {
    static_assert(std::is_trivially_copyable_v<T>);
    if (input.size() != sizeof(T))
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
    T value{};
    std::memcpy(&value, input.data(), sizeof(T));
    return value;
  }
};

static_assert(grpcxx::ServiceContract<OrderGatewayContract>);
static_assert(
    std::tuple_size_v<decltype(grpcxx::reflection_detail::Methods<OrderGatewayContract>())> == 4);
}  // namespace order_gateway
