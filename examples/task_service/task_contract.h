#pragma once

#include <array>

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

namespace task_gateway {

/** Submit-task request. Sequence and weight are integer units. */
struct SubmitTask {
  std::uint64_t task_id{0};
  std::int64_t sequence{0};
  std::int64_t weight{0};
  std::array<char, 16> channel_id{};
};

/** Task report produced after the deferred validation step. */
struct TaskReport {
  std::uint64_t task_id{0};
  std::int64_t sequence{0};
  std::int64_t weight{0};
  std::array<char, 16> status{};
};

/** Cancel request for one task id. */
struct CancelTask {
  std::uint64_t task_id{0};
};

/** Immediate cancel acknowledgement. */
struct CancelAck {
  std::uint64_t task_id{0};
  bool canceled{false};
};

/** One record emitted on the server stream. */
struct RecordEvent {
  std::uint64_t sequence{0};
  std::int64_t value{0};
  std::int64_t count{0};
};

/** Client message on the duplex task-entry stream. */
struct TaskStreamRequest {
  std::uint64_t task_id{0};
  std::int64_t weight{0};
};

/** Reflected gateway methods; unary routes use the canonical deferred responder. */
struct[[= grpcxx::service{.name = "task_gateway.v1.TaskGateway"}]] TaskGatewayContract;
struct TaskGatewayContract {
  [[= grpcxx::rpc{.name = "SubmitTask"}]] TaskReport SubmitTask(task_gateway::SubmitTask);
  [[= grpcxx::rpc{.name = "CancelTask"}]] CancelAck CancelTask(task_gateway::CancelTask);
  [[= grpcxx::rpc{
      .name = "SubscribeRecords",
      .kind = grpcxx::RpcKind::kServerStreaming}]] RecordEvent SubscribeRecords(std::uint32_t);
  [[= grpcxx::rpc{
      .name = "StreamTasks",
      .kind = grpcxx::RpcKind::kBidiStreaming}]] TaskReport StreamTasks(TaskStreamRequest);
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

static_assert(grpcxx::ServiceContract<TaskGatewayContract>);
static_assert(
    std::tuple_size_v<decltype(grpcxx::reflection_detail::Methods<TaskGatewayContract>())> == 4);
}  // namespace task_gateway
