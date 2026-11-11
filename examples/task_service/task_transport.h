#pragma once

#include "grpcxx/reflection/client_stub.h"
#include "grpcxx/streaming/client_stream_writer.h"
#include "grpcxx/streaming/server_stream_responder.h"
#include "task_gateway_service.h"

namespace task_gateway {
/** In-memory byte transport; canonical sessions own HTTP/2 and canonical muxes own stream FSMs. */
class LoopbackTransport final {
 public:
  explicit LoopbackTransport(TaskLoop& loop) noexcept;
  ~LoopbackTransport();
  [[nodiscard]] std::expected<void, grpcxx::Status> Start() noexcept;
  [[nodiscard]] std::expected<grpcxx::PollResult, grpcxx::Status> PollOnce(
      grpcxx::PollBudget, grpcxx::MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> BeginShutdown(
      grpcxx::MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<bool, grpcxx::Status> PollShutdown(
      grpcxx::PollBudget, grpcxx::MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> Shutdown() noexcept;
  [[nodiscard]] grpcxx::ServiceState State() const noexcept { return state_; }
  [[nodiscard]] bool Stopped() const noexcept { return state_ == grpcxx::ServiceState::kStopped; }
  [[nodiscard]] bool Idle() const noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> Submit(const task_gateway::SubmitTask&,
                                                           grpcxx::MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<grpcxx::Response<CancelAck>, grpcxx::Status> Cancel(
      const task_gateway::CancelTask&) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> Subscribe(std::uint32_t) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> OpenTasks() noexcept;
  [[nodiscard]] std::expected<grpcxx::WriteStatus, grpcxx::Status> Send(
      const TaskStreamRequest&) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> HalfClose() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> CancelStream() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> Drain() noexcept;

 private:
  struct Impl;
  TaskLoop& loop_;
  grpcxx::ServiceState state_{grpcxx::ServiceState::kStopped};
  std::unique_ptr<Impl> impl_;
};
static_assert(grpcxx::ServiceConcept<LoopbackTransport>);
}  // namespace task_gateway
