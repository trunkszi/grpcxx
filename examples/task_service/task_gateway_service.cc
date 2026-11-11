#include "task_gateway_service.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <ranges>

#include "grpcxx/core/timeout.h"
#include "task_transport.h"

namespace task_gateway {
using grpcxx::Status;
using grpcxx::StatusCode;

TaskLoop::TaskLoop(LoopConfig config) noexcept
    : config_(config), frames_(config.max_receive_message_size) {}
TaskLoop::~TaskLoop() = default;

std::expected<void, Status> TaskLoop::CheckAdmission() noexcept {
  if (!Accepting()) {
    ++rejected_transitions_;
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "gateway is not accepting work"});
  }
  return {};
}
std::expected<void, Status> TaskLoop::Start() noexcept {
  if (!Stopped()) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (config_.stream_window == 0 || config_.stream_window > 16 || config_.validation_steps == 0 ||
      config_.max_receive_message_size < sizeof(task_gateway::SubmitTask) ||
      config_.max_receive_message_size > grpcxx::UnaryResponder<TaskReport>::kMaxPayloadBytes)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto transport = std::make_unique<LoopbackTransport>(*this);
  if (auto started = transport->Start(); !started) return started;
  transport_ = std::move(transport);
  frames_.Reset();
  now_ = 0;
  report_count_ = event_count_ = cancel_count_ = failure_count_ = 0;
  rejected_transitions_ = 0;
  high_water_seen_ = write_ready_seen_ = resource_exhausted_seen_ = shutdown_drained_responder_ =
      false;
  state_ = grpcxx::ServiceState::kRunning;
  return {};
}
std::expected<grpcxx::PollResult, Status> TaskLoop::PollOnce(
    grpcxx::PollBudget budget, grpcxx::MonotonicTimestamp now) noexcept {
  if (!budget.Valid() || now < now_) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (Stopped() || !transport_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  now_ = now;
  auto result = transport_->PollOnce(budget, now);
  if (!result) return result;
  for (auto& pending : pending_) {
    if (!pending.responder) continue;
    result->pending = true;
    if (!result->deadline || pending.deadline < *result->deadline)
      result->deadline = pending.deadline;
    if (result->timers >= budget.timers || result->commands >= budget.commands) continue;
    ++result->timers;
    if (pending.polls_left > 0) --pending.polls_left;
    if (now >= pending.deadline || pending.polls_left == 0) {
      if (auto finished = FinishTask(pending); !finished) return std::unexpected(finished.error());
      ++result->commands;
    }
  }
  result->pending = result->pending || !transport_->Idle();
  return result;
}
std::expected<void, Status> TaskLoop::BeginShutdown(grpcxx::MonotonicTimestamp now) noexcept {
  if (!Accepting() || now < now_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto stopping = transport_->BeginShutdown(now); !stopping) return stopping;
  now_ = now;
  state_ = grpcxx::ServiceState::kStopping;
  return {};
}
std::expected<bool, Status> TaskLoop::PollShutdown(grpcxx::PollBudget budget,
                                                   grpcxx::MonotonicTimestamp now) noexcept {
  if (state_ != grpcxx::ServiceState::kStopping)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto result = PollOnce(budget, now); !result) return std::unexpected(result.error());
  return PendingCount() == 0 && transport_->Idle();
}
std::expected<void, Status> TaskLoop::Shutdown() noexcept {
  if (state_ != grpcxx::ServiceState::kStopping || PendingCount() != 0 || !transport_->Idle())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto stopped = transport_->Shutdown(); !stopped) return stopped;
  transport_.reset();
  state_ = grpcxx::ServiceState::kStopped;
  return {};
}
std::expected<void, Status> TaskLoop::SubmitTask(const task_gateway::SubmitTask& task,
                                                 grpcxx::MonotonicTimestamp deadline) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  if (task.weight <= 0 || task.sequence <= 0)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  return transport_->Submit(task, deadline);
}
std::expected<void, Status> TaskLoop::SubmitWithTimeout(const task_gateway::SubmitTask& task,
                                                        std::string_view timeout) noexcept {
  auto duration = grpcxx::GrpcTimeout::Parse(timeout);
  if (!duration) return std::unexpected(duration.error());
  if (*duration > std::numeric_limits<std::uint64_t>::max() - now_)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  return SubmitTask(task, now_ + *duration);
}
void TaskLoop::SubmitTask(grpcxx::ServerContext& context, const task_gateway::SubmitTask& task,
                          grpcxx::UnaryResponder<TaskReport> responder) noexcept {
  auto duplicate = std::ranges::any_of(pending_, [&](const auto& pending) {
    return pending.responder && pending.task.task_id == task.task_id;
  });
  auto free =
      std::ranges::find_if(pending_, [](const auto& pending) { return !pending.responder; });
  if (duplicate || free == pending_.end()) {
    const auto failed = responder.Fail(
        Status{duplicate ? StatusCode::kAlreadyExists : StatusCode::kResourceExhausted});
    static_cast<void>(failed);
    return;
  }
  free->task = task;
  free->deadline = context.deadline.value_or(now_);
  free->polls_left = config_.validation_steps;
  free->responder.emplace(std::move(responder));
}
std::expected<grpcxx::Response<CancelAck>, Status> TaskLoop::CancelTask(
    const task_gateway::CancelTask& request) noexcept {
  if (auto ready = CheckAdmission(); !ready) return std::unexpected(ready.error());
  return transport_->Cancel(request);
}
std::expected<grpcxx::Response<CancelAck>, Status> TaskLoop::CancelTask(
    grpcxx::ServerContext&, const task_gateway::CancelTask& request) noexcept {
  if (cancel_count_ == cancels_.size())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto found = std::ranges::find_if(pending_, [&](const auto& pending) {
    return pending.responder && pending.task.task_id == request.task_id;
  });
  if (found == pending_.end()) {
    ++rejected_transitions_;
    return std::unexpected(Status{StatusCode::kNotFound});
  }
  if (auto failed = found->responder->Fail(Status{StatusCode::kCancelled}); !failed)
    return std::unexpected(failed.error());
  found->responder.reset();
  cancels_[cancel_count_++] = CancelAck{request.task_id, true};
  return grpcxx::Response<CancelAck>{.message = cancels_[cancel_count_ - 1]};
}
std::expected<void, Status> TaskLoop::SubscribeRecords(std::uint32_t count) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->Subscribe(count);
}
std::expected<void, Status> TaskLoop::OpenTaskStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->OpenTasks();
}
std::expected<grpcxx::WriteStatus, Status> TaskLoop::SendTask(
    const TaskStreamRequest& request) noexcept {
  if (auto ready = CheckAdmission(); !ready) return std::unexpected(ready.error());
  if (request.weight <= 0) return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto sent = transport_->Send(request);
  if (sent && *sent == grpcxx::WriteStatus::kHighWaterMarkReached) high_water_seen_ = true;
  return sent;
}
std::expected<void, Status> TaskLoop::HalfCloseTaskStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->HalfClose();
}
std::expected<void, Status> TaskLoop::CancelTaskStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->CancelStream();
}
std::expected<void, Status> TaskLoop::DrainStreamWindow() noexcept {
  if (Stopped()) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return transport_->Drain();
}
std::expected<void, Status> TaskLoop::PushRawFrame(std::span<const std::byte> wire) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  auto parsed = frames_.Feed(wire);
  if (!parsed) {
    resource_exhausted_seen_ = parsed.error().Code() == StatusCode::kResourceExhausted;
    return std::unexpected(parsed.error());
  }
  for (const auto& frame : *parsed) {
    if (frame.Compressed()) return std::unexpected(Status{StatusCode::kUnimplemented});
    auto task = LoopbackCodec::Decode<task_gateway::SubmitTask>(frame.Payload());
    if (!task) return std::unexpected(task.error());
    if (auto accepted =
            SubmitTask(*task, now_ > std::numeric_limits<std::uint64_t>::max() - 1'000'000'000
                                  ? std::numeric_limits<std::uint64_t>::max()
                                  : now_ + 1'000'000'000);
        !accepted)
      return accepted;
  }
  return {};
}
std::expected<void, Status> TaskLoop::FinishTask(PendingTask& pending) noexcept {
  if (now_ >= pending.deadline) {
    if (auto failed = pending.responder->Fail(Status{StatusCode::kDeadlineExceeded}); !failed)
      return failed;
  } else {
    if (auto completed = pending.responder->Complete(
            grpcxx::Response<TaskReport>{.message = MakeReport(pending.task)});
        !completed)
      return completed;
    if (state_ == grpcxx::ServiceState::kStopping) shutdown_drained_responder_ = true;
  }
  pending.responder.reset();
  return {};
}
void TaskLoop::RecordFailure(StatusCode code) noexcept {
  if (failure_count_ < failures_.size()) failures_[failure_count_++] = code;
}
bool TaskLoop::DeadlineExceededSeen() const noexcept {
  return std::ranges::find(Failures(), StatusCode::kDeadlineExceeded) != Failures().end();
}
std::size_t TaskLoop::PendingCount() const noexcept {
  return static_cast<std::size_t>(std::ranges::count_if(
      pending_, [](const auto& pending) { return pending.responder.has_value(); }));
}
TaskReport TaskLoop::MakeReport(const task_gateway::SubmitTask& task) noexcept {
  TaskReport report{};
  report.task_id = task.task_id;
  report.weight = task.weight;
  report.sequence = task.sequence;
  std::ranges::copy(std::string_view{"new"}, report.status.begin());
  return report;
}
}  // namespace task_gateway
