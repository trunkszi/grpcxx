#include "order_gateway_service.h"

#include <algorithm>
#include <limits>
#include <new>
#include <ranges>

#include "grpcxx/core/timeout.h"
#include "order_transport.h"

namespace order_gateway {
using grpcxx::Status;
using grpcxx::StatusCode;

TradingLoop::TradingLoop(LoopConfig config) noexcept
    : config_(config), frames_(config.max_receive_message_size) {}
TradingLoop::~TradingLoop() = default;

std::expected<void, Status> TradingLoop::CheckAdmission() noexcept {
  if (!Accepting()) {
    ++rejected_transitions_;
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "gateway is not accepting work"});
  }
  return {};
}
std::expected<void, Status> TradingLoop::Start() noexcept {
  if (!Stopped()) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (config_.stream_window == 0 || config_.stream_window > 16 || config_.risk_check_ticks == 0 ||
      config_.max_receive_message_size < sizeof(order_gateway::NewOrderSingle) ||
      config_.max_receive_message_size > grpcxx::UnaryResponder<ExecutionReport>::kMaxPayloadBytes)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto transport = std::unique_ptr<LoopbackTransport>{new (std::nothrow) LoopbackTransport(*this)};
  if (!transport) return std::unexpected(Status{StatusCode::kResourceExhausted});
  if (auto started = transport->Start(); !started) return started;
  transport_ = std::move(transport);
  frames_.Reset();
  now_ = 0;
  report_count_ = tick_count_ = cancel_count_ = failure_count_ = 0;
  rejected_transitions_ = 0;
  high_water_seen_ = write_ready_seen_ = resource_exhausted_seen_ = shutdown_drained_responder_ =
      false;
  state_ = grpcxx::ServiceState::kRunning;
  return {};
}
std::expected<grpcxx::PollResult, Status> TradingLoop::PollOnce(
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
      if (auto finished = FinishOrder(pending); !finished) return std::unexpected(finished.error());
      ++result->commands;
    }
  }
  result->pending = result->pending || !transport_->Idle();
  return result;
}
std::expected<void, Status> TradingLoop::BeginShutdown(grpcxx::MonotonicTimestamp now) noexcept {
  if (!Accepting() || now < now_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto stopping = transport_->BeginShutdown(now); !stopping) return stopping;
  now_ = now;
  state_ = grpcxx::ServiceState::kStopping;
  return {};
}
std::expected<bool, Status> TradingLoop::PollShutdown(grpcxx::PollBudget budget,
                                                      grpcxx::MonotonicTimestamp now) noexcept {
  if (state_ != grpcxx::ServiceState::kStopping)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto result = PollOnce(budget, now); !result) return std::unexpected(result.error());
  return PendingCount() == 0 && transport_->Idle();
}
std::expected<void, Status> TradingLoop::Shutdown() noexcept {
  if (state_ != grpcxx::ServiceState::kStopping || PendingCount() != 0 || !transport_->Idle())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto stopped = transport_->Shutdown(); !stopped) return stopped;
  transport_.reset();
  state_ = grpcxx::ServiceState::kStopped;
  return {};
}
std::expected<void, Status> TradingLoop::NewOrderSingle(
    const order_gateway::NewOrderSingle& order, grpcxx::MonotonicTimestamp deadline) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  if (order.quantity <= 0 || order.price_ticks <= 0)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  return transport_->Submit(order, deadline);
}
std::expected<void, Status> TradingLoop::NewOrderWithTimeout(
    const order_gateway::NewOrderSingle& order, std::string_view timeout) noexcept {
  auto duration = grpcxx::GrpcTimeout::Parse(timeout);
  if (!duration) return std::unexpected(duration.error());
  if (*duration > std::numeric_limits<std::uint64_t>::max() - now_)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  return NewOrderSingle(order, now_ + *duration);
}
void TradingLoop::NewOrderSingle(grpcxx::ServerContext& context,
                                 const order_gateway::NewOrderSingle& order,
                                 grpcxx::UnaryResponder<ExecutionReport> responder) noexcept {
  auto duplicate = std::ranges::any_of(pending_, [&](const auto& pending) {
    return pending.responder && pending.order.client_order_id == order.client_order_id;
  });
  auto free =
      std::ranges::find_if(pending_, [](const auto& pending) { return !pending.responder; });
  if (duplicate || free == pending_.end()) {
    const auto failed = responder.Fail(
        Status{duplicate ? StatusCode::kAlreadyExists : StatusCode::kResourceExhausted});
    static_cast<void>(failed);
    return;
  }
  free->order = order;
  free->deadline = context.deadline.value_or(now_);
  free->polls_left = config_.risk_check_ticks;
  free->responder.emplace(std::move(responder));
}
std::expected<grpcxx::Response<CancelAck>, Status> TradingLoop::CancelOrder(
    const order_gateway::CancelOrder& request) noexcept {
  if (auto ready = CheckAdmission(); !ready) return std::unexpected(ready.error());
  return transport_->Cancel(request);
}
std::expected<grpcxx::Response<CancelAck>, Status> TradingLoop::CancelOrder(
    grpcxx::ServerContext&, const order_gateway::CancelOrder& request) noexcept {
  if (cancel_count_ == cancels_.size())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto found = std::ranges::find_if(pending_, [&](const auto& pending) {
    return pending.responder && pending.order.client_order_id == request.client_order_id;
  });
  if (found == pending_.end()) {
    ++rejected_transitions_;
    return std::unexpected(Status{StatusCode::kNotFound});
  }
  if (auto failed = found->responder->Fail(Status{StatusCode::kCancelled}); !failed)
    return std::unexpected(failed.error());
  found->responder.reset();
  cancels_[cancel_count_++] = CancelAck{request.client_order_id, true};
  return grpcxx::Response<CancelAck>{.message = cancels_[cancel_count_ - 1]};
}
std::expected<void, Status> TradingLoop::SubscribeMarketData(std::uint32_t count) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->Subscribe(count);
}
std::expected<void, Status> TradingLoop::OpenOrderStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->OpenOrders();
}
std::expected<grpcxx::WriteStatus, Status> TradingLoop::SendOrderEntry(
    const OrderEntryRequest& request) noexcept {
  if (auto ready = CheckAdmission(); !ready) return std::unexpected(ready.error());
  if (request.quantity <= 0) return std::unexpected(Status{StatusCode::kInvalidArgument});
  auto sent = transport_->Send(request);
  if (sent && *sent == grpcxx::WriteStatus::kHighWaterMarkReached) high_water_seen_ = true;
  return sent;
}
std::expected<void, Status> TradingLoop::HalfCloseOrderStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->HalfClose();
}
std::expected<void, Status> TradingLoop::CancelOrderStream() noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  return transport_->CancelStream();
}
std::expected<void, Status> TradingLoop::DrainStreamWindow() noexcept {
  if (Stopped()) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return transport_->Drain();
}
std::expected<void, Status> TradingLoop::PushRawFrame(std::span<const std::byte> wire) noexcept {
  if (auto ready = CheckAdmission(); !ready) return ready;
  auto parsed = frames_.Feed(wire);
  if (!parsed) {
    resource_exhausted_seen_ = parsed.error().Code() == StatusCode::kResourceExhausted;
    return std::unexpected(parsed.error());
  }
  for (const auto& frame : *parsed) {
    if (frame.Compressed()) return std::unexpected(Status{StatusCode::kUnimplemented});
    auto order = LoopbackCodec::Decode<order_gateway::NewOrderSingle>(frame.Payload());
    if (!order) return std::unexpected(order.error());
    if (auto accepted =
            NewOrderSingle(*order, now_ > std::numeric_limits<std::uint64_t>::max() - 1'000'000'000
                                       ? std::numeric_limits<std::uint64_t>::max()
                                       : now_ + 1'000'000'000);
        !accepted)
      return accepted;
  }
  return {};
}
std::expected<void, Status> TradingLoop::FinishOrder(PendingOrder& pending) noexcept {
  if (now_ >= pending.deadline) {
    if (auto failed = pending.responder->Fail(Status{StatusCode::kDeadlineExceeded}); !failed)
      return failed;
  } else {
    if (auto completed = pending.responder->Complete(
            grpcxx::Response<ExecutionReport>{.message = MakeReport(pending.order)});
        !completed)
      return completed;
    if (state_ == grpcxx::ServiceState::kStopping) shutdown_drained_responder_ = true;
  }
  pending.responder.reset();
  return {};
}
void TradingLoop::RecordFailure(StatusCode code) noexcept {
  if (failure_count_ < failures_.size()) failures_[failure_count_++] = code;
}
bool TradingLoop::DeadlineExceededSeen() const noexcept {
  return std::ranges::find(Failures(), StatusCode::kDeadlineExceeded) != Failures().end();
}
std::size_t TradingLoop::PendingCount() const noexcept {
  return static_cast<std::size_t>(std::ranges::count_if(
      pending_, [](const auto& pending) { return pending.responder.has_value(); }));
}
ExecutionReport TradingLoop::MakeReport(const order_gateway::NewOrderSingle& order) noexcept {
  ExecutionReport report{};
  report.client_order_id = order.client_order_id;
  report.quantity = order.quantity;
  report.price_ticks = order.price_ticks;
  std::ranges::copy(std::string_view{"new"}, report.status);
  return report;
}
}  // namespace order_gateway
