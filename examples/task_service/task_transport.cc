#include "task_transport.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <numeric>
#include <ranges>

#include "grpcxx/core/timeout.h"
#include "grpcxx/h2/h2_server_session.h"
#include "grpcxx/streaming/client_stream_observer.h"

namespace task_gateway {
using grpcxx::Status;
using grpcxx::StatusCode;
namespace {
struct GatewayHandlers {
  TaskLoop& loop;
  void SubmitTask(grpcxx::ServerContext& context, const task_gateway::SubmitTask& request,
                  grpcxx::UnaryResponder<TaskReport> responder) noexcept {
    loop.SubmitTask(context, request, std::move(responder));
  }
  std::expected<grpcxx::Response<CancelAck>, Status> CancelTask(
      grpcxx::ServerContext& context, const task_gateway::CancelTask& request) noexcept {
    return loop.CancelTask(context, request);
  }
};
grpcxx::StreamMultiplexer::Options StreamOptions(LoopConfig config, bool server) noexcept {
  const auto window =
      config.stream_window *
      (server ? std::lcm(sizeof(TaskReport), sizeof(RecordEvent)) : sizeof(TaskStreamRequest));
  return {.max_streams = 2,
          .queue_depth = 64,
          .max_message_bytes = config.max_receive_message_size,
          .high_water_bytes = window,
          .low_water_bytes = std::max<std::size_t>(1, window / 2),
          .initial_window = window};
}
}  // namespace
struct LoopbackTransport::Impl {
  using Router = grpcxx::ServiceRouter<TaskGatewayContract, GatewayHandlers, LoopbackCodec>;
  using Stub = grpcxx::ClientStub<TaskGatewayContract, LoopbackCodec>;
  struct Call {
    grpcxx::ClientContext context{};
    std::optional<std::expected<grpcxx::Response<TaskReport>, Status>> result{};
    std::int32_t stream{};
    grpcxx::MonotonicTimestamp submitted_at{};
    bool used{};
  };
  struct Request {
    std::array<char, 128> path{};
    std::size_t path_size{};
    std::array<std::byte, sizeof(task_gateway::SubmitTask)> payload{};
    std::size_t payload_size{};
    std::int32_t stream{};
    std::uint64_t generation{};
    bool used{};
  };
  explicit Impl(TaskLoop& owner) noexcept
      : loop(owner),
        handlers{owner},
        router(handlers),
        stub(client),
        outgoing(StreamOptions(owner.Config(), false)),
        incoming(StreamOptions(owner.Config(), true)) {
    server.SetRequestHandler(
        this, [](void* opaque, auto stream, auto generation, std::string_view path,
                 std::span<const std::byte> bytes) noexcept {
          static_cast<Impl*>(opaque)->QueueRequest(stream, generation, path, bytes);
        });
    outgoing.SetWireHandler([this](const grpcxx::StreamFrame& frame) noexcept {
      auto fed = incoming.Feed(frame.stream, frame);
      if (fed && frame.kind == grpcxx::StreamFrameKind::kMessage)
        refund_out[frame.stream.index] += frame.payload.size();
      return fed;
    });
    incoming.SetWireHandler([this](const grpcxx::StreamFrame& frame) noexcept {
      auto fed = outgoing.Feed(frame.stream, frame);
      if (fed && frame.kind == grpcxx::StreamFrameKind::kMessage)
        refund_in[frame.stream.index] += frame.payload.size();
      return fed;
    });
  }
  TaskLoop& loop;
  grpcxx::H2ServerSession server;
  grpcxx::H2ClientSession client;
  GatewayHandlers handlers;
  Router router;
  Stub stub;
  grpcxx::StreamMultiplexer outgoing;
  grpcxx::StreamMultiplexer incoming;
  std::array<Call, 8> calls{};
  std::array<Request, 16> requests{};
  std::optional<Status> error{};
  grpcxx::ClientContext cancel_context{};
  std::optional<std::expected<grpcxx::Response<CancelAck>, Status>> cancel_result{};
  bool cancel_active{};
  std::optional<grpcxx::StreamHandle> records{};
  std::optional<grpcxx::StreamHandle> tasks{};
  std::array<std::size_t, 2> refund_out{};
  std::array<std::size_t, 2> refund_in{};
  std::array<TaskStreamRequest, 64> inbound{};
  std::size_t inbound_count{};
  std::size_t tasks_outstanding{};
  std::uint32_t events_remaining{};
  std::uint64_t next_sequence{1};
  bool records_finished{};
  bool tasks_finished{};
  bool tasks_half_closed{};
  bool tasks_remote_closed{};
  bool tasks_cancelled{};
  bool stopping{};
  bool stream_pending{};

  void Save(std::expected<void, Status> result) noexcept {
    if (!result && !error) error = result.error();
  }
  grpcxx::UnaryResponderSink Sink() noexcept {
    return {.context = this,
            .is_current =
                [](void* opaque, std::uint64_t, std::int32_t stream,
                   std::uint64_t generation) noexcept {
                  return static_cast<Impl*>(opaque)->server.IsCurrent(stream, generation);
                },
            .complete =
                [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                   std::span<const std::byte> bytes, grpcxx::MetadataMap&&,
                   grpcxx::MetadataMap&&) noexcept {
                  return static_cast<Impl*>(opaque)->server.Complete(stream, generation, bytes);
                },
            .fail =
                [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                   Status status) noexcept {
                  return static_cast<Impl*>(opaque)->server.Complete(
                      stream, generation, {}, static_cast<int>(status.Code()));
                }};
  }
  void QueueRequest(std::int32_t stream, std::uint64_t generation, std::string_view path,
                    std::span<const std::byte> bytes) noexcept {
    auto free = std::ranges::find_if(requests, [](const auto& request) { return !request.used; });
    if (free == requests.end() || bytes.size() > loop.Config().max_receive_message_size ||
        bytes.size() > sizeof(task_gateway::SubmitTask) || path.size() > 128) {
      Save(server.Complete(stream, generation, {},
                           static_cast<int>(StatusCode::kResourceExhausted)));
      return;
    }
    free->stream = stream;
    free->generation = generation;
    free->path_size = path.size();
    free->payload_size = bytes.size();
    std::ranges::copy(path, free->path.data());
    std::ranges::copy(bytes, free->payload.data());
    free->used = true;
  }
  void Dispatch(grpcxx::PollBudget budget, grpcxx::PollResult& result) noexcept {
    for (auto& request : requests) {
      if (!request.used || result.commands >= budget.commands) continue;
      grpcxx::ServerContext context{};
      for (const auto& call : calls) {
        if (!call.used || call.stream != request.stream) continue;
        /** The loopback adapter bridges metadata because Submit exposes only path and payload. */
        if (auto timeout = call.context.metadata.WireValue("grpc-timeout")) {
          auto parsed = grpcxx::GrpcTimeout::Parse(*timeout);
          if (!parsed) {
            error = parsed.error();
            return;
          }
          Save(context.metadata.Add("grpc-timeout", *timeout));
          const auto maximum = std::numeric_limits<std::uint64_t>::max();
          const auto parsed_deadline =
              *parsed > maximum - call.submitted_at ? maximum : call.submitted_at + *parsed;
          context.deadline =
              std::min(call.context.deadline.value_or(parsed_deadline), parsed_deadline);
        }
      }
      Save(router.Dispatch({request.path.data(), request.path_size}, context,
                           std::span<const std::byte>{request.payload}.first(request.payload_size),
                           Sink(), 1, request.stream, request.generation));
      request.used = false;
      ++result.commands;
    }
  }
  template <typename From, typename To>
  bool Transfer(From& from, To& to, grpcxx::PollResult& result,
                grpcxx::PollBudget budget) noexcept {
    if (result.frames >= budget.frames) return false;
    auto wire = from.Send();
    if (!wire) {
      error = wire.error();
      return false;
    }
    if (wire->empty()) return false;
    auto received = to.Receive(*wire);
    if (!received) {
      error = received.error();
      return false;
    }
    if (*received != wire->size()) {
      error = Status{StatusCode::kInternal};
      return false;
    }
    ++result.frames;
    return true;
  }
  void PumpUnary(grpcxx::PollBudget budget, grpcxx::PollResult& result) noexcept {
    while (!error && result.frames < budget.frames) {
      const bool sent = Transfer(client, server, result, budget);
      Dispatch(budget, result);
      const bool received = Transfer(server, client, result, budget);
      if (!sent && !received) break;
    }
  }
  void Deliver(grpcxx::PollBudget budget, grpcxx::PollResult& result) noexcept {
    for (auto& call : calls) {
      if (!call.result || result.completions >= budget.completions) continue;
      if (*call.result)
        loop.reports_[loop.report_count_++] = (**call.result).message;
      else
        loop.RecordFailure(call.result->error().Code());
      call.result.reset();
      call.context.metadata.Clear();
      call.used = false;
      ++result.completions;
    }
  }
  bool ReportRoom() const noexcept {
    const auto reserved = static_cast<std::size_t>(
        std::ranges::count_if(calls, [](const auto& call) { return call.used; }));
    return loop.report_count_ + reserved + tasks_outstanding < loop.reports_.size() &&
           loop.failure_count_ + reserved < loop.failures_.size();
  }
  std::expected<void, Status> Pair(grpcxx::RpcKind kind,
                                   std::optional<grpcxx::StreamHandle>& target) noexcept {
    auto client_handle = outgoing.Open(kind);
    if (!client_handle) return std::unexpected(client_handle.error());
    auto server_handle = incoming.Open(kind, true);
    if (!server_handle) {
      Save(outgoing.Cancel(*client_handle));
      Save(outgoing.Release(*client_handle));
      return std::unexpected(server_handle.error());
    }
    if (*client_handle != *server_handle) return std::unexpected(Status{StatusCode::kInternal});
    target = *client_handle;
    Save(outgoing.SendHeaders(*target, {}));
    Save(incoming.SendHeaders(*target, {}));
    return error ? std::expected<void, Status>{std::unexpected(*error)}
                 : std::expected<void, Status>{};
  }
  template <typename T>
  static auto Encoder() noexcept {
    return [](const T& message) noexcept -> std::expected<std::span<const std::byte>, Status> {
      return std::as_bytes(std::span{&message, 1});
    };
  }
  auto TaskWriter() noexcept {
    return grpcxx::ClientStreamWriter<TaskStreamRequest>{
        grpcxx::RawClientStreamWriter{outgoing, *tasks}, Encoder<TaskStreamRequest>()};
  }
  void Refund() noexcept {
    for (auto handle : {records, tasks}) {
      if (!handle) continue;
      const auto index = handle->index;
      if (refund_out[index]) {
        Save(outgoing.UpdateWindow(*handle, refund_out[index]));
        refund_out[index] = 0;
      }
      if (refund_in[index]) {
        Save(incoming.UpdateWindow(*handle, refund_in[index]));
        refund_in[index] = 0;
      }
    }
  }
  void Produce(grpcxx::PollBudget budget, grpcxx::PollResult& result) noexcept {
    if (records) {
      grpcxx::ServerStreamResponder<RecordEvent> sender{
          grpcxx::RawServerStreamResponder{incoming, *records}, Encoder<RecordEvent>()};
      while (events_remaining && result.commands < budget.commands) {
        const RecordEvent event{next_sequence, 99 + static_cast<std::int64_t>(next_sequence),
                                100 + static_cast<std::int64_t>(next_sequence)};
        auto sent = sender.Send(event);
        if (!sent) {
          if (sent.error().Code() != StatusCode::kResourceExhausted) error = sent.error();
          break;
        }
        ++result.commands;
        --events_remaining;
        ++next_sequence;
        if (*sent == grpcxx::WriteStatus::kHighWaterMarkReached) {
          loop.high_water_seen_ = true;
          break;
        }
      }
      if (!events_remaining && !records_finished) {
        auto finished = sender.Finish(Status{});
        if (finished)
          records_finished = true;
        else if (finished.error().Code() != StatusCode::kResourceExhausted)
          error = finished.error();
      }
    }
    if (!tasks || tasks_cancelled) return;
    grpcxx::ServerStreamResponder<TaskReport> sender{
        grpcxx::RawServerStreamResponder{incoming, *tasks}, Encoder<TaskReport>()};
    while (inbound_count && result.commands < budget.commands) {
      TaskReport report{};
      report.task_id = inbound[0].task_id;
      report.weight = inbound[0].weight;
      std::ranges::copy(std::string_view{"accepted"}, report.status.begin());
      auto sent = sender.Send(report);
      if (!sent) {
        if (sent.error().Code() != StatusCode::kResourceExhausted) error = sent.error();
        break;
      }
      std::ranges::move(std::span{inbound}.subspan(1, inbound_count - 1), inbound.data());
      --inbound_count;
      ++result.commands;
      if (*sent == grpcxx::WriteStatus::kHighWaterMarkReached) {
        loop.high_water_seen_ = true;
        break;
      }
    }
    if (tasks_remote_closed && !inbound_count && !tasks_finished) {
      auto finished = sender.Finish(Status{});
      if (finished)
        tasks_finished = true;
      else if (finished.error().Code() != StatusCode::kResourceExhausted)
        error = finished.error();
    }
  }
  void PollStreams(grpcxx::PollBudget budget, grpcxx::PollResult& result) noexcept {
    Refund();
    Produce(budget, result);
    for (auto* mux : {&outgoing, &incoming}) {
      auto remaining = budget;
      remaining.frames -= result.frames;
      remaining.commands -= result.commands;
      auto polled = mux->PollOnce(remaining, loop.Now());
      if (!polled) {
        error = polled.error();
        return;
      }
      result.frames += polled->frames;
      result.commands += polled->commands;
      stream_pending = stream_pending || polled->pending;
    }
  }
};

LoopbackTransport::LoopbackTransport(TaskLoop& loop) noexcept : loop_(loop) {}
LoopbackTransport::~LoopbackTransport() = default;
std::expected<void, Status> LoopbackTransport::Start() noexcept {
  if (!Stopped()) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto candidate = std::make_unique<Impl>(loop_);
  if (!candidate || !candidate->server.Ready() || !candidate->client.Ready() ||
      !candidate->outgoing.Ready() || !candidate->incoming.Ready())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  impl_ = std::move(candidate);
  state_ = grpcxx::ServiceState::kRunning;
  return {};
}
std::expected<grpcxx::PollResult, Status> LoopbackTransport::PollOnce(
    grpcxx::PollBudget budget, grpcxx::MonotonicTimestamp) noexcept {
  if (!budget.Valid()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (!impl_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto& self = *impl_;
  grpcxx::PollResult result{};
  self.stream_pending = false;
  self.PumpUnary(budget, result);
  self.Dispatch(budget, result);
  self.Deliver(budget, result);
  self.PollStreams(budget, result);
  if (self.error) return std::unexpected(*self.error);
  result.pending = !Idle();
  return result;
}
std::expected<void, Status> LoopbackTransport::BeginShutdown(grpcxx::MonotonicTimestamp) noexcept {
  if (state_ != grpcxx::ServiceState::kRunning)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (impl_->tasks && !impl_->tasks_half_closed && !impl_->tasks_cancelled) {
    if (auto closed = HalfClose(); !closed) return closed;
  }
  impl_->stopping = true;
  state_ = grpcxx::ServiceState::kStopping;
  return {};
}
std::expected<bool, Status> LoopbackTransport::PollShutdown(
    grpcxx::PollBudget budget, grpcxx::MonotonicTimestamp now) noexcept {
  if (state_ != grpcxx::ServiceState::kStopping)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (auto polled = PollOnce(budget, now); !polled) return std::unexpected(polled.error());
  return Idle();
}
std::expected<void, Status> LoopbackTransport::Shutdown() noexcept {
  if (state_ != grpcxx::ServiceState::kStopping || !Idle())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  impl_.reset();
  state_ = grpcxx::ServiceState::kStopped;
  return {};
}
bool LoopbackTransport::Idle() const noexcept {
  if (!impl_) return true;
  const auto& self = *impl_;
  const auto terminal = [](const auto& mux, grpcxx::StreamHandle handle) {
    auto state = mux.State(handle);
    return state &&
           (*state == grpcxx::StreamState::kClosed || *state == grpcxx::StreamState::kReset);
  };
  if (self.stopping) {
    for (auto handle : {self.records, self.tasks}) {
      if (handle && (!terminal(self.outgoing, *handle) || !terminal(self.incoming, *handle)))
        return false;
    }
  }
  return !self.error && !self.cancel_active && self.client.ActiveStreams() == 0 &&
         self.server.ActiveStreams() == 0 &&
         std::ranges::none_of(self.calls, [](const auto& call) { return call.used; }) &&
         std::ranges::none_of(self.requests, [](const auto& request) { return request.used; }) &&
         self.inbound_count == 0 && self.tasks_outstanding == 0 && self.events_remaining == 0 &&
         !self.stream_pending;
}
std::expected<void, Status> LoopbackTransport::Submit(
    const task_gateway::SubmitTask& task, grpcxx::MonotonicTimestamp deadline) noexcept {
  if (!impl_->ReportRoom()) return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto free = std::ranges::find_if(impl_->calls, [](const auto& call) { return !call.used; });
  if (free == impl_->calls.end()) return std::unexpected(Status{StatusCode::kResourceExhausted});
  std::array<char, 9> text{};
  auto formatted =
      grpcxx::GrpcTimeout::FormatTo(deadline > loop_.Now() ? deadline - loop_.Now() : 0, text);
  if (formatted.empty()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  free->submitted_at = loop_.Now();
  free->context.deadline = deadline;
  free->context.metadata.Clear();
  if (auto added = free->context.metadata.Add("grpc-timeout", formatted); !added) return added;
  free->used = true;
  auto stream = impl_->stub.CallUnary<0>(
      free->context, task,
      [slot = &*free](grpcxx::ClientContext&,
                      std::expected<grpcxx::Response<TaskReport>, Status> result) noexcept {
        slot->result.emplace(std::move(result));
      });
  if (!stream) {
    free->used = false;
    return std::unexpected(stream.error());
  }
  free->stream = *stream;
  return {};
}
std::expected<grpcxx::Response<CancelAck>, Status> LoopbackTransport::Cancel(
    const task_gateway::CancelTask& request) noexcept {
  if (impl_->cancel_active) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  impl_->cancel_active = true;
  impl_->cancel_result.reset();
  auto stream = impl_->stub.CallUnary<1>(
      impl_->cancel_context, request,
      [self = impl_.get()](grpcxx::ClientContext&,
                           std::expected<grpcxx::Response<CancelAck>, Status> result) noexcept {
        self->cancel_result.emplace(std::move(result));
        self->cancel_active = false;
      });
  if (!stream) {
    impl_->cancel_active = false;
    return std::unexpected(stream.error());
  }
  grpcxx::PollResult polled{};
  impl_->PumpUnary(grpcxx::PollBudget{}, polled);
  if (impl_->error) return std::unexpected(*impl_->error);
  if (!impl_->cancel_result) return std::unexpected(Status{StatusCode::kUnavailable});
  return std::move(*impl_->cancel_result);
}
std::expected<void, Status> LoopbackTransport::Subscribe(std::uint32_t count) noexcept {
  if (impl_->records) return std::unexpected(Status{StatusCode::kAlreadyExists});
  if (count > loop_.events_.size() - loop_.event_count_)
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  if (auto opened = impl_->Pair(grpcxx::RpcKind::kServerStreaming, impl_->records); !opened)
    return opened;
  const auto handle = *impl_->records;
  impl_->Save(
      impl_->outgoing.SetOnMessage(handle, [this](std::span<const std::byte> bytes) noexcept {
        auto event = LoopbackCodec::Decode<RecordEvent>(bytes);
        if (event)
          loop_.events_[loop_.event_count_++] = *event;
        else
          impl_->error = event.error();
      }));
  impl_->Save(impl_->incoming.SetOnWriteReady(
      handle, [this]() noexcept { loop_.write_ready_seen_ = true; }));
  impl_->Save(impl_->outgoing.HalfClose(handle));
  impl_->events_remaining = count;
  return impl_->error ? std::expected<void, Status>{std::unexpected(*impl_->error)}
                      : std::expected<void, Status>{};
}
std::expected<void, Status> LoopbackTransport::OpenTasks() noexcept {
  if (impl_->tasks) return std::unexpected(Status{StatusCode::kAlreadyExists});
  if (auto opened = impl_->Pair(grpcxx::RpcKind::kBidiStreaming, impl_->tasks); !opened)
    return opened;
  const auto handle = *impl_->tasks;
  impl_->Save(impl_->outgoing.SetOnWriteReady(
      handle, [this]() noexcept { loop_.write_ready_seen_ = true; }));
  impl_->Save(impl_->incoming.SetOnHalfClose(
      handle, [this]() noexcept { impl_->tasks_remote_closed = true; }));
  impl_->Save(impl_->incoming.SetOnCancel(handle, [this]() noexcept {
    impl_->tasks_cancelled = true;
    impl_->inbound_count = impl_->tasks_outstanding = 0;
  }));
  impl_->Save(
      impl_->incoming.SetOnMessage(handle, [this](std::span<const std::byte> bytes) noexcept {
        auto request = LoopbackCodec::Decode<TaskStreamRequest>(bytes);
        if (!request)
          impl_->error = request.error();
        else if (impl_->inbound_count == impl_->inbound.size())
          impl_->error = Status{StatusCode::kResourceExhausted};
        else
          impl_->inbound[impl_->inbound_count++] = *request;
      }));
  impl_->Save(
      impl_->outgoing.SetOnMessage(handle, [this](std::span<const std::byte> bytes) noexcept {
        auto report = LoopbackCodec::Decode<TaskReport>(bytes);
        if (!report)
          impl_->error = report.error();
        else {
          loop_.reports_[loop_.report_count_++] = *report;
          --impl_->tasks_outstanding;
        }
      }));
  return impl_->error ? std::expected<void, Status>{std::unexpected(*impl_->error)}
                      : std::expected<void, Status>{};
}
std::expected<grpcxx::WriteStatus, Status> LoopbackTransport::Send(
    const TaskStreamRequest& request) noexcept {
  if (!impl_->tasks || impl_->tasks_cancelled || impl_->tasks_half_closed)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (!impl_->ReportRoom()) return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto sent = impl_->TaskWriter().Send(request);
  if (sent) ++impl_->tasks_outstanding;
  return sent;
}
std::expected<void, Status> LoopbackTransport::HalfClose() noexcept {
  if (!impl_->tasks || impl_->tasks_half_closed || impl_->tasks_cancelled)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto closed = impl_->TaskWriter().HalfClose();
  if (closed) impl_->tasks_half_closed = true;
  return closed;
}
std::expected<void, Status> LoopbackTransport::CancelStream() noexcept {
  if (!impl_->tasks || impl_->tasks_cancelled)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto cancelled = impl_->TaskWriter().Cancel();
  if (cancelled) impl_->tasks_cancelled = true;
  return cancelled;
}
std::expected<void, Status> LoopbackTransport::Drain() noexcept {
  for (int round = 0; round < 8; ++round) {
    auto result = PollOnce({}, loop_.Now());
    if (!result) return std::unexpected(result.error());
    if (!result->pending) break;
  }
  return {};
}
}  // namespace task_gateway
