#include <benchmark/benchmark.h>
#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <ranges>
#include <vector>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/reflection/service_router.h"
#include "latency_samples.h"
#include "task_gateway_service.h"

namespace {
using e2e_bench::Samples;

void RpcRoundtrip(benchmark::State& state) {
  Samples samples{state};
  auto loop = std::make_unique<task_gateway::TaskLoop>();
  if (!loop->Start()) {
    state.SkipWithError("gateway initialization failed");
    return;
  }
  std::uint64_t sequence = 1;
  std::uint64_t now = 1;
  for ([[maybe_unused]] auto iteration : state) {
    if (loop->Reports().size() >= 32) {
      state.PauseTiming();
      auto stopped = loop->BeginShutdown(now);
      auto drained = stopped
                         ? loop->PollShutdown({}, now)
                         : std::expected<bool, grpcxx::Status>{std::unexpected(stopped.error())};
      auto released = drained && *drained ? loop->Shutdown()
                                          : std::expected<void, grpcxx::Status>{std::unexpected(
                                                grpcxx::Status{grpcxx::StatusCode::kInternal})};
      auto restarted = released ? loop->Start() : released;
      now = 1;
      state.ResumeTiming();
      if (!restarted) {
        state.SkipWithError("gateway restart failed");
        break;
      }
    }
    samples.Begin();
    const task_gateway::SubmitTask task{sequence, 101, 5, {}};
    const auto before = loop->Reports().size();
    if (auto accepted = loop->SubmitTask(task, now + 10'000); !accepted) {
      state.SkipWithError("SubmitTask rejected");
      break;
    }
    bool failed = false;
    for (int step = 0; step < 16 && loop->Reports().size() == before; ++step) {
      if (!loop->PollOnce({}, now++)) {
        failed = true;
        break;
      }
    }
    if (failed || loop->Reports().size() != before + 1 ||
        loop->Reports().back().task_id != sequence) {
      state.SkipWithError("canonical HTTP/2 deferred response failed");
      break;
    }
    auto report = loop->Reports().back();
    benchmark::DoNotOptimize(report);
    samples.End();
    ++sequence;
  }
  samples.Report(state, sizeof(task_gateway::SubmitTask) + sizeof(task_gateway::TaskReport));
}

void AlignedFraming(benchmark::State& state) {
  Samples samples{state};
  grpcxx::LengthPrefixedFrame parser{1024};
  const task_gateway::SubmitTask task{7, 101, 5, {}};
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto frame = grpcxx::AlignedMessageBuf::Allocate(sizeof(task));
    if (!frame || frame->PayloadAddress() % 8 != 0) {
      state.SkipWithError("aligned buffer allocation failed");
      break;
    }
    std::memcpy(frame->Payload().data(), &task, sizeof(task));
    auto parsed = parser.Feed(frame->WireBytes());
    if (!parsed || parsed->size() != 1) {
      state.SkipWithError("frame parse failed");
      break;
    }
    benchmark::DoNotOptimize(parsed->front().Payload().data());
    samples.End();
  }
  samples.Report(state, sizeof(task) + 5);
}

struct DispatchHandlers {
  void SubmitTask(grpcxx::ServerContext&, const task_gateway::SubmitTask& request,
                  grpcxx::UnaryResponder<task_gateway::TaskReport> responder) noexcept {
    auto response = responder.Complete(grpcxx::Response<task_gateway::TaskReport>{
        .message = {request.task_id, request.sequence, request.weight, {}}});
    static_cast<void>(response);
  }
  std::expected<grpcxx::Response<task_gateway::CancelAck>, grpcxx::Status> CancelTask(
      grpcxx::ServerContext&, const task_gateway::CancelTask& request) noexcept {
    return grpcxx::Response<task_gateway::CancelAck>{.message = {request.task_id, true}};
  }
};
void ReflectionDispatch(benchmark::State& state) {
  Samples samples{state};
  DispatchHandlers handler;
  grpcxx::ServiceRouter<task_gateway::TaskGatewayContract, DispatchHandlers,
                        task_gateway::LoopbackCodec>
      router{handler};
  static_assert(decltype(router)::RouteCount() == 4);
  std::uint64_t completions{};
  grpcxx::UnaryResponderSink sink{
      .context = &completions,
      .is_current = [](void*, std::uint64_t, std::int32_t, std::uint64_t) noexcept { return true; },
      .complete = [](void* context, std::uint64_t, std::int32_t, std::uint64_t,
                     std::span<const std::byte>, grpcxx::MetadataMap&&,
                     grpcxx::MetadataMap&&) noexcept -> std::expected<void, grpcxx::Status> {
        ++*static_cast<std::uint64_t*>(context);
        return {};
      },
      .fail = [](void*, std::uint64_t, std::int32_t, std::uint64_t, grpcxx::Status status) noexcept
      -> std::expected<void, grpcxx::Status> { return std::unexpected(std::move(status)); }};
  grpcxx::ServerContext context{};
  const task_gateway::CancelTask request{42};
  auto payload = std::as_bytes(std::span{&request, 1});
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto dispatched =
        router.Dispatch("/task_gateway.v1.TaskGateway/CancelTask", context, payload, sink, 1, 1, 1);
    if (!dispatched) {
      state.SkipWithError("canonical ServiceRouter dispatch failed");
      break;
    }
    benchmark::DoNotOptimize(completions);
    samples.End();
  }
  samples.Report(state, sizeof(request) + sizeof(task_gateway::CancelAck));
}
void DelegateInvocation(benchmark::State& state) {
  Samples samples{state};
  grpcxx::InplaceDelegate<std::uint64_t(std::uint64_t) noexcept> delegate{
      [](std::uint64_t value) noexcept { return value + 1; }};
  std::uint64_t value{};
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    value = delegate(value);
    benchmark::DoNotOptimize(value);
    samples.End();
  }
  samples.Report(state, sizeof(value));
}
}  // namespace
BENCHMARK(RpcRoundtrip)->Name("rpc_roundtrip")->Unit(benchmark::kNanosecond);
BENCHMARK(AlignedFraming)->Name("aligned_framing")->Unit(benchmark::kNanosecond);
BENCHMARK(ReflectionDispatch)->Name("reflection_dispatch")->Unit(benchmark::kNanosecond);
BENCHMARK(DelegateInvocation)->Name("delegate_invocation")->Unit(benchmark::kNanosecond);
BENCHMARK_MAIN();
