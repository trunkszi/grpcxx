#include <benchmark/benchmark.h>
#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <vector>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/reflection/service_router.h"
#include "latency_samples.h"
#include "order_gateway_service.h"

namespace {
using e2e_bench::Samples;

void OrderRoundtrip(benchmark::State& state) {
  Samples samples{state};
  auto loop =
      std::unique_ptr<order_gateway::TradingLoop>{new (std::nothrow) order_gateway::TradingLoop};
  if (!loop || !loop->Start()) {
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
    const order_gateway::NewOrderSingle order{sequence, 101, 5, {}};
    const auto before = loop->Reports().size();
    if (auto accepted = loop->NewOrderSingle(order, now + 10'000); !accepted) {
      state.SkipWithError("NewOrderSingle rejected");
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
        loop->Reports().back().client_order_id != sequence) {
      state.SkipWithError("canonical HTTP/2 deferred response failed");
      break;
    }
    auto report = loop->Reports().back();
    benchmark::DoNotOptimize(report);
    samples.End();
    ++sequence;
  }
  samples.Report(state,
                 sizeof(order_gateway::NewOrderSingle) + sizeof(order_gateway::ExecutionReport));
}

void AlignedFraming(benchmark::State& state) {
  Samples samples{state};
  grpcxx::LengthPrefixedFrame parser{1024};
  const order_gateway::NewOrderSingle order{7, 101, 5, {}};
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto frame = grpcxx::AlignedMessageBuf::Allocate(sizeof(order));
    if (!frame || reinterpret_cast<std::uintptr_t>(frame->Payload().data()) % 8 != 0) {
      state.SkipWithError("aligned buffer allocation failed");
      break;
    }
    std::memcpy(frame->Payload().data(), &order, sizeof(order));
    auto parsed = parser.Feed(frame->WireBytes());
    if (!parsed || parsed->size() != 1) {
      state.SkipWithError("frame parse failed");
      break;
    }
    benchmark::DoNotOptimize(parsed->front().Payload().data());
    samples.End();
  }
  samples.Report(state, sizeof(order) + 5);
}

struct DispatchHandlers {
  void NewOrderSingle(grpcxx::ServerContext&, const order_gateway::NewOrderSingle& request,
                      grpcxx::UnaryResponder<order_gateway::ExecutionReport> responder) noexcept {
    auto response = responder.Complete(grpcxx::Response<order_gateway::ExecutionReport>{
        .message = {request.client_order_id, request.price_ticks, request.quantity, {}}});
    static_cast<void>(response);
  }
  std::expected<grpcxx::Response<order_gateway::CancelAck>, grpcxx::Status> CancelOrder(
      grpcxx::ServerContext&, const order_gateway::CancelOrder& request) noexcept {
    return grpcxx::Response<order_gateway::CancelAck>{.message = {request.client_order_id, true}};
  }
};
void ReflectionDispatch(benchmark::State& state) {
  Samples samples{state};
  DispatchHandlers handler;
  grpcxx::ServiceRouter<order_gateway::OrderGatewayContract, DispatchHandlers,
                        order_gateway::LoopbackCodec>
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
  const order_gateway::CancelOrder request{42};
  auto payload = std::as_bytes(std::span{&request, 1});
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto dispatched = router.Dispatch("/order_gateway.v1.OrderGateway/CancelOrder", context,
                                      payload, sink, 1, 1, 1);
    if (!dispatched) {
      state.SkipWithError("canonical ServiceRouter dispatch failed");
      break;
    }
    benchmark::DoNotOptimize(completions);
    samples.End();
  }
  samples.Report(state, sizeof(request) + sizeof(order_gateway::CancelAck));
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
BENCHMARK(OrderRoundtrip)->Name("order_roundtrip")->Unit(benchmark::kNanosecond);
BENCHMARK(AlignedFraming)->Name("aligned_framing")->Unit(benchmark::kNanosecond);
BENCHMARK(ReflectionDispatch)->Name("reflection_dispatch")->Unit(benchmark::kNanosecond);
BENCHMARK(DelegateInvocation)->Name("delegate_invocation")->Unit(benchmark::kNanosecond);
BENCHMARK_MAIN();
