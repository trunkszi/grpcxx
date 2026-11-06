#include <fmt/format.h>

#include "order_gateway_service.h"

namespace {
int Fail(std::string_view stage, const grpcxx::Status& status) {
  fmt::print(stderr, "{} failed: {}\n", stage, static_cast<int>(status.Code()));
  return 1;
}
}  // namespace
/** One caller drives risk, HTTP/2, stream backpressure, and graceful shutdown. */
int main() {
  order_gateway::TradingLoop loop;
  if (auto started = loop.Start(); !started) return Fail("Start", started.error());
  if (auto accepted =
          loop.NewOrderWithTimeout(order_gateway::NewOrderSingle{42, 101, 5, {}}, "1000n");
      !accepted)
    return Fail("NewOrderSingle", accepted.error());
  if (auto accepted =
          loop.NewOrderWithTimeout(order_gateway::NewOrderSingle{43, 102, 1, {}}, "1000n");
      !accepted)
    return Fail("cancellable order", accepted.error());
  if (auto cancelled = loop.CancelOrder(order_gateway::CancelOrder{43}); !cancelled)
    return Fail("CancelOrder", cancelled.error());
  if (auto feed = loop.SubscribeMarketData(8); !feed)
    return Fail("SubscribeMarketData", feed.error());
  if (auto opened = loop.OpenOrderStream(); !opened) return Fail("OpenOrderStream", opened.error());
  grpcxx::MonotonicTimestamp now = 1;
  for (std::uint64_t id = 100; id < 108; ++id) {
    auto sent = loop.SendOrderEntry({id, 1});
    if (!sent) return Fail("SendOrderEntry", sent.error());
    if (*sent == grpcxx::WriteStatus::kHighWaterMarkReached) {
      if (auto drained = loop.DrainStreamWindow(); !drained)
        return Fail("DrainStreamWindow", drained.error());
    }
  }
  bool running = true;
  while (running && now < 1000) {
    if (auto tick = loop.PollOnce({}, now++); !tick) return Fail("PollOnce", tick.error());
    running = loop.Reports().size() < 9 || loop.Ticks().size() < 8 || loop.Failures().empty();
  }
  if (running) {
    fmt::print(stderr, "loopback did not complete within its tick bound\n");
    return 1;
  }
  if (auto begun = loop.BeginShutdown(now); !begun) return Fail("BeginShutdown", begun.error());
  bool drained = false;
  while (!drained && now < 2000) {
    auto progress = loop.PollShutdown({}, now++);
    if (!progress) return Fail("PollShutdown", progress.error());
    drained = *progress;
  }
  if (!drained) {
    fmt::print(stderr, "shutdown did not drain\n");
    return 1;
  }
  if (auto stopped = loop.Shutdown(); !stopped) return Fail("Shutdown", stopped.error());
  fmt::print(
      "order gateway loopback reports={} ticks={} cancels={} cancelled={} backpressure={} "
      "ready={}\n",
      loop.Reports().size(), loop.Ticks().size(), loop.Cancels().size(), loop.Failures().size(),
      loop.HighWaterSeen(), loop.WriteReadySeen());
  return 0;
}
