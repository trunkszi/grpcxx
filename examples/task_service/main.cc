#include <fmt/format.h>

#include "task_gateway_service.h"

namespace {
int Fail(std::string_view stage, const grpcxx::Status& status) {
  fmt::print(stderr, "{} failed: {}\n", stage, static_cast<int>(status.Code()));
  return 1;
}
}  // namespace
/** One caller drives validation, HTTP/2, stream backpressure, and graceful shutdown. */
int main() {
  task_gateway::TaskLoop loop;
  if (auto started = loop.Start(); !started) return Fail("Start", started.error());
  if (auto accepted = loop.SubmitWithTimeout(task_gateway::SubmitTask{42, 101, 5, {}}, "1000n");
      !accepted)
    return Fail("SubmitTask", accepted.error());
  if (auto accepted = loop.SubmitWithTimeout(task_gateway::SubmitTask{43, 102, 1, {}}, "1000n");
      !accepted)
    return Fail("cancellable task", accepted.error());
  if (auto cancelled = loop.CancelTask(task_gateway::CancelTask{43}); !cancelled)
    return Fail("CancelTask", cancelled.error());
  if (auto feed = loop.SubscribeRecords(8); !feed) return Fail("SubscribeRecords", feed.error());
  if (auto opened = loop.OpenTaskStream(); !opened) return Fail("OpenTaskStream", opened.error());
  grpcxx::MonotonicTimestamp now = 1;
  for (std::uint64_t id = 100; id < 108; ++id) {
    auto sent = loop.SendTask({id, 1});
    if (!sent) return Fail("SendTask", sent.error());
    if (*sent == grpcxx::WriteStatus::kHighWaterMarkReached) {
      if (auto drained = loop.DrainStreamWindow(); !drained)
        return Fail("DrainStreamWindow", drained.error());
    }
  }
  bool running = true;
  while (running && now < 1000) {
    if (auto tick = loop.PollOnce({}, now++); !tick) return Fail("PollOnce", tick.error());
    running = loop.Reports().size() < 9 || loop.Events().size() < 8 || loop.Failures().empty();
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
      "task gateway loopback reports={} events={} cancels={} cancelled={} backpressure={} "
      "ready={}\n",
      loop.Reports().size(), loop.Events().size(), loop.Cancels().size(), loop.Failures().size(),
      loop.HighWaterSeen(), loop.WriteReadySeen());
  return 0;
}
