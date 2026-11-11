#include <gtest/gtest.h>
#include <poll.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "grpcxx/sync/synchronous_channel.h"
#include "grpcxx/sync/synchronous_server.h"

namespace grpcxx {
namespace {

std::span<const std::byte> BytesOf(std::string_view text) noexcept {
  return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

std::string StringOf(std::span<const std::byte> bytes) {
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

class ScopedStopThread final {
 public:
  template <typename Fn>
  explicit ScopedStopThread(Fn&& fn) {
    worker_ = std::thread([this, f = std::forward<Fn>(fn)]() mutable { f(stop_); });
  }

  ScopedStopThread(const ScopedStopThread&) =
      delete("ScopedStopThread owns a single background thread");
  ScopedStopThread& operator=(const ScopedStopThread&) =
      delete("ScopedStopThread owns a single background thread");
  ScopedStopThread(ScopedStopThread&&) =
      delete("ScopedStopThread captures this in its worker thread");
  ScopedStopThread& operator=(ScopedStopThread&&) =
      delete("ScopedStopThread captures this in its worker thread");

  ~ScopedStopThread() { StopAndJoin(); }

  void StopAndJoin() noexcept {
    stop_.store(true, std::memory_order_relaxed);
    if (worker_.joinable()) worker_.join();
  }

 private:
  std::atomic<bool> stop_{false};
  std::thread worker_{};
};

void ServeUntilStopped(SynchronousServer& srv, const std::atomic<bool>& stop) noexcept {
  while (!stop.load(std::memory_order_relaxed) && srv.IsOpen()) {
    if (!srv.PollOnce().has_value()) break;
    const int fd = srv.NativeHandle();
    if (fd >= 0) {
      ::pollfd pfd{.fd = fd, .events = POLLIN, .revents = 0};
      static_cast<void>(::poll(&pfd, 1, 2));
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  }
}

void DelayedEcho(SynchronousServer& srv, const ServerRequest& req, std::chrono::milliseconds delay,
                 std::vector<std::byte>& body) noexcept {
  if (req.event == ServerRequest::Event::kMessage) {
    body.assign(req.payload.begin(), req.payload.end());
    return;
  }
  if (req.event != ServerRequest::Event::kHalfClose) return;
  std::this_thread::sleep_for(delay);
  static_cast<void>(srv.Send(req.stream_id, body));
  static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
}

TEST(SyncStallTest, SlowServerHonorsDeadline) {
  auto listener = nexora::TcpListener::BindLoopbackV4(0);
  ASSERT_TRUE(listener.has_value());
  const auto port = listener->LocalPort();
  ScopedStopThread server([&listener](const std::atomic<bool>& stop) {
    std::vector<std::byte> body{};
    auto srv = SynchronousServer::AcceptTcp(
        *listener, std::chrono::milliseconds{3000},
        [&body](SynchronousServer& srv, const ServerRequest& req) noexcept {
          DelayedEcho(srv, req, std::chrono::milliseconds{200}, body);
        });
    if (!srv.has_value()) return;
    ServeUntilStopped(**srv, stop);
  });

  SynchronousChannelOptions options{};
  options.read_timeout = std::chrono::milliseconds{0};
  auto channel = SynchronousChannel::Connect("grpc://127.0.0.1:" + std::to_string(port), options);
  ASSERT_TRUE(channel.has_value()) << channel.error().Message();
  CallOptions rpc{};
  rpc.timeout = std::chrono::seconds{2};
  const auto started = std::chrono::steady_clock::now();
  auto resp = (*channel)->Unary("/test.v1.EchoService/Unary", BytesOf("slow"), std::move(rpc));
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started)
                              .count();
  ASSERT_TRUE(resp.has_value()) << resp.error().Message();
  EXPECT_TRUE(resp->status.Ok());
  EXPECT_EQ(StringOf(resp->payload), "slow");
  EXPECT_GT(elapsed_ms, 150);
  EXPECT_LT(elapsed_ms, 1500);
}

TEST(SyncStallTest, ReadTimeoutAbortsStall) {
  auto listener = nexora::TcpListener::BindLoopbackV4(0);
  ASSERT_TRUE(listener.has_value());
  const auto port = listener->LocalPort();
  ScopedStopThread server([&listener](const std::atomic<bool>& stop) {
    std::vector<std::byte> body{};
    auto srv = SynchronousServer::AcceptTcp(
        *listener, std::chrono::milliseconds{3000},
        [&body](SynchronousServer& srv, const ServerRequest& req) noexcept {
          DelayedEcho(srv, req, std::chrono::milliseconds{400}, body);
        });
    if (!srv.has_value()) return;
    ServeUntilStopped(**srv, stop);
  });

  SynchronousChannelOptions options{};
  options.read_timeout = std::chrono::milliseconds{80};
  auto channel = SynchronousChannel::Connect("grpc://127.0.0.1:" + std::to_string(port), options);
  ASSERT_TRUE(channel.has_value()) << channel.error().Message();
  CallOptions rpc{};
  rpc.timeout = std::chrono::seconds{2};
  const auto started = std::chrono::steady_clock::now();
  auto resp = (*channel)->Unary("/test.v1.EchoService/Unary", BytesOf("stall"), std::move(rpc));
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started)
                              .count();
  ASSERT_FALSE(resp.has_value());
  EXPECT_EQ(resp.error().Code(), StatusCode::kDeadlineExceeded);
  EXPECT_GT(elapsed_ms, 60);
  EXPECT_LT(elapsed_ms, 1000);
}

TEST(SyncLifetimeTest, MovedFromCallsAreSafe) {
  auto pipes = nexora::MemoryPipe::CreatePair(262144uz);
  ASSERT_TRUE(pipes.has_value());
  auto server = SynchronousServer::FromMemoryPipe(std::move(pipes->server));
  ASSERT_TRUE(server.has_value());
  auto channel = SynchronousChannel::FromMemoryPipe(std::move(pipes->client), "localhost");
  ASSERT_TRUE(channel.has_value());
  SynchronousServer* srv = server->get();
  (*channel)->SetProgressHook([srv]() noexcept { return srv->PollOnce(); });

  auto opened = (*channel)->BidiStreamCall("/test.v1.EchoService/Bidi");
  ASSERT_TRUE(opened.has_value());
  RpcStream moved = std::move(**opened);
  auto recv = (*opened)->RecvNext();
  EXPECT_FALSE(recv.has_value());
  EXPECT_EQ(recv.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_EQ(recv.error().Message(), "stream moved");
  auto finished = (*opened)->Finish();
  EXPECT_FALSE(finished.has_value());
  EXPECT_EQ(finished.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_FALSE((*opened)->Send(BytesOf("x")).has_value());
  EXPECT_FALSE((*opened)->HalfClose().has_value());
  EXPECT_FALSE((*opened)->TryRecvNext().has_value());
  EXPECT_FALSE((*opened)->Cancel().has_value());
  EXPECT_EQ((*opened)->StreamId(), 0U);
  EXPECT_TRUE((*opened)->IsDone());
  static_cast<void>((*opened)->InitialMetadata());
  static_cast<void>((*opened)->FinalStatus());
  opened->reset();
  EXPECT_NE(moved.StreamId(), 0U);
}

TEST(SyncLifetimeTest, DestroyChannelThenRecv) {
  auto pipes = nexora::MemoryPipe::CreatePair(262144uz);
  ASSERT_TRUE(pipes.has_value());
  std::size_t cancels = 0uz;
  auto server = SynchronousServer::FromMemoryPipe(
      std::move(pipes->server), [&cancels](SynchronousServer&, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kCancel) ++cancels;
      });
  ASSERT_TRUE(server.has_value());
  auto channel = SynchronousChannel::FromMemoryPipe(std::move(pipes->client), "localhost");
  ASSERT_TRUE(channel.has_value());
  SynchronousServer* srv = server->get();
  (*channel)->SetProgressHook([srv]() noexcept { return srv->PollOnce(); });

  auto opened = (*channel)->BidiStreamCall("/test.v1.EchoService/Bidi");
  ASSERT_TRUE(opened.has_value());
  ASSERT_TRUE((*opened)->Send(BytesOf("live")).has_value());
  RpcStream live = std::move(**opened);
  opened->reset();
  channel->reset();

  auto recv = live.RecvNext();
  ASSERT_FALSE(recv.has_value());
  EXPECT_EQ(recv.error().Code(), StatusCode::kFailedPrecondition);
  EXPECT_EQ(recv.error().Message(), "channel destroyed");
  auto finished = live.Finish();
  ASSERT_FALSE(finished.has_value());
  EXPECT_EQ(finished.error().Message(), "channel destroyed");
  EXPECT_EQ(live.FinalStatus().Message(), "channel destroyed");
  ASSERT_TRUE((*server)->PollOnce().has_value());
  EXPECT_FALSE((*server)->IsOpen());
  EXPECT_GE(cancels, 1uz);
}

}  // namespace
}  // namespace grpcxx
