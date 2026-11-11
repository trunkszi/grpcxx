#include <gtest/gtest.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "grpcxx/sync/in_memory_channel_pair.h"
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
    if (worker_.joinable()) {
      worker_.join();
    }
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

TEST(SyncSocketsTest, LoopbackTcpLargePayload) {
  auto listener = nexora::TcpListener::BindLoopbackV4(0);
  ASSERT_TRUE(listener.has_value());
  const std::uint16_t port = listener->LocalPort();
  ASSERT_GT(port, 0U);

  ScopedStopThread server_thread([&listener](const std::atomic<bool>& stop) {
    auto srv = SynchronousServer::AcceptTcp(
        *listener, std::chrono::milliseconds{5000},
        [](SynchronousServer& s, const ServerRequest& req) noexcept {
          if (req.event == ServerRequest::Event::kMessage) {
            static_cast<void>(s.Send(req.stream_id, req.payload));
          } else if (req.event == ServerRequest::Event::kHalfClose) {
            static_cast<void>(s.Finish(req.stream_id, Status::OkStatus()));
          }
        });
    if (!srv.has_value()) return;
    ServeUntilStopped(**srv, stop);
  });

  auto channel = SynchronousChannel::Connect("grpc://localhost:" + std::to_string(port));
  ASSERT_TRUE(channel.has_value()) << channel.error().Message();

  constexpr std::size_t kFiveMiB = 5uz * 1024uz * 1024uz;
  std::vector<std::byte> payload(kFiveMiB);
  for (std::size_t i = 0uz; i < kFiveMiB; ++i) {
    payload[i] = static_cast<std::byte>((i * 73uz + 9uz) & 0xffuz);
  }

  auto resp = (*channel)->Unary("/svc.Tcp/LargeEcho", payload);
  ASSERT_TRUE(resp.has_value()) << resp.error().Message();
  EXPECT_TRUE(resp->status.Ok());
  ASSERT_EQ(resp->payload.size(), kFiveMiB);
  EXPECT_TRUE(std::equal(resp->payload.begin(), resp->payload.end(), payload.begin()));

  (*channel)->Close();
  server_thread.StopAndJoin();
}

TEST(SyncSocketsTest, LoopbackTlsAndAlpnCheck) {
  auto srv_tls = nexora::TlsServerConfig::SelfSignedForLocalhost();
  ASSERT_TRUE(srv_tls.has_value());
  ASSERT_TRUE(srv_tls->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2).has_value());
  auto listener = nexora::TcpListener::BindLoopbackV4(0);
  ASSERT_TRUE(listener.has_value());
  const std::uint16_t port = listener->LocalPort();

  ScopedStopThread server_thread([&listener, &srv_tls](const std::atomic<bool>& stop) {
    auto srv = SynchronousServer::AcceptTls(
        *listener, *srv_tls, std::chrono::milliseconds{5000},
        [](SynchronousServer& s, const ServerRequest& req) noexcept {
          if (req.event == ServerRequest::Event::kMessage) {
            static_cast<void>(s.Send(req.stream_id, req.payload));
          } else if (req.event == ServerRequest::Event::kHalfClose) {
            static_cast<void>(s.Finish(req.stream_id, Status::OkStatus()));
          }
        });
    if (!srv.has_value()) return;
    ServeUntilStopped(**srv, stop);
  });

  SyncTlsClientOptions tls_opts{.insecure_skip_verify = true};
  auto channel = SynchronousChannel::Connect("grpcs://localhost:" + std::to_string(port), tls_opts);
  ASSERT_TRUE(channel.has_value()) << channel.error().Message();
  auto resp = (*channel)->Unary("/svc.Tls/Echo", BytesOf("tls13-h2-verified"));
  ASSERT_TRUE(resp.has_value()) << resp.error().Message();
  EXPECT_EQ(StringOf(resp->payload), "tls13-h2-verified");
  (*channel)->Close();
  server_thread.StopAndJoin();

  auto h1_only_tls = nexora::TlsServerConfig::SelfSignedForLocalhost();
  ASSERT_TRUE(h1_only_tls.has_value());
  static constexpr std::array<nexora::AlpnProtocol, 1> kHttp11Only{nexora::AlpnProtocol::kHttp11};
  ASSERT_TRUE(h1_only_tls->SetAlpn(kHttp11Only).has_value());

  auto bad_listener = nexora::TcpListener::BindLoopbackV4(0);
  ASSERT_TRUE(bad_listener.has_value());
  const std::uint16_t bad_port = bad_listener->LocalPort();

  ScopedStopThread bad_server([&bad_listener, &h1_only_tls](const std::atomic<bool>&) {
    static_cast<void>(
        SynchronousServer::AcceptTls(*bad_listener, *h1_only_tls, std::chrono::milliseconds{2000}));
  });

  auto rejected =
      SynchronousChannel::Connect("grpcs://127.0.0.1:" + std::to_string(bad_port), tls_opts);
  EXPECT_FALSE(rejected.has_value());
  bad_server.StopAndJoin();
}

TEST(SyncSocketsTest, UnixDomainSocketRoundtrip) {
  const std::string sock_path = "./grpcxx_sync_uds_" + std::to_string(::getpid()) + ".sock";
  auto listener = nexora::UnixListener::Bind(sock_path);
  ASSERT_TRUE(listener.has_value());

  ScopedStopThread uds_server([&listener](const std::atomic<bool>& stop) {
    auto srv = SynchronousServer::AcceptUnix(
        *listener, std::chrono::milliseconds{5000},
        [](SynchronousServer& s, const ServerRequest& req) noexcept {
          if (req.event == ServerRequest::Event::kMessage) {
            static_cast<void>(s.Send(req.stream_id, req.payload));
          } else if (req.event == ServerRequest::Event::kHalfClose) {
            static_cast<void>(s.Finish(req.stream_id, Status::OkStatus()));
          }
        });
    if (!srv.has_value()) return;
    ServeUntilStopped(**srv, stop);
  });

  auto channel = SynchronousChannel::Connect("unix://" + sock_path);
  ASSERT_TRUE(channel.has_value()) << channel.error().Message();
  auto resp = (*channel)->Unary("/svc.Unix/Echo", BytesOf("uds-ok"));
  ASSERT_TRUE(resp.has_value());
  EXPECT_EQ(StringOf(resp->payload), "uds-ok");
  (*channel)->Close();
  uds_server.StopAndJoin();
  listener->Close();

  auto sock_pair = nexora::UnixSocket::CreatePair();
  ASSERT_TRUE(sock_pair.has_value());
  auto pair_srv = SynchronousServer::FromUnix(
      std::move(sock_pair->second), [](SynchronousServer& s, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kMessage) {
          static_cast<void>(s.Send(req.stream_id, req.payload));
        } else if (req.event == ServerRequest::Event::kHalfClose) {
          static_cast<void>(s.Finish(req.stream_id, Status::OkStatus()));
        }
      });
  ASSERT_TRUE(pair_srv.has_value());
  auto pair_cli = SynchronousChannel::AdoptUnix(sock_pair->first.ReleaseFd());
  ASSERT_TRUE(pair_cli.has_value());
  SynchronousServer* srv_ptr = pair_srv->get();
  (*pair_cli)->SetProgressHook([srv_ptr]() noexcept { return srv_ptr->PollOnce(); });

  auto pair_resp = (*pair_cli)->Unary("/svc.UnixPair/Echo", BytesOf("adopted-uds"));
  ASSERT_TRUE(pair_resp.has_value());
  EXPECT_EQ(StringOf(pair_resp->payload), "adopted-uds");
}

TEST(SyncSocketsTest, DeadlineAndStreamCancel) {
  std::atomic<std::size_t> cancel_events{0uz};
  auto pair = InMemoryChannelPair::Create(
      [&cancel_events](SynchronousServer& srv, const ServerRequest& req) noexcept {
        if (req.event == ServerRequest::Event::kCancel) {
          cancel_events.fetch_add(1uz, std::memory_order_relaxed);
          return;
        }
        if (req.path == "/svc.Control/Hang") {
          return;
        }
        if (req.path == "/svc.Control/Healthy" && req.event == ServerRequest::Event::kHalfClose) {
          static_cast<void>(srv.Send(req.stream_id, BytesOf("still-healthy")));
          static_cast<void>(srv.Finish(req.stream_id, Status::OkStatus()));
        }
      });
  ASSERT_TRUE(pair.has_value());

  CallOptions short_timeout{};
  short_timeout.timeout = std::chrono::milliseconds{30};
  auto timed_out =
      (*pair)->Channel().Unary("/svc.Control/Hang", BytesOf("wait"), std::move(short_timeout));
  ASSERT_FALSE(timed_out.has_value());
  EXPECT_EQ(timed_out.error().Code(), StatusCode::kDeadlineExceeded);
  ASSERT_TRUE((*pair)->Server().PollOnce().has_value());
  EXPECT_GE((*pair)->Server().CancelledStreams(), 1uz);
  EXPECT_GE(cancel_events.load(std::memory_order_relaxed), 1uz);

  {
    auto stream = (*pair)->Channel().BidiStreamCall("/svc.Control/Hang");
    ASSERT_TRUE(stream.has_value());
    ASSERT_TRUE((*stream)->Send(BytesOf("inflight")).has_value());
    EXPECT_EQ((*pair)->Channel().ActiveStreams(), 1uz);
    ASSERT_TRUE((*stream)->Cancel().has_value());
  }
  ASSERT_TRUE((*pair)->Server().PollOnce().has_value());
  EXPECT_EQ((*pair)->Channel().ActiveStreams(), 0uz);
  EXPECT_GE((*pair)->Server().CancelledStreams(), 2uz);

  {
    auto scoped_stream = (*pair)->Channel().BidiStreamCall("/svc.Control/Hang");
    ASSERT_TRUE(scoped_stream.has_value());
    ASSERT_TRUE((*scoped_stream)->Send(BytesOf("raii-drop")).has_value());
  }
  ASSERT_TRUE((*pair)->Server().PollOnce().has_value());
  EXPECT_EQ((*pair)->Channel().ActiveStreams(), 0uz);
  EXPECT_GE((*pair)->Server().CancelledStreams(), 3uz);

  auto healthy = (*pair)->Channel().Unary("/svc.Control/Healthy", BytesOf("ping"));
  ASSERT_TRUE(healthy.has_value());
  EXPECT_EQ(StringOf(healthy->payload), "still-healthy");
}

}  // namespace
}  // namespace grpcxx
