#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/h2/nghttp2_client_session.h>
#include <grpcxx/reflection/unary_responder.h>
#include <grpcxx/server/grpc_server_service.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <memory>
#include <optional>
#include <tuple>

namespace {
using namespace grpcxx;
static_assert(ServiceConcept<ProactorService>);
static_assert(ServiceConcept<ClientChannelService>);
static_assert(ServiceConcept<GrpcServerService>);

/** The application boundary uses real kernel TCP and HTTP/2; payloads are opaque bytes. */
constexpr std::array kPayload{std::byte{0x11}, std::byte{0}, std::byte{0xfe}};

struct ByteCodec {
  template <typename T>
  static auto Encode(const T& payload,
                     std::span<std::byte> output) noexcept -> std::expected<std::size_t, Status> {
    if (output.size() < payload.size())
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    std::ranges::copy(payload, output.begin());
    return payload.size();
  }
};

struct Exchange {
  bool deferred{};
  bool probe_reentry{};
  bool requested{};
  bool completed{};
  bool valid_path{};
  bool valid_request{};
  bool valid_response{};
  bool reply_ok{};
  int status{-1};
  std::size_t connection{};
  std::int32_t stream{};
  std::uint64_t generation{};
  GrpcServerService* server{};
  ClientChannelService* client{};
  bool server_reentry_rejected{};
  bool client_reentry_rejected{};
  UnaryResponder<std::array<std::byte, 3>> responder{};
  UnaryResponder<std::array<std::byte, 3>> stale_responder{};
  bool captured{};
  unsigned requests{};
  unsigned completions{};
  unsigned deadline_completions{};
  unsigned unavailable_completions{};
  bool all_valid{true};

  static void Request(void* opaque, GrpcServerService& server, std::size_t connection,
                      std::int32_t stream, std::uint64_t generation, std::string_view path,
                      std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Exchange*>(opaque);
    self.requested = true;
    ++self.requests;
    self.valid_path = path == "/example.Echo/Echo";
    self.valid_request = std::ranges::equal(payload, kPayload);
    self.all_valid = self.all_valid && self.valid_path && self.valid_request;
    self.connection = connection;
    self.stream = stream;
    self.generation = generation;
    self.server = &server;
    if (self.probe_reentry) {
      const auto nested = server.PollOnce(PollBudget{}, 0);
      self.server_reentry_rejected =
          !nested && nested.error().Code() == StatusCode::kFailedPrecondition;
    }
    UnaryResponderSink sink{.context = &self,
                            .is_current =
                                [](void* opaque, std::uint64_t connection, std::int32_t stream,
                                   std::uint64_t generation) noexcept {
                                  return static_cast<Exchange*>(opaque)->server->IsCurrent(
                                      connection, stream, generation);
                                },
                            .complete =
                                [](void* opaque, std::uint64_t connection, std::int32_t stream,
                                   std::uint64_t generation, std::span<const std::byte> bytes,
                                   MetadataMap&&, MetadataMap&&) noexcept {
                                  return static_cast<Exchange*>(opaque)->server->Complete(
                                      connection, stream, generation, bytes);
                                }};
    self.responder = decltype(responder)::Bind<ByteCodec>(sink, connection, stream, generation);
    if (!self.captured) {
      self.stale_responder =
          decltype(responder)::Bind<ByteCodec>(sink, connection, stream, generation);
      self.captured = true;
    }
    if (!self.deferred) self.Reply();
  }

  void Reply() noexcept {
    reply_ok =
        responder.Complete(Response<std::array<std::byte, 3>>{.message = kPayload}).has_value();
  }

  static void Complete(void* opaque, std::int32_t, int status,
                       std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<Exchange*>(opaque);
    self.completed = true;
    ++self.completions;
    if (status == 4) ++self.deadline_completions;
    if (status == 14) ++self.unavailable_completions;
    self.status = status;
    self.valid_response = std::ranges::equal(payload, kPayload);
    self.all_valid = self.all_valid && self.valid_response && status == 0;
    if (self.probe_reentry) {
      const auto nested = self.client->PollOnce(PollBudget{}, 0);
      self.client_reentry_rejected =
          !nested && nested.error().Code() == StatusCode::kFailedPrecondition;
    }
  }
};

class UnaryLoopbackE2E : public testing::TestWithParam<std::tuple<bool, bool>> {};

TEST_P(UnaryLoopbackE2E, ImmediateAndDeferredResponseRejectRecursiveAdvanceAndDrain) {
  ProactorService proactor;
  const auto started = proactor.Start();
  ASSERT_TRUE(started.has_value()) << "A kernel with permitted io_uring is required";

  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  auto address = std::as_bytes(std::span{&endpoint, 1});
  const auto [deferred, unix_endpoint] = GetParam();
  sockaddr_un local{};
  if (unix_endpoint) {
    local.sun_family = AF_UNIX;
    constexpr std::string_view kPrefix{"grpcxx-ticket03-"};
    std::ranges::copy(kPrefix, local.sun_path + 1);
    const auto suffix =
        std::to_chars(local.sun_path + 1 + kPrefix.size(), local.sun_path + sizeof(local.sun_path),
                      static_cast<unsigned>(::getpid()));
    ASSERT_EQ(suffix.ec, std::errc{});
    address = std::as_bytes(std::span{&local, 1})
                  .first(offsetof(sockaddr_un, sun_path) +
                         static_cast<std::size_t>(suffix.ptr - local.sun_path));
  }
  Exchange exchange{.deferred = deferred, .probe_reentry = true};
  GrpcServerService server(
      proactor, {.address = address, .context = &exchange, .handler = &Exchange::Request});
  ASSERT_TRUE(server.Start().has_value());
  std::array<std::byte, sizeof(sockaddr_storage)> bound{};
  const auto bound_size = server.LocalAddress(bound);
  ASSERT_TRUE(bound_size.has_value());
  ClientChannelService client(proactor, {.address = std::span{bound}.first(*bound_size)});
  exchange.client = &client;
  ASSERT_TRUE(client.Start().has_value());

  const PollBudget budget{.frames = 1};
  auto tick = [&]() {
    const auto now = MonotonicTimestamp{};
    if (!proactor.PollOnce(budget, now)) return false;
    const auto s = server.PollOnce(budget, now);
    const auto c = client.PollOnce(budget, now);
    if (!s || !c) return false;
    EXPECT_LE(s->frames, 1U);
    EXPECT_LE(c->frames, 1U);
    return true;
  };
  for (unsigned i = 0;
       i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
    ASSERT_TRUE(tick());
  ASSERT_EQ(client.GetState(), ClientChannelService::ChannelState::kReady);
  ASSERT_TRUE(
      client.CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete).has_value());
  for (unsigned i = 0; i < 100000 && !exchange.requested; ++i) ASSERT_TRUE(tick());
  ASSERT_TRUE(exchange.requested);
  ASSERT_TRUE(exchange.valid_path);
  ASSERT_TRUE(exchange.valid_request);
  if (exchange.deferred) {
    EXPECT_FALSE(exchange.completed);
    ASSERT_TRUE(server.BeginShutdown(UINT64_MAX).has_value());
    ASSERT_TRUE(server.PollShutdown(budget, 0).has_value());
    EXPECT_FALSE(server.Stopped());
    ASSERT_TRUE(tick());
    exchange.Reply();
  }
  ASSERT_TRUE(exchange.reply_ok);
  for (unsigned i = 0; i < 100000 && !exchange.completed; ++i) ASSERT_TRUE(tick());
  ASSERT_TRUE(exchange.completed);
  EXPECT_EQ(exchange.status, 0);
  EXPECT_TRUE(exchange.valid_response);
  EXPECT_TRUE(exchange.server_reentry_rejected);
  EXPECT_TRUE(exchange.client_reentry_rejected);
  EXPECT_EQ(server.RejectedTransitions(), 1U);
  EXPECT_EQ(client.RejectedTransitions(), 1U);
  EXPECT_FALSE(exchange.responder.Complete(Response<std::array<std::byte, 3>>{.message = kPayload})
                   .has_value());
  EXPECT_FALSE(server.Complete(exchange.connection, exchange.stream, exchange.generation, kPayload)
                   .has_value());

  const auto client_shutdown = client.BeginShutdown(UINT64_MAX);
  ASSERT_TRUE(client_shutdown.has_value())
      << (client_shutdown ? std::string_view{} : client_shutdown.error().Message());
  ASSERT_TRUE(server.BeginShutdown(UINT64_MAX).has_value());
  bool client_drained = false;
  bool server_drained = false;
  for (unsigned i = 0; i < 100000 && !(client_drained && server_drained); ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, MonotonicTimestamp{}).has_value());
    const auto c = client.PollShutdown(budget, MonotonicTimestamp{});
    const auto s = server.PollShutdown(budget, MonotonicTimestamp{});
    ASSERT_TRUE(c.has_value());
    ASSERT_TRUE(s.has_value());
    client_drained = client.Stopped();
    server_drained = server.Stopped();
  }
  ASSERT_TRUE(client_drained);
  ASSERT_TRUE(server_drained);
  ASSERT_TRUE(client.Shutdown().has_value());
  ASSERT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  bool drained = false;
  for (unsigned i = 0; i < 100000 && !drained; ++i) {
    const auto result = proactor.PollShutdown(budget, MonotonicTimestamp{});
    ASSERT_TRUE(result.has_value());
    drained = proactor.Stopped();
  }
  ASSERT_TRUE(drained);
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

INSTANTIATE_TEST_SUITE_P(SocketEndpoints, UnaryLoopbackE2E,
                         testing::Combine(testing::Bool(), testing::Bool()));

TEST(UnaryLoopbackE2ELifecycle, ReusesConnectionSlotsAndRejectsAnOldResponder) {
  ProactorService proactor;
  ASSERT_TRUE(proactor.Start().has_value());
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  Exchange exchange{.deferred = true};
  GrpcServerService server(proactor, {.address = std::as_bytes(std::span{&endpoint, 1}),
                                      .context = &exchange,
                                      .handler = &Exchange::Request});
  ASSERT_TRUE(server.Start().has_value());
  std::array<std::byte, sizeof(sockaddr_in)> bound{};
  const auto bound_size = server.LocalAddress(bound);
  ASSERT_TRUE(bound_size.has_value());
  const PollBudget budget{};
  std::size_t first_connection{};
  std::int32_t first_stream{};
  std::uint64_t first_generation{};
  bool reused = false;

  for (unsigned attempt = 0; attempt < 20; ++attempt) {
    SCOPED_TRACE(attempt);
    exchange.requested = false;
    exchange.completed = false;
    exchange.reply_ok = false;
    ClientChannelService client(proactor, {.address = std::span{bound}.first(*bound_size)});
    ASSERT_TRUE(client.Start().has_value());
    auto tick = [&]() {
      return proactor.PollOnce(budget, 0).has_value() && server.PollOnce(budget, 0).has_value() &&
             client.PollOnce(budget, 0).has_value();
    };
    for (unsigned i = 0;
         i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
      ASSERT_TRUE(tick());
    ASSERT_EQ(client.GetState(), ClientChannelService::ChannelState::kReady);
    ASSERT_TRUE(client.CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                    .has_value());
    for (unsigned i = 0; i < 100000 && !exchange.requested; ++i) ASSERT_TRUE(tick());
    ASSERT_TRUE(exchange.requested);
    ASSERT_TRUE(exchange.valid_path);
    ASSERT_TRUE(exchange.valid_request);
    if (attempt == 0) {
      first_connection = exchange.connection;
      first_stream = exchange.stream;
      first_generation = exchange.generation;
    } else {
      if (exchange.connection == first_connection && exchange.stream == first_stream) {
        reused = true;
        EXPECT_NE(exchange.generation, first_generation);
      }
      EXPECT_FALSE(exchange.stale_responder.Current());
      EXPECT_FALSE(
          exchange.stale_responder.Complete(Response<std::array<std::byte, 3>>{.message = kPayload})
              .has_value());
      EXPECT_FALSE(
          server.Complete(first_connection, first_stream, first_generation, kPayload).has_value());
      EXPECT_TRUE(server.IsCurrent(exchange.connection, exchange.stream, exchange.generation));
    }
    exchange.Reply();
    ASSERT_TRUE(exchange.reply_ok);
    for (unsigned i = 0; i < 100000 && !exchange.completed; ++i) ASSERT_TRUE(tick());
    ASSERT_TRUE(exchange.completed);
    EXPECT_EQ(exchange.status, 0);
    EXPECT_TRUE(exchange.valid_response);
    ASSERT_TRUE(client.BeginShutdown(UINT64_MAX).has_value());
    for (unsigned i = 0; i < 100000 && !client.Stopped(); ++i) {
      ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
      ASSERT_TRUE(server.PollOnce(budget, 0).has_value());
      ASSERT_TRUE(client.PollShutdown(budget, 0).has_value());
    }
    ASSERT_TRUE(client.Stopped());
    ASSERT_TRUE(client.Shutdown().has_value());
  }
  EXPECT_TRUE(reused);
  ASSERT_TRUE(server.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !server.Stopped(); ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    ASSERT_TRUE(server.PollShutdown(budget, 0).has_value());
  }
  ASSERT_TRUE(server.Stopped());
  ASSERT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
    ASSERT_TRUE(proactor.PollShutdown(budget, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

TEST(UnaryLoopbackE2EScenario, OneFrameBudgetBoundsThreeQueuedUnaryCallsAndDrain) {
  ProactorService proactor;
  ASSERT_TRUE(proactor.Start().has_value());
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  Exchange exchange{};
  GrpcServerService server(proactor, {.address = std::as_bytes(std::span{&endpoint, 1}),
                                      .context = &exchange,
                                      .handler = &Exchange::Request});
  ASSERT_TRUE(server.Start().has_value());
  std::array<std::byte, sizeof(sockaddr_in)> bound{};
  const auto bound_size = server.LocalAddress(bound);
  ASSERT_TRUE(bound_size.has_value());
  ClientChannelService client(proactor, {.address = std::span{bound}.first(*bound_size)});
  ASSERT_TRUE(client.Start().has_value());
  const PollBudget budget{.frames = 1};
  unsigned server_frames = 0;
  unsigned client_frames = 0;
  auto tick = [&]() {
    if (!proactor.PollOnce(budget, 0)) return false;
    const auto requests_before = exchange.requests;
    const auto s = server.PollOnce(budget, 0);
    if (!s) return false;
    EXPECT_LE(s->frames, 1U);
    EXPECT_LE(exchange.requests - requests_before, 1U);
    server_frames += s->frames;
    const auto completions_before = exchange.completions;
    const auto c = client.PollOnce(budget, 0);
    if (!c) return false;
    EXPECT_LE(c->frames, 1U);
    EXPECT_LE(exchange.completions - completions_before, 1U);
    client_frames += c->frames;
    return true;
  };
  for (unsigned i = 0;
       i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
    ASSERT_TRUE(tick());
  ASSERT_EQ(client.GetState(), ClientChannelService::ChannelState::kReady);
  for (unsigned i = 0; i < 3; ++i)
    ASSERT_TRUE(client.CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                    .has_value());
  for (unsigned i = 0; i < 100000 && exchange.completions != 3; ++i) ASSERT_TRUE(tick());
  ASSERT_EQ(exchange.requests, 3U);
  ASSERT_EQ(exchange.completions, 3U);
  EXPECT_TRUE(exchange.all_valid);
  EXPECT_GT(server_frames, 0U);
  EXPECT_GT(client_frames, 0U);

  ASSERT_TRUE(client.BeginShutdown(UINT64_MAX).has_value());
  ASSERT_TRUE(server.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !(client.Stopped() && server.Stopped()); ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    const auto c = client.PollShutdown(budget, 0);
    const auto s = server.PollShutdown(budget, 0);
    ASSERT_TRUE(c.has_value());
    ASSERT_TRUE(s.has_value());
    EXPECT_LE(c->frames, 1U);
    EXPECT_LE(s->frames, 1U);
  }
  ASSERT_TRUE(client.Stopped());
  ASSERT_TRUE(server.Stopped());
  ASSERT_TRUE(client.Shutdown().has_value());
  ASSERT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
    ASSERT_TRUE(proactor.PollShutdown(budget, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

TEST(UnaryLoopbackE2ELifecycle, ShutdownAfterKernelCallbackBeforeServiceConsumption) {
  ProactorService proactor;
  ASSERT_TRUE(proactor.Start().has_value());
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  Exchange exchange{};
  GrpcServerService server(proactor, {.address = std::as_bytes(std::span{&endpoint, 1}),
                                      .context = &exchange,
                                      .handler = &Exchange::Request});
  ASSERT_TRUE(server.Start().has_value());
  std::array<std::byte, sizeof(sockaddr_in)> bound{};
  const auto bound_size = server.LocalAddress(bound);
  ASSERT_TRUE(bound_size.has_value());
  ClientChannelService client(proactor, {.address = std::span{bound}.first(*bound_size)});
  ASSERT_TRUE(client.Start().has_value());
  const PollBudget budget{.frames = 1};
  bool callback_ran = false;
  /** Only ACCEPT and CONNECT are submitted; neither service consumes their callbacks yet. */
  for (unsigned i = 0; i < 100000 && !callback_ran; ++i) {
    const auto result = proactor.PollOnce(budget, 0);
    ASSERT_TRUE(result.has_value());
    callback_ran = result->completions != 0;
  }
  ASSERT_TRUE(callback_ran);
  const auto server_shutdown = server.BeginShutdown(UINT64_MAX);
  EXPECT_TRUE(server_shutdown.has_value())
      << (server_shutdown ? std::string_view{} : server_shutdown.error().Message());
  const auto client_shutdown = client.BeginShutdown(UINT64_MAX);
  EXPECT_TRUE(client_shutdown.has_value())
      << (client_shutdown ? std::string_view{} : client_shutdown.error().Message());
  for (unsigned i = 0; i < 100000 && !(client.Stopped() && server.Stopped()); ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    const auto result = client.PollShutdown(budget, 0);
    ASSERT_TRUE(result.has_value()) << (result ? std::string_view{} : result.error().Message());
    ASSERT_TRUE(server.PollShutdown(budget, 0).has_value());
  }
  ASSERT_TRUE(client.Stopped());
  ASSERT_TRUE(server.Stopped());
  ASSERT_TRUE(client.Shutdown().has_value());
  ASSERT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
    ASSERT_TRUE(proactor.PollShutdown(budget, 0).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
}

TEST(UnaryLoopbackE2ELifecycle, InvalidBudgetPreservesDeferredCallsUntilTheActualShutdownDeadline) {
  ProactorService proactor;
  ASSERT_TRUE(proactor.Start().has_value());
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  Exchange exchange{.deferred = true};
  GrpcServerService server(proactor, {.address = std::as_bytes(std::span{&endpoint, 1}),
                                      .context = &exchange,
                                      .handler = &Exchange::Request});
  ASSERT_TRUE(server.Start().has_value());
  EXPECT_FALSE(server.Start().has_value());
  EXPECT_EQ(server.RejectedTransitions(), 1U);
  std::array<std::byte, sizeof(sockaddr_in)> bound{};
  const auto bound_size = server.LocalAddress(bound);
  ASSERT_TRUE(bound_size.has_value());
  ClientChannelService client(proactor, {.address = std::span{bound}.first(*bound_size)});
  ASSERT_TRUE(client.Start().has_value());
  EXPECT_FALSE(client.Start().has_value());
  EXPECT_EQ(client.RejectedTransitions(), 1U);
  const PollBudget budget{.commands = 1, .completions = 1, .frames = 1};
  auto tick = [&]() {
    return proactor.PollOnce(budget, 0).has_value() && server.PollOnce(budget, 0).has_value() &&
           client.PollOnce(budget, 0).has_value();
  };
  for (unsigned i = 0;
       i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
    ASSERT_TRUE(tick());
  ASSERT_EQ(client.GetState(), ClientChannelService::ChannelState::kReady);
  for (unsigned i = 0; i < 3; ++i)
    ASSERT_TRUE(client.CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                    .has_value());
  for (unsigned i = 0; i < 100000 && exchange.requests != 3; ++i) ASSERT_TRUE(tick());
  ASSERT_EQ(exchange.requests, 3U);
  ASSERT_TRUE(exchange.responder.Current());
  ASSERT_TRUE(server.BeginShutdown(100).has_value());
  ASSERT_TRUE(client.BeginShutdown(100).has_value());
  const PollBudget invalid{.frames = 0};
  const auto invalid_server = server.PollShutdown(invalid, 100);
  const auto invalid_client = client.PollShutdown(invalid, 100);
  ASSERT_FALSE(invalid_server.has_value());
  ASSERT_FALSE(invalid_client.has_value());
  EXPECT_EQ(invalid_server.error().Code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(invalid_client.error().Code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(server.RejectedTransitions(), 1U);
  EXPECT_EQ(client.RejectedTransitions(), 1U);
  EXPECT_TRUE(exchange.responder.Current());
  EXPECT_EQ(exchange.completions, 0U);
  ASSERT_TRUE(server.PollShutdown(budget, 99).has_value());
  ASSERT_TRUE(client.PollShutdown(budget, 99).has_value());
  EXPECT_TRUE(exchange.responder.Current());
  EXPECT_EQ(exchange.completions, 0U);

  for (unsigned i = 0; i < 100000 && !(client.Stopped() && server.Stopped()); ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 100).has_value());
    const auto before = exchange.completions;
    const auto c = client.PollShutdown(budget, 100);
    const auto s = server.PollShutdown(budget, 100);
    ASSERT_TRUE(c.has_value());
    ASSERT_TRUE(s.has_value());
    EXPECT_LE(c->commands, 1U);
    EXPECT_LE(s->commands, 1U);
    EXPECT_LE(c->completions, 1U);
    EXPECT_LE(exchange.completions - before, 1U);
  }
  ASSERT_TRUE(client.Stopped());
  ASSERT_TRUE(server.Stopped());
  EXPECT_EQ(exchange.completions, 3U);
  EXPECT_EQ(exchange.deadline_completions, 3U);
  EXPECT_EQ(exchange.status, 4);
  EXPECT_FALSE(exchange.responder.Current());
  EXPECT_FALSE(exchange.responder.Complete(Response<std::array<std::byte, 3>>{.message = kPayload})
                   .has_value());
  ASSERT_TRUE(client.Shutdown().has_value());
  ASSERT_TRUE(server.Shutdown().has_value());
  ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
  for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
    ASSERT_TRUE(proactor.PollShutdown(budget, 100).has_value());
  ASSERT_TRUE(proactor.Stopped());
  EXPECT_TRUE(proactor.Shutdown().has_value());
  EXPECT_EQ(exchange.completions, 3U);
}
class UnaryBoundaryE2E : public testing::Test {
 protected:
  ProactorService proactor;
  Exchange exchange;
  std::unique_ptr<GrpcServerService> server;
  std::unique_ptr<ClientChannelService> client;
  std::array<std::byte, sizeof(sockaddr_storage)> bound{};
  std::size_t bound_size{};
  const PollBudget budget{.commands = 1, .completions = 1, .frames = 1};
  int raw_fd{-1};
  std::array<std::byte, 24> raw_output{};
  std::array<std::byte, 256> raw_input{};
  std::optional<OperationId> raw_operation;
  bool raw_done{};
  int raw_result{};
  unsigned rejected_callbacks{};

  auto Address() const noexcept -> std::span<const std::byte> {
    return std::span{bound}.first(bound_size);
  }
  bool Tick() noexcept {
    return proactor.PollOnce(budget, 0).has_value() && server->PollOnce(budget, 0).has_value() &&
           (!client || client->PollOnce(budget, 0).has_value());
  }
  bool ConnectClient() noexcept {
    client = std::make_unique<ClientChannelService>(
        proactor, ClientChannelService::Options{.address = Address()});
    if (!client->Start()) return false;
    for (unsigned i = 0;
         i < 100000 && client->GetState() != ClientChannelService::ChannelState::kReady; ++i)
      if (!Tick()) return false;
    return client->GetState() == ClientChannelService::ChannelState::kReady;
  }
  auto RawCompletion() noexcept -> ProactorService::CompletionCallback {
    raw_done = false;
    return [this](int result) noexcept {
      raw_result = result;
      raw_done = true;
      raw_operation.reset();
    };
  }
  bool WaitRaw() noexcept {
    for (unsigned i = 0; i < 100000 && !raw_done; ++i)
      if (!Tick()) return false;
    return raw_done;
  }
  void SetUp() override {
    ASSERT_TRUE(proactor.Start().has_value());
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server = std::make_unique<GrpcServerService>(
        proactor, GrpcServerService::Options{.address = std::as_bytes(std::span{&endpoint, 1}),
                                             .context = &exchange,
                                             .handler = &Exchange::Request});
    ASSERT_TRUE(server->Start().has_value());
    const auto size = server->LocalAddress(bound);
    ASSERT_TRUE(size.has_value());
    bound_size = *size;
  }
  void TearDown() override {
    if (!server) return;
    if (raw_operation) {
      EXPECT_TRUE(proactor.Cancel(*raw_operation).has_value());
      for (unsigned i = 0; i < 100000 && raw_operation; ++i) {
        EXPECT_TRUE(proactor.PollOnce(budget, 0).has_value());
        EXPECT_TRUE(server->PollOnce(budget, 0).has_value());
      }
      EXPECT_FALSE(raw_operation.has_value());
    }
    if (raw_fd >= 0) {
      EXPECT_EQ(::close(raw_fd), 0);
    }
    if (client) {
      EXPECT_TRUE(client->BeginShutdown(UINT64_MAX).has_value());
    }
    ASSERT_TRUE(server->BeginShutdown(UINT64_MAX).has_value());
    for (unsigned i = 0; i < 100000 && (!server->Stopped() || (client && !client->Stopped()));
         ++i) {
      ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
      ASSERT_TRUE(server->PollShutdown(budget, 0).has_value());
      if (client) {
        ASSERT_TRUE(client->PollShutdown(budget, 0).has_value());
      }
    }
    ASSERT_TRUE(server->Stopped());
    ASSERT_TRUE(server->Shutdown().has_value());
    if (client) {
      ASSERT_TRUE(client->Stopped());
      ASSERT_TRUE(client->Shutdown().has_value());
    }
    ASSERT_TRUE(proactor.BeginShutdown(UINT64_MAX).has_value());
    for (unsigned i = 0; i < 100000 && !proactor.Stopped(); ++i)
      ASSERT_TRUE(proactor.PollShutdown(budget, 0).has_value());
    ASSERT_TRUE(proactor.Stopped());
    EXPECT_TRUE(proactor.Shutdown().has_value());
    EXPECT_EQ(rejected_callbacks, 0U);
  }
};

TEST_F(UnaryBoundaryE2E, IdleListenerHasNoReadyWork) {
  for (unsigned i = 0; i < 3; ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    const auto result = server->PollOnce(budget, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->pending);
    EXPECT_EQ(result->commands, 0U);
    EXPECT_EQ(result->socket_events, 0U);
    EXPECT_EQ(result->frames, 0U);
  }
}

TEST_F(UnaryBoundaryE2E, OversizedSubmissionDoesNotInvokeItsCallbackOrPoisonTheChannel) {
  ASSERT_TRUE(ConnectClient());
  std::array<std::byte, Nghttp2ClientSession::kMaxMessageBytes + 1> oversized{};
  const auto rejected = client->CallUnary(
      "/example.Echo/Echo", oversized,
      [this](std::int32_t, int, std::span<const std::byte>) noexcept { ++rejected_callbacks; });
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().Code(), StatusCode::kResourceExhausted);
  ASSERT_TRUE(client->CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                  .has_value());
  for (unsigned i = 0; i < 100000 && exchange.completions == 0; ++i) ASSERT_TRUE(Tick());
  EXPECT_EQ(exchange.requests, 1U);
  EXPECT_EQ(exchange.completions, 1U);
  EXPECT_TRUE(exchange.all_valid);
  EXPECT_EQ(rejected_callbacks, 0U);
}

TEST_F(UnaryBoundaryE2E, MalformedTcpPrefaceClosesOnlyItsConnectionAndLeavesTheServerUsable) {
  raw_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  ASSERT_GE(raw_fd, 0);
  const auto connected = proactor.SubmitConnect(raw_fd, Address(), RawCompletion());
  ASSERT_TRUE(connected.has_value());
  raw_operation = *connected;
  ASSERT_TRUE(WaitRaw());
  ASSERT_EQ(raw_result, 0);
  raw_output.fill(std::byte{0x58});
  std::size_t sent = 0;
  while (sent != raw_output.size()) {
    const auto operation =
        proactor.SubmitSend(raw_fd, std::span{raw_output}.subspan(sent), RawCompletion());
    ASSERT_TRUE(operation.has_value());
    raw_operation = *operation;
    ASSERT_TRUE(WaitRaw());
    ASSERT_GT(raw_result, 0);
    sent += static_cast<std::size_t>(raw_result);
  }
  bool closed = false;
  for (unsigned i = 0; i < 100 && !closed; ++i) {
    const auto operation = proactor.SubmitRecv(raw_fd, raw_input, RawCompletion());
    ASSERT_TRUE(operation.has_value());
    raw_operation = *operation;
    ASSERT_TRUE(WaitRaw());
    closed = raw_result == 0 || raw_result == -ECONNRESET;
    ASSERT_TRUE(raw_result > 0 || closed);
  }
  EXPECT_TRUE(closed);
  bool idle = false;
  for (unsigned i = 0; i < 100000 && !idle; ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    const auto result = server->PollOnce(budget, 0);
    ASSERT_TRUE(result.has_value());
    idle = !result->pending;
  }
  EXPECT_TRUE(idle);
  EXPECT_EQ(exchange.requests, 0U);
  ASSERT_TRUE(ConnectClient());
  ASSERT_TRUE(client->CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                  .has_value());
  for (unsigned i = 0; i < 100000 && exchange.completions == 0; ++i) ASSERT_TRUE(Tick());
  EXPECT_EQ(exchange.requests, 1U);
  EXPECT_EQ(exchange.completions, 1U);
  EXPECT_TRUE(exchange.all_valid);
}

TEST_F(UnaryBoundaryE2E, PeerCloseFailsDeferredCallsOnceWithinTheCompletionBudget) {
  exchange.deferred = true;
  ASSERT_TRUE(ConnectClient());
  for (unsigned i = 0; i < 3; ++i)
    ASSERT_TRUE(client->CallUnary("/example.Echo/Echo", kPayload, &exchange, &Exchange::Complete)
                    .has_value());
  for (unsigned i = 0; i < 100000 && exchange.requests != 3; ++i) ASSERT_TRUE(Tick());
  ASSERT_EQ(exchange.requests, 3U);
  ASSERT_TRUE(server->BeginShutdown(0).has_value());
  bool unavailable = false;
  for (unsigned i = 0; i < 100000 && exchange.completions != 3; ++i) {
    ASSERT_TRUE(proactor.PollOnce(budget, 0).has_value());
    ASSERT_TRUE(server->PollShutdown(budget, 0).has_value());
    const auto before = exchange.completions;
    const auto result = client->PollOnce(budget, 0);
    if (!result) {
      EXPECT_EQ(result.error().Code(), StatusCode::kUnavailable);
      unavailable = true;
    } else {
      EXPECT_LE(result->completions, 1U);
      if (unavailable && exchange.completions != 3) {
        EXPECT_TRUE(result->pending);
      }
    }
    EXPECT_LE(exchange.completions - before, 1U);
  }
  EXPECT_TRUE(unavailable);
  EXPECT_EQ(exchange.completions, 3U);
  EXPECT_EQ(exchange.unavailable_completions, 3U);
  ASSERT_TRUE(client->BeginShutdown(0).has_value());
}
}  // namespace
