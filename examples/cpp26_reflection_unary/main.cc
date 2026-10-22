#include <fmt/format.h>
#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/reflection/service_router.h>
#include <grpcxx/server/grpc_server_service.h>
#include <netinet/in.h>

#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <optional>

namespace {
using namespace grpcxx;

auto Now() noexcept -> MonotonicTimestamp {
  return static_cast<MonotonicTimestamp>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             std::chrono::steady_clock::now().time_since_epoch())
                                             .count());
}

struct Quote {
  std::uint32_t price{};
};

struct[[= service{.name = "example.Quotes"}]] Quotes {
  [[= rpc{.name = "Immediate", .kind = RpcKind::kUnary}]] Quote Immediate(Quote request);
  [[= rpc{.name = "Deferred", .kind = RpcKind::kUnary}]] Quote Deferred(Quote request);
};

/** A small explicit wire codec keeps this example independent of external IDL tooling. */
struct QuoteCodec {
  static constexpr auto DecodePrice(std::span<const std::byte, 4> input) noexcept -> std::uint32_t {
    if consteval {
      std::uint32_t price{};
      for (const auto byte : input) price = (price << 8) | std::to_integer<std::uint32_t>(byte);
      return price;
    } else {
      std::uint32_t price{};
      std::memcpy(&price, input.data(), sizeof(price));
      if constexpr (std::endian::native == std::endian::little) return std::byteswap(price);
      return price;
    }
  }
  template <typename T>
  static auto Encode(const T& quote,
                     std::span<std::byte> output) noexcept -> std::expected<std::size_t, Status> {
    if (output.size() < 4) return std::unexpected(Status{StatusCode::kResourceExhausted});
    for (unsigned i = 0; i < 4; ++i)
      output[i] = std::byte{static_cast<unsigned char>(quote.price >> (24 - 8 * i))};
    return 4;
  }
  template <typename T>
  static auto Decode(std::span<const std::byte> input) noexcept -> std::expected<T, Status> {
    if (input.size() != 4) return std::unexpected(Status{StatusCode::kInvalidArgument});
    return T{.price = DecodePrice(input.first<4>())};
  }
};

constexpr std::array kPriceBytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{100}};
static_assert(QuoteCodec::DecodePrice(kPriceBytes) == 100);

struct QuoteHandlers {
  std::optional<UnaryResponder<Quote>> pending;
  Quote deferred{};
  auto Immediate(ServerContext&,
                 const Quote& request) noexcept -> std::expected<Response<Quote>, Status> {
    return Response<Quote>{.message = {.price = request.price + 1}};
  }
  void Deferred(ServerContext&, const Quote& request, UnaryResponder<Quote> responder) noexcept {
    deferred.price = request.price + 2;
    pending.emplace(std::move(responder));
  }
  bool CompleteDeferred() noexcept {
    if (!pending) return true;
    auto reply = pending->Complete(Response<Quote>{.message = deferred});
    if (!reply) return false;
    pending.reset();
    return true;
  }
};

struct Application {
  QuoteHandlers handlers;
  ServiceRouter<Quotes, QuoteHandlers, QuoteCodec> router{handlers};
  GrpcServerService* server{};
  ServerContext context{};
  bool good{true};
  unsigned replies{};
  struct ExpectedReply {
    Application* app;
    std::uint32_t price;
    bool received{};
  };

  static auto SinkComplete(void* opaque, std::uint64_t connection, std::int32_t stream,
                           std::uint64_t generation, std::span<const std::byte> bytes,
                           MetadataMap&&, MetadataMap&&) noexcept -> std::expected<void, Status> {
    return static_cast<Application*>(opaque)->server->Complete(connection, stream, generation,
                                                               bytes);
  }
  static auto SinkFail(void* opaque, std::uint64_t connection, std::int32_t stream,
                       std::uint64_t generation,
                       Status status) noexcept -> std::expected<void, Status> {
    return static_cast<Application*>(opaque)->server->Complete(connection, stream, generation, {},
                                                               static_cast<int>(status.Code()));
  }
  static void Request(void* opaque, GrpcServerService& server, std::size_t connection,
                      std::int32_t stream, std::uint64_t generation, std::string_view path,
                      std::span<const std::byte> bytes) noexcept {
    auto& app = *static_cast<Application*>(opaque);
    app.server = &server;
    UnaryResponderSink sink{.context = &app,
                            .is_current =
                                [](void* opaque, std::uint64_t connection, std::int32_t stream,
                                   std::uint64_t generation) noexcept {
                                  return static_cast<Application*>(opaque)->server->IsCurrent(
                                      connection, stream, generation);
                                },
                            .complete = &SinkComplete,
                            .fail = &SinkFail};
    const auto dispatched =
        app.router.Dispatch(path, app.context, bytes, sink, connection, stream, generation);
    app.good = app.good && dispatched.has_value();
  }
  static void Response(void* opaque, std::int32_t, int status,
                       std::span<const std::byte> bytes) noexcept {
    auto& expected = *static_cast<ExpectedReply*>(opaque);
    auto& app = *expected.app;
    auto quote = QuoteCodec::Decode<Quote>(bytes);
    app.good = app.good && status == 0 && quote.has_value() && !expected.received &&
               quote->price == expected.price;
    expected.received = true;
    ++app.replies;
  }
};
}  // namespace

int main() {
  ProactorService proactor;
  if (auto started = proactor.Start(); !started) {
    fmt::print("Proactor start failed: {}\n", started.error().Message());
    return 1;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  Application app;
  GrpcServerService server(proactor, {.address = std::as_bytes(std::span{&address, 1}),
                                      .context = &app,
                                      .handler = &Application::Request});
  if (!server.Start()) return 1;
  std::array<std::byte, sizeof(sockaddr_in)> bound{};
  auto length = server.LocalAddress(bound);
  if (!length) return 1;
  ClientChannelService client(proactor, {.address = std::span{bound}.first(*length)});
  if (!client.Start()) return 1;
  const PollBudget budget{};
  auto tick = [&]() {
    const auto now = Now();
    return proactor.PollOnce(budget, now).has_value() && server.PollOnce(budget, now).has_value() &&
           client.PollOnce(budget, now).has_value();
  };
  for (unsigned i = 0;
       i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
    if (!tick()) return 1;
  if (client.GetState() != ClientChannelService::ChannelState::kReady) return 1;
  std::array<std::byte, 4> request{};
  if (!QuoteCodec::Encode(Quote{.price = 100}, request)) return 1;
  Application::ExpectedReply immediate{.app = &app, .price = 101};
  Application::ExpectedReply deferred{.app = &app, .price = 102};
  if (!client.CallUnary("/example.Quotes/Immediate", request, &immediate, &Application::Response))
    return 1;
  if (!client.CallUnary("/example.Quotes/Deferred", request, &deferred, &Application::Response))
    return 1;
  for (unsigned i = 0; i < 100000 && app.replies != 2; ++i) {
    /** Deferred work completes on a later caller tick, before services progress. */
    if (!app.handlers.CompleteDeferred() || !tick() || !app.good) return 1;
  }
  if (app.replies != 2 || !app.good || !immediate.received || !deferred.received) return 1;
  if (!client.BeginShutdown(UINT64_MAX) || !server.BeginShutdown(UINT64_MAX)) return 1;
  bool client_drained = false;
  bool server_drained = false;
  for (unsigned i = 0; i < 100000 && !(client_drained && server_drained); ++i) {
    const auto now = Now();
    if (!proactor.PollOnce(budget, now)) return 1;
    auto c = client.PollShutdown(budget, now);
    auto s = server.PollShutdown(budget, now);
    if (!c || !s) return 1;
    client_drained = client.Stopped();
    server_drained = server.Stopped();
  }
  if (!client_drained || !server_drained || !client.Shutdown() || !server.Shutdown()) return 1;
  if (!proactor.BeginShutdown(UINT64_MAX)) return 1;
  bool drained = false;
  for (unsigned i = 0; i < 100000 && !drained; ++i) {
    auto result = proactor.PollShutdown(budget, Now());
    if (!result) return 1;
    drained = proactor.Stopped();
  }
  if (!drained || !proactor.Shutdown()) return 1;
  fmt::print("Immediate quote: 101; deferred quote: 102\n");
  return 0;
}
