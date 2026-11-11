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
#include <utility>

namespace {
using namespace grpcxx;

auto Now() noexcept -> MonotonicTimestamp {
  return static_cast<MonotonicTimestamp>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             std::chrono::steady_clock::now().time_since_epoch())
                                             .count());
}

struct EchoMessage {
  std::uint32_t sequence{};
};

struct[[= service{.name = "example.Echo"}]] EchoService;
struct EchoService {
  [[= rpc{.name = "Immediate", .kind = RpcKind::kUnary}]] EchoMessage Immediate(
      EchoMessage request);
  [[= rpc{.name = "Deferred", .kind = RpcKind::kUnary}]] EchoMessage Deferred(EchoMessage request);
};

/** A small explicit wire codec keeps this example independent of external IDL tooling. */
struct EchoCodec {
  static constexpr auto DecodeSequence(std::span<const std::byte, 4> input) noexcept
      -> std::uint32_t {
    if consteval {
      std::uint32_t sequence{};
      for (const auto byte : input)
        sequence = (sequence << 8) | std::to_integer<std::uint32_t>(byte);
      return sequence;
    } else {
      std::uint32_t sequence{};
      std::memcpy(&sequence, input.data(), sizeof(sequence));
      if constexpr (std::endian::native == std::endian::little) return std::byteswap(sequence);
      return sequence;
    }
  }
  template <typename T>
  static auto Encode(const T& message,
                     std::span<std::byte> output) noexcept -> std::expected<std::size_t, Status> {
    if (output.size() < 4) return std::unexpected(Status{StatusCode::kResourceExhausted});
    std::uint32_t wire = message.sequence;
    if constexpr (std::endian::native == std::endian::little) {
      wire = std::byteswap(wire);
    }
    std::memcpy(output.data(), &wire, sizeof(wire));
    return 4;
  }
  template <typename T>
  static auto Decode(std::span<const std::byte> input) noexcept -> std::expected<T, Status> {
    if (input.size() != 4) return std::unexpected(Status{StatusCode::kInvalidArgument});
    return T{.sequence = DecodeSequence(input.first<4>())};
  }
};

constexpr std::array kSequenceBytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{100}};
static_assert(EchoCodec::DecodeSequence(kSequenceBytes) == 100);

struct EchoHandlers {
  std::optional<UnaryResponder<EchoMessage>> pending;
  EchoMessage deferred{};
  auto Immediate(ServerContext&, const EchoMessage& request) noexcept
      -> std::expected<Response<EchoMessage>, Status> {
    return Response<EchoMessage>{.message = {.sequence = request.sequence + 1}};
  }
  void Deferred(ServerContext&, const EchoMessage& request,
                UnaryResponder<EchoMessage> responder) noexcept {
    deferred.sequence = request.sequence + 2;
    pending.emplace(std::move(responder));
  }
  bool CompleteDeferred() noexcept {
    if (!pending) return true;
    auto reply = pending->Complete(Response<EchoMessage>{.message = deferred});
    if (!reply) return false;
    pending.reset();
    return true;
  }
};

struct Application {
  EchoHandlers handlers;
  ServiceRouter<EchoService, EchoHandlers, EchoCodec> router{handlers};
  GrpcServerService* server{};
  ServerContext context{};
  bool good{true};
  unsigned replies{};
  struct ExpectedReply {
    Application* app;
    std::uint32_t sequence;
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
    return static_cast<Application*>(opaque)->server->Complete(
        connection, stream, generation, {}, static_cast<int>(std::to_underlying(status.Code())));
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
    auto decoded = EchoCodec::Decode<EchoMessage>(bytes);
    app.good = app.good && status == 0 && decoded.has_value() && !expected.received &&
               decoded->sequence == expected.sequence;
    expected.received = true;
    ++app.replies;
  }
};
}  // namespace

int main() {
  auto ring = uring::Proactor::Create(256);
  if (!ring) return 1;
  auto& proactor = *ring;
  nexora::CompletionRoutes<> routes;
  Application app;
  GrpcServerService server(proactor, routes, {.context = &app, .handler = &Application::Request});
  if (!server.Start()) return 1;
  auto endpoint = server.LocalAddress();
  if (!endpoint) return 1;
  ClientChannelService client(proactor, routes, {.endpoint = *endpoint});
  if (!client.Start()) return 1;
  const PollBudget budget{};
  auto tick = [&]() {
    const auto now = Now();
    return proactor.Tick(routes).has_value() && server.PollOnce(budget, now).has_value() &&
           client.PollOnce(budget, now).has_value();
  };
  for (unsigned i = 0;
       i < 100000 && client.GetState() != ClientChannelService::ChannelState::kReady; ++i)
    if (!tick()) return 1;
  if (client.GetState() != ClientChannelService::ChannelState::kReady) return 1;
  std::array<std::byte, 4> request{};
  if (!EchoCodec::Encode(EchoMessage{.sequence = 100}, request)) return 1;
  Application::ExpectedReply immediate{.app = &app, .sequence = 101};
  Application::ExpectedReply deferred{.app = &app, .sequence = 102};
  if (!client.CallUnary("/example.Echo/Immediate", request, &immediate, &Application::Response))
    return 1;
  if (!client.CallUnary("/example.Echo/Deferred", request, &deferred, &Application::Response))
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
    if (!proactor.Tick(routes)) return 1;
    auto c = client.PollShutdown(budget, now);
    auto s = server.PollShutdown(budget, now);
    if (!c || !s) return 1;
    client_drained = client.Stopped();
    server_drained = server.Stopped();
  }
  if (!client_drained || !server_drained || !client.Shutdown() || !server.Shutdown()) return 1;

  fmt::print("Immediate echo: 101; deferred echo: 102\n");
  return 0;
}
