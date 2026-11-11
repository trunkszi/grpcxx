#pragma once
#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/h2/h2_client_session.h>
#include <grpcxx/h2/session_detail.h>

#include <algorithm>
#include <limits>
#include <nexora/pool/endpoint_failover.hpp>
#include <nexora/socket/uring_socket.hpp>
#include <nexora/tls/tls_session.hpp>
#include <optional>
#include <string>

namespace grpcxx {
struct ClientChannelService::Impl {
  struct Call {
    Impl* owner{};
    UnaryCompletion completion;
    MessageCompletion message;
    static void Message(void* context, std::int32_t id,
                        std::span<const std::byte> payload) noexcept {
      auto& self = *static_cast<Call*>(context);
      if (self.message) self.message(id, payload);
    }
    std::int32_t stream{};
    int status{};
    h2_detail::MessageBuffer response;
    h2_detail::FlagState flags;
    static void Done(void* context, std::int32_t stream, int status,
                     std::span<const std::byte> payload) noexcept {
      auto& call = *static_cast<Call*>(context);
      call.stream = stream;
      call.status = status;
      if (call.owner && call.owner->session) {
        const auto msg = call.owner->session->LastStatusMessage();
        call.owner->last_message_size = std::min(msg.size(), call.owner->last_message.size());
        if (call.owner->last_message_size != 0)
          std::memcpy(call.owner->last_message.data(), msg.data(), call.owner->last_message_size);
      }
      if (!call.response.StoreRaw(payload)) {
        call.response.Reset();
        call.status = 8;
      }
      call.flags.Set<^^h2_detail::RuntimeFlags::ready>(true);
    }
  };

  Options options;
  std::string target, server_name;
  std::array<cares::Endpoint, 16> seeded_endpoints{};
  std::size_t seeded_count{};
  cares::Endpoint active_endpoint{};
  cares::Resolver<>* resolver{};
  std::optional<cares::QueryHandle> query;
  std::optional<cares::Result<cares::EndpointList>> resolution;
  std::unique_ptr<H2ClientSession> session{std::make_unique<H2ClientSession>()};
  nexora::EndpointFailover<> endpoints;
  std::optional<nexora::IoUringSocket> socket;
  std::optional<nexora::TlsSession> tls;
  std::array<Call, H2ClientSession::kMaxStreams> calls;
  uarch::Bitmap64 active{};
  ChannelState state{ChannelState::kIdle};
  std::array<std::byte, h2_detail::kIoBufferBytes> output;
  std::array<std::uint8_t, 65536> network;
  std::size_t output_size{}, output_offset{}, tx_size{};
  std::uint64_t connect_started_at{};
  h2_detail::FlagState flags;
  MonotonicTimestamp deadline{std::numeric_limits<MonotonicTimestamp>::max()};
  std::array<char, 512> last_message{};
  std::size_t last_message_size{};

  explicit Impl(Options opts) noexcept
      : options(opts),
        target(opts.target),
        server_name(opts.server_name),
        active_endpoint(opts.endpoint) {
    options.target = target;
    options.server_name = server_name;
    seeded_count = std::min(opts.endpoints.size(), seeded_endpoints.size());
    for (std::size_t idx = 0; idx < seeded_count; ++idx)
      seeded_endpoints[idx] = opts.endpoints[idx];
    options.endpoints = std::span<const cares::Endpoint>(seeded_endpoints.data(), seeded_count);
    for (auto& call : calls) call.owner = this;
  }

  static void Resolved(void* context, cares::Result<cares::EndpointList> result) noexcept {
    auto& self = *static_cast<Impl*>(context);
    self.query.reset();
    self.resolution.emplace(std::move(result));
  }

  void FailActiveCalls(int code) noexcept {
    for (const auto slot : active | uarch::views::ones) {
      auto& call = calls[slot];
      if (call.flags.Test<^^h2_detail::RuntimeFlags::ready>()) continue;
      if (session) static_cast<void>(session->Cancel(call.stream));
      call.response.Reset();
      call.status = code;
      call.flags.Set<^^h2_detail::RuntimeFlags::ready>(true);
    }
  }

  [[gnu::noinline, gnu::cold]] void StepResolver() noexcept {
    if (resolver) resolver->StepDriver();
  }
  [[gnu::noinline, gnu::cold]] void CancelResolver() noexcept {
    if (query && resolver) {
      static_cast<void>(resolver->CancelQuery(*query));
      query.reset();
    }
  }
  [[gnu::noinline]] bool SeedEndpoints(std::span<const cares::Endpoint> list) noexcept;
  [[nodiscard]] std::optional<cares::Endpoint> PickEndpoint(std::uint64_t now_ns) noexcept;
  void MarkConnected(std::uint64_t now_ns) noexcept;
  void ConfigureSessionTarget() noexcept;
  [[nodiscard]] std::string_view FormatTlsHost(std::span<char, 256> scratch) const noexcept;
  [[nodiscard]] std::expected<void, Status> ConnectNext(uring::Proactor& ring,
                                                        nexora::CompletionRoutes<>& routes,
                                                        std::uint64_t now_ns) noexcept;
  [[nodiscard]] std::expected<void, Status> RotateEndpoint(uring::Proactor& ring,
                                                           nexora::CompletionRoutes<>& routes,
                                                           std::uint64_t now_ns) noexcept;
  void TearDownAndFailover(uring::Proactor& ring, nexora::CompletionRoutes<>& routes,
                           std::uint64_t now_ns, int call_error = 14) noexcept;
  [[nodiscard]] std::expected<void, Status> Advance(uring::Proactor& ring,
                                                    nexora::CompletionRoutes<>& routes,
                                                    std::uint64_t now_ns) noexcept;
  [[nodiscard]] std::expected<void, Status> Ingress(std::span<const std::uint8_t>,
                                                    std::uint64_t now_ns) noexcept;
  [[nodiscard]] std::expected<void, Status> Egress() noexcept;
};
}  // namespace grpcxx
