#pragma once
#include <grpcxx/core/inplace_delegate.h>
#include <grpcxx/core/service.h>

#include <cares/cares.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <nexora/dispatch/completion_routes.hpp>
#include <nexora/tls/tls_config.hpp>
#include <span>
#include <string_view>
#include <uring/uring.hpp>

namespace grpcxx {
/** gRPC HTTP/2 channel with optional TLS; the caller drives this and its shared uring::Proactor. */
class ClientChannelService final {
 public:
  using UnaryCompletion =
      InplaceDelegate<void(std::int32_t, int, std::span<const std::byte>) noexcept>;
  using Completion = void (*)(void*, std::int32_t, int, std::span<const std::byte>) noexcept;
  enum class ChannelState : std::uint8_t {
    kIdle,
    kConnecting,
    kReady,
    kTransientFailure,
    kShuttingDown,
    kClosed
  };
  struct Options {
    cares::Endpoint endpoint{cares::IpAddress::FromV4({127, 0, 0, 1}), 0};
    std::span<const cares::Endpoint> endpoints{};
    std::string_view target{};
    std::uint16_t port{80};
    const nexora::TlsClientConfig* tls{};
    std::string_view server_name{};
    std::uint64_t connect_timeout_ns{5'000'000'000ULL};
    std::uint64_t default_timeout_ns{0};
  };
  ClientChannelService(uring::Proactor&, nexora::CompletionRoutes<>&, Options) noexcept;
  ClientChannelService(uring::Proactor&, nexora::CompletionRoutes<>&, cares::Resolver<>&,
                       Options) noexcept;
  ~ClientChannelService();
  ClientChannelService(const ClientChannelService&) =
      delete("ClientChannelService is pinned to its owner uring::Proactor; copying is disabled");
  ClientChannelService& operator=(const ClientChannelService&) =
      delete("ClientChannelService is pinned to its owner uring::Proactor; copying is disabled");
  ClientChannelService(ClientChannelService&&) =
      delete("ClientChannelService is pinned to its owner uring::Proactor; moving is disabled");
  ClientChannelService& operator=(ClientChannelService&&) =
      delete("ClientChannelService is pinned to its owner uring::Proactor; moving is disabled");
  [[nodiscard]] std::expected<void, Status> Start() noexcept;
  [[nodiscard]] std::expected<void, Status> ReconcileEndpoints(
      std::span<const cares::Endpoint> endpoints) noexcept;
  [[nodiscard]] std::expected<void, Status> ReResolve() noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget, MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp deadline) noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollShutdown(PollBudget,
                                                               MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;
  [[nodiscard]] ChannelState GetState() const noexcept;
  [[nodiscard]] std::string_view LastStatusMessage() const noexcept;
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(std::string_view path,
                                                              std::span<const std::byte> payload,
                                                              UnaryCompletion completion) noexcept;
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(std::string_view path,
                                                              std::span<const std::byte> payload,
                                                              void* context, Completion) noexcept;
  using MessageCompletion =
      InplaceDelegate<void(std::int32_t, std::span<const std::byte>) noexcept>;
  [[nodiscard]] std::expected<std::int32_t, Status> OpenStream(std::string_view, MessageCompletion,
                                                               UnaryCompletion) noexcept;
  [[nodiscard]] std::expected<void, Status> SendMessage(std::int32_t,
                                                        std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> HalfClose(std::int32_t) noexcept;
  [[nodiscard]] std::expected<void, Status> Cancel(std::int32_t) noexcept;
  [[nodiscard]] bool CanRoute(std::uint8_t) const noexcept;
  [[nodiscard]] uring::Result<void> Dispatch(const uring::Delivery&) noexcept;
  [[nodiscard]] ServiceState State() const noexcept;
  [[nodiscard]] bool Stopped() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

 private:
  std::uint64_t rejected_transitions_{};
  [[nodiscard]] Status RejectTransition() noexcept;
  uring::Proactor& proactor_;
  nexora::CompletionRoutes<>& routes_;
  std::uint64_t owner_{};
  [[nodiscard]] std::uint64_t CallerIdentity() const noexcept;
  bool advancing_{};
  [[nodiscard]] std::expected<PollResult, Status> Pump(PollBudget, MonotonicTimestamp,
                                                       PollResult result = {}) noexcept;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
static_assert(ServiceConcept<ClientChannelService>);
}  // namespace grpcxx
