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
/** Runs bounded HTTP/2 unary and streaming connections on a caller-owned uring::Proactor. */
class GrpcServerService final {
 public:
  static constexpr std::size_t kMaxConnections = 64;
  using RequestHandler = void (*)(void*, class GrpcServerService&, std::size_t, std::int32_t,
                                  std::uint64_t, std::string_view,
                                  std::span<const std::byte>) noexcept;
  /** An empty end callback with IsCurrent() false reports cancelled ingress. */
  using StreamHandler = void (*)(void*, GrpcServerService&, std::size_t, std::int32_t,
                                 std::uint64_t, std::string_view, std::span<const std::byte>,
                                 bool) noexcept;
  struct Options {
    cares::Endpoint endpoint{cares::IpAddress::FromV4({127, 0, 0, 1}), 0};
    const nexora::TlsServerConfig* tls{};
    void* context{};
    RequestHandler handler{};
    StreamHandler stream_handler{};
    std::uint64_t handshake_timeout_ns{5'000'000'000ULL};
    std::uint64_t peer_eof_drain_timeout_ns{5'000'000'000ULL};
    std::uint64_t idle_timeout_ns{60'000'000'000ULL};
  };
  GrpcServerService(uring::Proactor&, nexora::CompletionRoutes<>&, Options) noexcept;
  ~GrpcServerService();
  GrpcServerService(const GrpcServerService&) =
      delete("GrpcServerService is pinned to its owner uring::Proactor; copying is disabled");
  GrpcServerService& operator=(const GrpcServerService&) =
      delete("GrpcServerService is pinned to its owner uring::Proactor; copying is disabled");
  GrpcServerService(GrpcServerService&&) =
      delete("GrpcServerService is pinned to its owner uring::Proactor; moving is disabled");
  GrpcServerService& operator=(GrpcServerService&&) =
      delete("GrpcServerService is pinned to its owner uring::Proactor; moving is disabled");
  [[nodiscard]] std::expected<void, Status> Start() noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget, MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp deadline) noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollShutdown(PollBudget,
                                                               MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;
  [[nodiscard]] std::expected<cares::Endpoint, Status> LocalAddress() const noexcept;
  /** Connection/stream/generation identify one live request, including deferred replies. */
  [[nodiscard]] bool IsCurrent(std::size_t connection, std::int32_t stream,
                               std::uint64_t generation) const noexcept;
  [[nodiscard]] std::string_view StreamScheme(std::size_t connection, std::int32_t stream,
                                              std::uint64_t generation) const noexcept;
  [[nodiscard]] std::string_view StreamAuthority(std::size_t connection, std::int32_t stream,
                                                 std::uint64_t generation) const noexcept;
  [[nodiscard]] std::uint64_t StreamTimeoutNs(std::size_t connection, std::int32_t stream,
                                              std::uint64_t generation) const noexcept;
  [[nodiscard]] std::expected<void, Status> Complete(std::size_t connection, std::int32_t stream,
                                                     std::uint64_t generation,
                                                     std::span<const std::byte> payload,
                                                     int grpc_status = 0,
                                                     std::string_view grpc_message = {}) noexcept;
  [[nodiscard]] std::expected<void, Status> SendMessage(std::size_t, std::int32_t, std::uint64_t,
                                                        std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> Finish(std::size_t, std::int32_t, std::uint64_t,
                                                   int status = 0,
                                                   std::string_view grpc_message = {}) noexcept;
  [[nodiscard]] bool CanRoute(std::uint8_t) const noexcept;
  [[nodiscard]] uring::Result<void> Dispatch(const uring::Delivery&) noexcept;
  [[nodiscard]] ServiceState State() const noexcept;
  [[nodiscard]] std::size_t ActiveConnections() const noexcept;
  [[nodiscard]] bool Stopped() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

 private:
  enum class Lifecycle : std::uint8_t { kCreated, kRunning, kDraining, kQuiescent, kClosed };
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
static_assert(ServiceConcept<GrpcServerService>);
}  // namespace grpcxx
