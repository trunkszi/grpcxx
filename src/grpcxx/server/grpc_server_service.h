#pragma once
#include <grpcxx/uring/proactor_service.h>

#include <cstddef>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

namespace grpcxx {
/** Runs bounded HTTP/2 unary connections on a caller-owned ProactorService. */
class GrpcServerService final {
 public:
  using RequestHandler = void (*)(void*, class GrpcServerService&, std::size_t, std::int32_t,
                                  std::uint64_t, std::string_view,
                                  std::span<const std::byte>) noexcept;
  struct Options {
    std::span<const std::byte> address;
    void* context{};
    RequestHandler handler{};
  };
  GrpcServerService(ProactorService& proactor, Options options) noexcept;
  ~GrpcServerService();
  GrpcServerService(const GrpcServerService&) = delete;
  GrpcServerService& operator=(const GrpcServerService&) = delete;
  GrpcServerService(GrpcServerService&&) = delete;
  GrpcServerService& operator=(GrpcServerService&&) = delete;
  [[nodiscard]] std::expected<void, Status> Start() noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget, MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp deadline) noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollShutdown(PollBudget,
                                                               MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;
  [[nodiscard]] std::expected<std::size_t, Status> LocalAddress(
      std::span<std::byte>) const noexcept;
  /** Connection/stream/generation identify one live request, including deferred replies. */
  [[nodiscard]] bool IsCurrent(std::size_t connection, std::int32_t stream,
                               std::uint64_t generation) const noexcept;
  [[nodiscard]] std::expected<void, Status> Complete(std::size_t connection, std::int32_t stream,
                                                     std::uint64_t generation,
                                                     std::span<const std::byte> payload,
                                                     int grpc_status = 0) noexcept;
  [[nodiscard]] ServiceState State() const noexcept;
  [[nodiscard]] bool Stopped() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

 private:
  enum class Lifecycle { kCreated, kRunning, kDraining, kQuiescent, kClosed };
  [[nodiscard]] bool Transition(Lifecycle next) noexcept;
  std::uint64_t rejected_transitions_{};
  [[nodiscard]] Status RejectTransition() noexcept;
  ProactorService& proactor_;
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
