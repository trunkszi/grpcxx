#pragma once
#include <grpcxx/uring/proactor_service.h>

#include <cstddef>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

namespace grpcxx {
/** Plaintext HTTP/2 channel; the caller drives this and its shared ProactorService. */
class ClientChannelService final {
 public:
  using UnaryCompletion =
      InplaceDelegate<void(std::int32_t, int, std::span<const std::byte>) noexcept>;
  using Completion = void (*)(void*, std::int32_t, int, std::span<const std::byte>) noexcept;
  enum class ChannelState { kIdle, kConnecting, kReady, kTransientFailure, kShuttingDown, kClosed };
  struct Options {
    std::span<const std::byte> address;
  };
  ClientChannelService(ProactorService&, Options) noexcept;
  ~ClientChannelService();
  ClientChannelService(const ClientChannelService&) = delete;
  ClientChannelService& operator=(const ClientChannelService&) = delete;
  ClientChannelService(ClientChannelService&&) = delete;
  ClientChannelService& operator=(ClientChannelService&&) = delete;
  [[nodiscard]] std::expected<void, Status> Start() noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget, MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp deadline) noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollShutdown(PollBudget,
                                                               MonotonicTimestamp) noexcept;
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;
  [[nodiscard]] ChannelState GetState() const noexcept;
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(std::string_view path,
                                                              std::span<const std::byte> payload,
                                                              UnaryCompletion completion) noexcept;
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(std::string_view path,
                                                              std::span<const std::byte> payload,
                                                              void* context, Completion) noexcept;
  [[nodiscard]] ServiceState State() const noexcept;
  [[nodiscard]] bool Stopped() const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

 private:
  [[nodiscard]] bool Transition(ChannelState next) noexcept;
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
static_assert(ServiceConcept<ClientChannelService>);
}  // namespace grpcxx
