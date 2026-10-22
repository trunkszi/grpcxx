#pragma once

#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/core/service.h"
#include "grpcxx/uring/uring_ring.h"

namespace grpcxx {

/** Selects a caller-identity provider at compile time. The provider must return
 * a positive identity and outlive every service using its proactor.
 */
template <typename Provider>
  requires requires(const Provider& provider) {
    { provider.Current() } noexcept -> std::same_as<std::uint64_t>;
  }
struct ThreadIdSeam {
  const Provider& provider;
  constexpr explicit ThreadIdSeam(const Provider& value) noexcept : provider(value) {}
  ThreadIdSeam(const Provider&&) = delete;
};

struct ProactorOptions {
  std::uint32_t ring_entries{256};
  std::uint32_t max_operations{256};
  std::uint32_t max_timers{128};
};

struct OperationId {
  std::uint64_t value{};
  auto operator==(const OperationId&) const noexcept -> bool = default;
};

/** All methods and callbacks execute on one caller thread. Borrowed I/O buffers
remain valid until completion, including cancellation. SEND may complete short.
Submission reserves a fixed slot; PollOnce emits at most budget.commands logical
operations or cancellations. SQ pressure retains staged work for later polls. */
class ProactorService final {
 public:
  using OperationId = grpcxx::OperationId;
  using CompletionCallback = InplaceDelegate<void(int) noexcept>;
  explicit ProactorService(ProactorOptions options = {}) noexcept;
  template <typename Provider>
  ProactorService(ProactorOptions options, ThreadIdSeam<Provider> seam) noexcept
      : ProactorService(options, &seam.provider, [](const void* context) noexcept {
          return static_cast<const Provider*>(context)->Current();
        }) {}
  /** Destruction requires the Start caller, no active polling, and all I/O,
   * timer callbacks, and cancellation completions drained through the lifecycle.
   */
  ~ProactorService();
  ProactorService(const ProactorService&) = delete;
  auto operator=(const ProactorService&) -> ProactorService& = delete;
  ProactorService(ProactorService&&) = delete;
  auto operator=(ProactorService&&) -> ProactorService& = delete;

  [[nodiscard]] auto Start() noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Start(ProactorOptions options) noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto PollOnce(PollBudget budget,
                              MonotonicTimestamp now) noexcept -> std::expected<PollResult, Status>;
  [[nodiscard]] auto BeginShutdown(MonotonicTimestamp deadline) noexcept
      -> std::expected<void, Status>;
  [[nodiscard]] auto PollShutdown(PollBudget budget, MonotonicTimestamp now) noexcept
      -> std::expected<PollResult, Status>;
  [[nodiscard]] auto Shutdown() noexcept -> std::expected<void, Status>;

  [[nodiscard]] auto SubmitAccept(int fd, CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto SubmitConnect(int fd, std::span<const std::byte> address,
                                   CompletionCallback callback,
                                   std::chrono::nanoseconds timeout = std::chrono::nanoseconds{
                                       -1}) noexcept -> std::expected<OperationId, Status>;
  [[nodiscard]] auto SubmitRecv(int fd, std::span<std::byte> buffer, CompletionCallback callback,
                                std::chrono::nanoseconds timeout = std::chrono::nanoseconds{
                                    -1}) noexcept -> std::expected<OperationId, Status>;
  [[nodiscard]] auto SubmitSend(int fd, std::span<const std::byte> buffer,
                                CompletionCallback callback,
                                std::chrono::nanoseconds timeout = std::chrono::nanoseconds{
                                    -1}) noexcept -> std::expected<OperationId, Status>;
  [[nodiscard]] auto SubmitPoll(int fd, unsigned events, CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto ScheduleTimer(MonotonicTimestamp deadline,
                                   CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto Cancel(OperationId operation) noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto State() const noexcept -> ServiceState;
  [[nodiscard]] auto Stopped() const noexcept -> bool;
  [[nodiscard]] auto RejectedTransitions() const noexcept -> std::uint64_t;
  [[nodiscard]] auto PendingOperations() const noexcept -> std::size_t;
  /** Returns this caller's identity; the default provider reads gettid(). */
  [[nodiscard]] auto CallerThreadId() const noexcept -> std::uint64_t;

 private:
  ProactorService(ProactorOptions options, const void* context,
                  std::uint64_t (*read_identity)(const void*) noexcept) noexcept;
  struct Impl;
  ProactorOptions options_;
  std::uint64_t owner_{};
  const void* identity_context_{};
  std::uint64_t (*read_identity_)(const void*) noexcept {};
  std::uint64_t rejected_transitions_{};
  std::unique_ptr<Impl> impl_;
  [[nodiscard]] auto RejectTransition() noexcept -> Status;
  [[nodiscard]] auto Queue(UringRing::Request request, CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
};

}  // namespace grpcxx
