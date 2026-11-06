#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include <uarch/uarch.hpp>

#include "grpcxx/dns/ares_channel.h"

namespace grpcxx {

/**
 * Caller-driven DNS service over a threadless c-ares channel.
 *
 * This is a single-thread service: every method is called by the same caller
 * that drives the reactor. Lifecycle state, not a thread identity, decides
 * whether an operation is legal. No method blocks or creates a thread. Copy
 * and move are disabled so the channel cannot be shared accidentally.
 */
/**
 * Lifecycle flag and phase of the caller-driven DNS service.
 *
 * A zero word is `kStopped` and not quiescent, which is the constructed state.
 */
struct DnsServiceMetaSpec {
  using Word = std::uint64_t;
  bool quiescent[[= uarch::bits::Flag{0}]];
  ServiceState state[[= uarch::bits::Field{8, 8}]];
};

class DnsService final {
 public:
  /** Constructs a service in `ServiceState::kStopped`. */
  DnsService() noexcept;
  ~DnsService();

  DnsService(const DnsService&) = delete("dns service owns the c-ares channel");
  DnsService& operator=(const DnsService&) = delete("dns service owns the c-ares channel");
  DnsService(DnsService&&) = delete("dns service owns the c-ares channel");
  DnsService& operator=(DnsService&&) = delete("dns service owns the c-ares channel");

  /**
   * Opens the c-ares channel and enters `kRunning`.
   *
   * A start while running, starting, or stopping returns
   * `kFailedPrecondition`.
   */
  [[nodiscard]] std::expected<void, Status> Start() noexcept;

  /** Returns the lifecycle phase. */
  [[nodiscard]] ServiceState State() const noexcept;

  /**
   * Advances DNS socket and timeout work within `budget`.
   *
   * Valid only while running.
   */
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept;

  /**
   * Stops new lookups and cancels outstanding ones.
   *
   * The service enters `kStopping`.
   */
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp now) noexcept;

  /**
   * Drives cancellation to completion.
   *
   * Returns true once no lookup remains. `Stopped()` is then true.
   */
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget budget,
                                                         MonotonicTimestamp now) noexcept;

  /** Reports whether shutdown has drained outstanding lookups. */
  [[nodiscard]] bool Stopped() const noexcept;

  /**
   * Releases the c-ares channel.
   *
   * Idempotent after the service has stopped. Also succeeds from `kStopped`.
   * A running service must drain first.
   */
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;

  /**
   * Submits `host` for resolution on the open channel.
   *
   * The lookup is rejected unless the service is running.
   */
  [[nodiscard]] std::expected<void, Status> Resolve(std::string_view host, std::uint16_t port,
                                                    ResolveCallback callback, void* user) noexcept;

  /** Reports lookups that have not completed. */
  [[nodiscard]] std::size_t PendingResolutions() const noexcept;

 private:
  [[nodiscard]] std::expected<void, Status> RequireRunning() const noexcept;
  [[nodiscard]] ServiceState Lifecycle() const noexcept;
  void SetLifecycle(ServiceState state) noexcept;
  [[nodiscard]] bool Quiescent() const noexcept;
  void SetQuiescent(bool quiescent) noexcept;

  AresChannel channel_{};
  uarch::BitWord<DnsServiceMetaSpec> life_{};
};

static_assert(ServiceConcept<DnsService>);
static_assert(uarch::NoPadding<uarch::BitWord<DnsServiceMetaSpec>>);

}  // namespace grpcxx
