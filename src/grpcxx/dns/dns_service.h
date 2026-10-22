#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

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
class DnsService final {
 public:
  /** Constructs a service in `ServiceState::kStopped`. */
  DnsService() noexcept;
  ~DnsService();

  DnsService(const DnsService&) = delete;
  DnsService& operator=(const DnsService&) = delete;
  DnsService(DnsService&&) = delete;
  DnsService& operator=(DnsService&&) = delete;

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

  AresChannel channel_{};
  ServiceState state_{ServiceState::kStopped};
  bool quiescent_{false};
};

static_assert(ServiceConcept<DnsService>);

}  // namespace grpcxx
