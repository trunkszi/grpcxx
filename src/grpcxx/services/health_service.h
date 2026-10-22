#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "grpcxx/interceptor/call_types.h"

namespace grpcxx {

/**
 * Serving status for `grpc.health.v1.Health`.
 *
 * Values match the `grpc.health.v1.HealthCheckResponse.ServingStatus` wire
 * enum. `kServiceUnknown` is produced only by `Watch` for a name that has
 * not been registered.
 */
enum class ServingStatus : std::uint8_t {
  kUnknown = 0,
  kServing = 1,
  kNotServing = 2,
  kServiceUnknown = 3,
};

/**
 * Non-blocking `grpc.health.v1.Health` service.
 *
 * `Check` is unary. An empty service name is the overall status and defaults
 * to `kServing`. A known service returns its status. An unknown service
 * returns `kNotFound`.
 *
 * `Watch` is server-streaming. It registers a watcher, queues the current
 * status (`kServiceUnknown` when the name is not registered), and queues a
 * new event on `SetServingStatus` for watchers of that name. `DrainWatch`
 * copies queued events into the caller-supplied span.
 *
 * The service is single-threaded: the caller drives `Start`, `PollOnce`,
 * `BeginShutdown`, `PollShutdown`, and `Shutdown`. No method blocks or
 * creates a thread.
 */
class HealthService final {
 public:
  /** Constructs a service in `ServiceState::kStopped` with overall `kServing`. */
  HealthService() noexcept;
  ~HealthService() = default;

  HealthService(const HealthService&) = delete;
  HealthService& operator=(const HealthService&) = delete;
  HealthService(HealthService&&) = delete;
  HealthService& operator=(HealthService&&) = delete;

  /**
   * Enters `kRunning`.
   *
   * A start while running, starting, or stopping returns
   * `kFailedPrecondition`.
   */
  [[nodiscard]] std::expected<void, Status> Start() noexcept;

  /** Returns the lifecycle phase. */
  [[nodiscard]] ServiceState State() const noexcept;

  /**
   * Records one caller tick.
   *
   * Health has no sockets. The call succeeds only while running and reports
   * an empty `PollResult`. `budget` and `now` come from the caller.
   */
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept;

  /**
   * Stops new checks and watches.
   *
   * The service enters `ServiceState::kStopping`. A second call is a no-op.
   */
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp now) noexcept;

  /**
   * Drops watchers and reports whether shutdown has drained.
   *
   * Returns true once nothing remains. `Stopped()` is then true.
   */
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget budget,
                                                         MonotonicTimestamp now) noexcept;

  /** Reports whether shutdown has drained or the service was shut down. */
  [[nodiscard]] bool Stopped() const noexcept;

  /**
   * Releases watchers and per-service status.
   *
   * Idempotent after the service has stopped. Also succeeds from `kStopped`.
   * A running service must drain first.
   */
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;

  /**
   * Unary `Check`.
   *
   * Empty `service` returns the overall status. Unknown names return
   * `kNotFound` rather than a serving status.
   */
  [[nodiscard]] std::expected<ServingStatus, Status> Check(std::string_view service) noexcept;

  /**
   * Registers a server-streaming `Watch`.
   *
   * The returned id is passed to `DrainWatch`. The initial status is queued
   * immediately.
   */
  [[nodiscard]] std::expected<std::uint32_t, Status> Watch(std::string_view service) noexcept;

  /**
   * Copies queued watch events into `out` and removes the copied events.
   *
   * Returns the number of statuses written. A short span copies a prefix and
   * leaves the rest queued. An unknown id returns `kNotFound`.
   */
  [[nodiscard]] std::expected<std::size_t, Status> DrainWatch(
      std::uint32_t watcher, std::span<ServingStatus> out) noexcept;

  /**
   * Sets the status published by `Check` and `Watch`.
   *
   * Empty `service` updates the overall status. Any other name is registered
   * or updated, then every matching watcher is queued the new status.
   */
  [[nodiscard]] std::expected<void, Status> SetServingStatus(std::string_view service,
                                                             ServingStatus status) noexcept;

 private:
  static constexpr std::size_t kMaxServices = 16;
  static constexpr std::size_t kMaxWatchers = 16;
  static constexpr std::size_t kNameCapacity = 96;
  static constexpr std::size_t kQueueCapacity = 8;

  struct NameStorage {
    std::array<char, kNameCapacity> chars{};
    std::uint8_t length{0};

    [[nodiscard]] std::string_view View() const noexcept {
      return std::string_view{chars.data(), length};
    }
  };

  struct ServiceRecord {
    NameStorage name{};
    ServingStatus status{ServingStatus::kUnknown};
    bool used{false};
  };

  struct Watcher {
    std::uint32_t id{0};
    NameStorage service{};
    std::array<ServingStatus, kQueueCapacity> events{};
    std::uint8_t head{0};
    std::uint8_t count{0};
    bool used{false};

    void Push(ServingStatus status) noexcept;
    [[nodiscard]] std::size_t Drain(std::span<ServingStatus> out) noexcept;
  };

  [[nodiscard]] std::expected<void, Status> RequireRunning() const noexcept;
  [[nodiscard]] ServiceRecord* FindService(std::string_view service) noexcept;
  void Notify(std::string_view service, ServingStatus status) noexcept;
  void ClearRuntime() noexcept;

  std::array<ServiceRecord, kMaxServices> services_{};
  std::uint16_t service_count_{0};
  std::array<Watcher, kMaxWatchers> watchers_{};
  ServingStatus overall_{ServingStatus::kServing};
  std::uint32_t next_watcher_id_{1};
  ServiceState state_{ServiceState::kStopped};
  bool quiescent_{false};
  MonotonicTimestamp last_poll_{0};
};

static_assert(ServiceConcept<HealthService>);

}  // namespace grpcxx
