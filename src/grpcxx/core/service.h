#pragma once

#include <concepts>
#include <cstdint>
#include <expected>
#include <optional>

#include "grpcxx/core/status.h"

namespace grpcxx {

/** Nanoseconds on a monotonic clock; callers advance it once per reactor tick. */
using MonotonicTimestamp = std::uint64_t;

/** Lifecycle state shared by every service composed onto a proactor loop. */
enum class ServiceState : std::uint8_t {
  kStopped = 0,
  kStarting = 1,
  kRunning = 2,
  kStopping = 3,
  kFailed = 4,
};

/**
 * Upper bound on the work one `PollOnce` call may perform.
 *
 * A budget is enforced per resource so a single tick cannot starve the caller's other
 * services. Every field must be positive; `Valid()` rejects a zero budget.
 */
struct PollBudget {
  std::uint32_t commands{32U};
  std::uint32_t socket_events{64U};
  std::uint32_t completions{64U};
  std::uint32_t frames{64U};
  std::uint32_t timers{32U};

  /** Reports whether every resource budget is positive. */
  [[nodiscard]] constexpr bool Valid() const noexcept {
    return commands > 0U && socket_events > 0U && completions > 0U && frames > 0U && timers > 0U;
  }
};

/**
 * Work actually completed by one `PollOnce` call, plus what the caller must schedule next.
 *
 * Counters report how much of each budget was consumed. `pending` tells the caller that the
 * service still has work ready, so the caller should poll it again before waiting. `deadline`
 * carries the earliest timer the service wants to be woken at.
 */
struct PollResult {
  std::uint32_t commands{0U};
  std::uint32_t socket_events{0U};
  std::uint32_t completions{0U};
  std::uint32_t frames{0U};
  std::uint32_t timers{0U};
  bool pending{false};
  std::optional<MonotonicTimestamp> deadline{};
};

/**
 * Accepted `PollShutdown` result shapes.
 *
 * A service may report quiescence as a boolean or return a `PollResult` describing the work
 * drained by the call. Both shapes keep the caller-driven drain contract.
 */
template <typename T>
concept PollShutdownResult = std::same_as<T, std::expected<bool, Status>> ||
                             std::same_as<T, std::expected<PollResult, Status>>;

/**
 * Caller-driven lifecycle contract implemented by every runtime unit.
 *
 * A service never owns a thread and never blocks: the caller composes services onto one loop
 * and drives them with `PollOnce`. The lifecycle is explicit:
 *
 * - `Start` moves kStopped to kRunning (or kFailed on error).
 * - `PollOnce` performs at most `budget` units of work and reports progress.
 * - `BeginShutdown` moves kRunning to kStopping and starts draining.
 * - `PollShutdown` drains until quiescent, returning `true` when nothing is left or a
 *   `PollResult` describing the drained work, depending on the service's result shape.
 * - `Shutdown` releases remaining resources and moves to kStopped.
 * - `State` and `Stopped` expose the current lifecycle state.
 *
 * Implementations must mark every member `[[nodiscard]]` where a return value exists (the
 * concept cannot enforce the attribute) and must declare every member `noexcept`; the concept
 * rejects signatures that can throw.
 */
template <typename S>
concept ServiceConcept = requires(S& service, PollBudget budget, MonotonicTimestamp now) {
  { service.Start() } noexcept -> std::same_as<std::expected<void, Status>>;
  { service.State() } noexcept -> std::same_as<ServiceState>;
  { service.PollOnce(budget, now) } noexcept -> std::same_as<std::expected<PollResult, Status>>;
  { service.BeginShutdown(now) } noexcept -> std::same_as<std::expected<void, Status>>;
  { service.PollShutdown(budget, now) } noexcept -> PollShutdownResult;
  { service.Stopped() } noexcept -> std::same_as<bool>;
  { service.Shutdown() } noexcept -> std::same_as<std::expected<void, Status>>;
};

}  // namespace grpcxx
