#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

#include "grpcxx/interceptor/call_types.h"

namespace grpcxx {

/** Connectivity phase of one client channel. */
enum class ChannelState : std::uint8_t {
  kIdle,
  kResolving,
  kConnecting,
  kTlsHandshaking,
  kReady,
  kTransientFailure,
  kShuttingDown,
};

/**
 * Discrete input to `ChannelConnectivityFsm`.
 *
 * Time-based inputs are `kBackoffElapsed`, `kPingTimeout`, and the implicit
 * connect timeout emitted by `OnTime`.
 */
enum class ChannelEvent : std::uint8_t {
  kStart,
  kResolutionSucceeded,
  kResolutionFailed,
  kTcpConnected,
  kConnectFailed,
  kTlsRequired,
  kTlsSucceeded,
  kTlsFailed,
  kTransportFailed,
  kBackoffElapsed,
  kPingSent,
  kPingAcked,
  kPingTimeout,
  kShutdown,
};

/**
 * Caller-owned timers for reconnect backoff, connect, and HTTP/2 PING.
 *
 * Delays are nanoseconds on the same clock as `MonotonicTimestamp`. Backoff
 * grows by `backoff_numerator / backoff_denominator` per failure and receives
 * a deterministic jitter of +/- `jitter_permille` per mille. The library never
 * reads a clock itself.
 */
struct ChannelTiming {
  std::int64_t initial_backoff_nanos{100'000'000};
  std::int64_t max_backoff_nanos{30'000'000'000};
  std::int64_t keepalive_timeout_nanos{20'000'000'000};
  std::int64_t connect_timeout_nanos{20'000'000'000};
  std::uint32_t backoff_numerator{8};
  std::uint32_t backoff_denominator{5};
  std::uint32_t jitter_permille{200};
};

/**
 * Single-threaded connectivity state machine.
 *
 * No heap, lock, or thread. `OnEvent` and `OnTime` are the only mutators.
 * Unexpected events leave `state_` unchanged and increment
 * `rejected_transitions_`. A repeated `kShutdown` while already shutting
 * down is accepted and does not increment that counter.
 */
class ChannelConnectivityFsm final {
 public:
  /** Starts in `kIdle` with `timing` copied by value. */
  explicit ChannelConnectivityFsm(ChannelTiming timing = {}) noexcept;

  /** Current connectivity phase. */
  [[nodiscard]] ChannelState State() const noexcept;

  /** Events that were ignored because they are illegal in `State()`. */
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;

  /**
   * Consecutive transient failures since the last `kReady` entry.
   *
   * Also the next backoff attempt. The first failure uses attempt 0
   * (`initial_backoff_nanos`). A successful transition to `kReady` resets it.
   */
  [[nodiscard]] std::uint32_t FailureCount() const noexcept;

  /** Times a ready channel left `kReady` because a PING was not acked. */
  [[nodiscard]] std::uint32_t KeepaliveTimeouts() const noexcept;

  /** True while a reconnect wait is armed. */
  [[nodiscard]] bool BackoffArmed() const noexcept;

  /** True while a PING ack is outstanding. */
  [[nodiscard]] bool PingArmed() const noexcept;

  /** True while a TCP connect deadline is armed. */
  [[nodiscard]] bool ConnectArmed() const noexcept;

  /** Deadline of the armed reconnect wait, or nullopt. */
  [[nodiscard]] std::optional<MonotonicTimestamp> BackoffDeadline() const noexcept;

  /** Deadline of the outstanding PING, or nullopt. */
  [[nodiscard]] std::optional<MonotonicTimestamp> PingDeadline() const noexcept;

  /** Deadline of the TCP connect attempt, or nullopt. */
  [[nodiscard]] std::optional<MonotonicTimestamp> ConnectDeadline() const noexcept;

  /**
   * Writes a one-line status into `buffer` and NUL-terminates it.
   *
   * Returns the number of characters stored, excluding the terminator.
   * A buffer shorter than two bytes is left untouched and yields 0.
   */
  [[nodiscard]] std::size_t Describe(std::span<char> buffer) const noexcept;

  /**
   * Applies `event` at caller time `now`.
   *
   * Returns false when the event is illegal in the current state.
   * `kBackoffElapsed`, `kPingTimeout`, and the internal connect timeout are
   * legal only at or after the deadline armed for that wait.
   */
  [[nodiscard]] bool OnEvent(ChannelEvent event, MonotonicTimestamp now) noexcept;

  /**
   * Fires an armed deadline.
   *
   * In `kConnecting`, `now >=` the connect deadline applies `kConnectFailed`.
   * In `kTransientFailure`, `now >=` the backoff deadline applies
   * `kBackoffElapsed` and returns to `kResolving`. In `kReady`, `now >=`
   * the PING deadline applies `kPingTimeout`, records a keepalive timeout,
   * and enters `kTransientFailure`. Other states do nothing.
   */
  [[nodiscard]] bool OnTime(MonotonicTimestamp now) noexcept;

  /**
   * `min(max_nanos, initial_nanos * (numerator / denominator)^attempt)`,
   * with deterministic jitter.
   *
   * Attempt 0 returns the jittered initial value when it is inside the cap.
   * Non-positive delays, a zero denominator, or a non-positive cap yield 0.
   */
  [[nodiscard]] static std::int64_t BackoffDelay(const ChannelTiming& timing,
                                                 std::uint32_t attempt) noexcept;

  /** Stable name for logs and tests. */
  [[nodiscard]] static constexpr std::string_view StateName(ChannelState state) noexcept {
    switch (state) {
      case ChannelState::kIdle:
        return "idle";
      case ChannelState::kResolving:
        return "resolving";
      case ChannelState::kConnecting:
        return "connecting";
      case ChannelState::kTlsHandshaking:
        return "tls_handshaking";
      case ChannelState::kReady:
        return "ready";
      case ChannelState::kTransientFailure:
        return "transient_failure";
      case ChannelState::kShuttingDown:
        return "shutting_down";
    }
    std::unreachable();
  }

 private:
  [[nodiscard]] bool OnIdle(ChannelEvent event) noexcept;
  [[nodiscard]] bool OnResolving(ChannelEvent event, MonotonicTimestamp now) noexcept;
  [[nodiscard]] bool OnConnecting(ChannelEvent event, MonotonicTimestamp now) noexcept;
  [[nodiscard]] bool OnTlsHandshaking(ChannelEvent event, MonotonicTimestamp now) noexcept;
  [[nodiscard]] bool OnReady(ChannelEvent event, MonotonicTimestamp now) noexcept;
  [[nodiscard]] bool OnTransientFailure(ChannelEvent event, MonotonicTimestamp now) noexcept;
  [[nodiscard]] bool OnShuttingDown(ChannelEvent event) noexcept;

  void EnterReady() noexcept;
  void EnterTransientFailure(MonotonicTimestamp now, bool keepalive_timeout) noexcept;
  void EnterShuttingDown() noexcept;
  void Reject() noexcept;

  [[nodiscard]] static constexpr MonotonicTimestamp SaturatingAdd(MonotonicTimestamp now,
                                                                  std::int64_t delay) noexcept {
    if (delay <= 0) {
      return now;
    }
    constexpr auto kUintMax = std::numeric_limits<std::uint64_t>::max();
    const auto amount = static_cast<std::uint64_t>(delay);
    if (now > kUintMax - amount) {
      return kUintMax;
    }
    return now + amount;
  }

  [[nodiscard]] static constexpr std::int64_t SaturatingMulDiv(std::int64_t value,
                                                               std::uint32_t numerator,
                                                               std::uint32_t denominator) noexcept {
    constexpr auto kIntMax = std::numeric_limits<std::int64_t>::max();
    if (value <= 0 || numerator == 0 || denominator == 0) {
      return 0;
    }
    if (value > kIntMax / numerator) {
      return kIntMax;
    }
    return (value * numerator) / denominator;
  }

  ChannelTiming timing_{};
  ChannelState state_{ChannelState::kIdle};
  std::uint64_t rejected_transitions_{0};
  std::uint32_t failure_count_{0};
  std::uint32_t keepalive_timeouts_{0};
  bool backoff_armed_{false};
  bool ping_armed_{false};
  bool connect_armed_{false};
  MonotonicTimestamp backoff_deadline_{0};
  MonotonicTimestamp ping_deadline_{0};
  MonotonicTimestamp connect_deadline_{0};
};

}  // namespace grpcxx
