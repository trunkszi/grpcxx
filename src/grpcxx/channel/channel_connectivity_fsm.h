#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

#include <uarch/uarch.hpp>

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
 * Packed connectivity phase and reconnect bookkeeping.
 *
 * Flags sit in bits 0..=2 so a test is an imm32 mask. `failure_count` occupies
 * the top 32 bits so a successful attempt can advance it with one add.
 */
struct ChannelFsmMetaSpec {
  using Word = std::uint64_t;
  bool backoff_armed[[= uarch::bits::Flag{0}]];
  bool ping_armed[[= uarch::bits::Flag{1}]];
  bool connect_armed[[= uarch::bits::Flag{2}]];
  ChannelState state[[= uarch::bits::Field{8, 8}]];
  std::uint32_t failure_count[[= uarch::bits::Field{32, 32}]];
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
    return uarch::arithmetic::saturating_add(now, static_cast<MonotonicTimestamp>(delay));
  }

  /** Divisor for `timing.backoff_denominator`, or a dummy 1 when that value is 0. */
  [[nodiscard]] static uarch::FastDivisorU64 MakeBackoffDivisor(
      const ChannelTiming& timing) noexcept;

  /** Divisor for `2 * min(jitter_permille, 1000) + 1`, which is never zero. */
  [[nodiscard]] static uarch::FastDivisorU32 MakeJitterDivisor(
      const ChannelTiming& timing) noexcept;

  /**
   * Backoff using divisors frozen for `timing`.
   *
   * The quotients are magic multiplies. Callers that still have a raw
   * `ChannelTiming` build the divisors once before entering this function.
   */
  [[nodiscard]] static std::int64_t ApplyBackoff(const ChannelTiming& timing, std::uint32_t attempt,
                                                 const uarch::FastDivisorU64& backoff,
                                                 const uarch::FastDivisorU32& jitter_span) noexcept;

  ChannelTiming timing_{};
  uarch::FastDivisorU64 backoff_div_;
  uarch::FastDivisorU32 jitter_div_;
  uarch::BitWord<ChannelFsmMetaSpec> meta_{};
  std::uint64_t rejected_transitions_{0};
  std::uint32_t keepalive_timeouts_{0};
  MonotonicTimestamp backoff_deadline_{0};
  MonotonicTimestamp ping_deadline_{0};
  MonotonicTimestamp connect_deadline_{0};
};

inline ChannelConnectivityFsm::ChannelConnectivityFsm(ChannelTiming timing) noexcept
    : timing_{timing},
      backoff_div_{MakeBackoffDivisor(timing)},
      jitter_div_{MakeJitterDivisor(timing)} {}

inline uarch::FastDivisorU64 ChannelConnectivityFsm::MakeBackoffDivisor(
    const ChannelTiming& timing) noexcept {
  if (timing.backoff_denominator == 0U) {
    return uarch::arithmetic::divisor_u64<1>();
  }
  return *uarch::FastDivisorU64::try_new(timing.backoff_denominator);
}

inline uarch::FastDivisorU32 ChannelConnectivityFsm::MakeJitterDivisor(
    const ChannelTiming& timing) noexcept {
  const auto bounded = timing.jitter_permille < 1000U ? timing.jitter_permille : 1000U;
  const auto span = 2U * bounded + 1U;
  return *uarch::FastDivisorU32::try_new(span);
}

inline std::int64_t ChannelConnectivityFsm::BackoffDelay(const ChannelTiming& timing,
                                                         std::uint32_t attempt) noexcept {
  if (timing.backoff_denominator == 0U) {
    return 0;
  }
  return ApplyBackoff(timing, attempt, MakeBackoffDivisor(timing), MakeJitterDivisor(timing));
}

static_assert(uarch::NoPadding<uarch::BitWord<ChannelFsmMetaSpec>> &&
              uarch::RegisterPassable<uarch::BitWord<ChannelFsmMetaSpec>>);

}  // namespace grpcxx
