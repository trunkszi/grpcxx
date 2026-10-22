#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "grpcxx/interceptor/call_types.h"

namespace grpcxx {

/**
 * One numeric address produced by a threadless c-ares lookup.
 *
 * `numeric` is a NUL-terminated presentation string. `Numeric()` views the
 * bytes before the terminator.
 */
struct ResolvedEndpoint {
  int family{0};
  std::uint16_t port{0};
  std::array<char, 64> numeric{};
  std::uint8_t numeric_length{0};

  /** Views the presentation form stored in `numeric`. */
  [[nodiscard]] std::string_view Numeric() const noexcept {
    return std::string_view{numeric.data(), numeric_length};
  }
};

/**
 * Completion function for `AresChannel::Resolve`.
 *
 * The endpoint span is valid only for the duration of the call. `status` is
 * `kOk` when at least one address was produced.
 */
using ResolveCallback = void (*)(void* user, Status status,
                                 std::span<const ResolvedEndpoint> endpoints) noexcept;

/** Readiness of one c-ares socket, as reported by the socket-state callback. */
struct SocketInterest {
  int fd{-1};
  bool readable{false};
  bool writable{false};
};

/**
 * Move-only RAII wrapper around a threadless `ares_channel_t`.
 *
 * The channel is created with `ARES_OPT_SOCK_STATE_CB` and never with
 * `ARES_OPT_EVENT_THREAD`. Socket readiness is recorded here so `DnsService`
 * can drive `ares_process_fd` and `ares_timeout` from `Process` without
 * blocking or spawning a thread. The c-ares header stays inside the
 * implementation file.
 */
class AresChannel final {
 public:
  /** Creates a closed channel. */
  AresChannel() noexcept;

  /** Cancels outstanding lookups and destroys the c-ares channel. */
  ~AresChannel();

  AresChannel(const AresChannel&) = delete;
  AresChannel& operator=(const AresChannel&) = delete;
  AresChannel(AresChannel&&) noexcept;
  AresChannel& operator=(AresChannel&&) noexcept;

  /**
   * Initializes the threadless c-ares channel.
   *
   * Returns `kUnavailable` when library or channel initialization fails.
   * A second successful open without `Close` returns `kFailedPrecondition`.
   */
  [[nodiscard]] std::expected<void, Status> Open() noexcept;

  /** Cancels lookups and releases the c-ares channel. Safe on a closed channel. */
  void Close() noexcept;

  /**
   * Starts a host lookup for `host` and numeric `port`.
   *
   * `callback` may run before this function returns when c-ares answers from
   * the numeric parser or the hosts file. The channel must be open.
   */
  [[nodiscard]] std::expected<void, Status> Resolve(std::string_view host, std::uint16_t port,
                                                    ResolveCallback callback, void* user) noexcept;

  /**
   * Drives ready sockets and an expired c-ares timeout.
   *
   * `budget.socket_events` limits sockets polled. `budget.completions`
   * limits completed lookups. `budget.timers` limits timeout processing.
   * `now` is the caller's monotonic clock and is the only time base used to
   * decide that a previously armed timeout is due. Socket readiness uses a
   * zero timeout poll. The returned `PollResult` reports remaining work and
   * the next c-ares timeout.
   */
  [[nodiscard]] std::expected<PollResult, Status> Process(PollBudget budget,
                                                          MonotonicTimestamp now) noexcept;

  /** Reports whether a lookup is still outstanding. */
  [[nodiscard]] bool HasPending() const noexcept;

  /** Returns the number of lookups that have not completed. */
  [[nodiscard]] std::size_t PendingCount() const noexcept;

  /** Reports how many c-ares sockets are currently tracked. */
  [[nodiscard]] std::size_t SocketCount() const noexcept;

  /** Views the sockets the socket-state callback is tracking. */
  [[nodiscard]] std::span<const SocketInterest> Sockets() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace grpcxx
