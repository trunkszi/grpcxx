#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "grpcxx/core/status.h"

namespace grpcxx {

/** Owns kernel ring resources. All methods are confined to the caller thread. */
class UringRing {
 public:
  enum class Opcode { kAccept, kConnect, kRecv, kSend, kPoll, kCancel };
  struct Request {
    Opcode opcode{};
    int fd{-1};
    std::span<std::byte> receive{};
    std::span<const std::byte> transmit{};
    std::uint64_t token{};
    std::uint64_t cancel_token{};
    unsigned poll_events{};
    std::int64_t timeout_ns{-1};
    std::uint32_t timeout_slot{};
  };
  struct Completion {
    std::uint64_t token{};
    int result{};
  };

  UringRing() noexcept;
  ~UringRing();
  UringRing(UringRing&&) noexcept;
  UringRing& operator=(UringRing&&) noexcept;
  UringRing(const UringRing&) = delete;
  UringRing& operator=(const UringRing&) = delete;

  [[nodiscard]] auto Open(unsigned entries,
                          unsigned timeout_slots) noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Queue(const Request& request) noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Submit() noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Peek(std::span<Completion> output) noexcept -> std::size_t;
  void Close() noexcept;
  [[nodiscard]] auto IsOpen() const noexcept -> bool;

  static constexpr std::uint64_t kTimeoutTag = std::uint64_t{1} << 63;
  static constexpr std::uint64_t kCancelTag = std::uint64_t{1} << 62;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace grpcxx
