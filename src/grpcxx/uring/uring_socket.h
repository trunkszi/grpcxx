#pragma once

#include <expected>

#include "grpcxx/uring/proactor_service.h"

namespace grpcxx {

/** A descriptor may be closed only after its proactor operations have drained. */
class UringSocket {
 public:
  UringSocket() noexcept = default;
  ~UringSocket();
  UringSocket(UringSocket&& other) noexcept;
  auto operator=(UringSocket&& other) noexcept -> UringSocket&;
  UringSocket(const UringSocket&) = delete;
  auto operator=(const UringSocket&) -> UringSocket& = delete;

  [[nodiscard]] static auto CreateTcp() noexcept -> std::expected<UringSocket, Status>;
  [[nodiscard]] static auto CreateTcp(int family) noexcept -> std::expected<UringSocket, Status>;
  [[nodiscard]] static auto CreateUnix() noexcept -> std::expected<UringSocket, Status>;
  /** Consumes fd even when configuration fails. */
  [[nodiscard]] static auto Adopt(int fd) noexcept -> std::expected<UringSocket, Status>;
  [[nodiscard]] auto Connect(ProactorService& proactor, std::span<const std::byte> address,
                             ProactorService::CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto Recv(ProactorService& proactor, std::span<std::byte> buffer,
                          ProactorService::CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto Send(ProactorService& proactor, std::span<const std::byte> buffer,
                          ProactorService::CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto PollAdd(ProactorService& proactor, unsigned events,
                             ProactorService::CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto Cancel(ProactorService& proactor,
                            OperationId operation) noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Fd() const noexcept -> int { return fd_; }
  [[nodiscard]] auto Release() noexcept -> int;
  void Close() noexcept;

 private:
  explicit UringSocket(int fd) noexcept : fd_(fd) {}
  int fd_{-1};
};

}  // namespace grpcxx
