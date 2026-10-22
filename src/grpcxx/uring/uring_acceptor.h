#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "grpcxx/uring/uring_socket.h"

namespace grpcxx {

class UringAcceptor {
 public:
  UringAcceptor() noexcept = default;
  ~UringAcceptor();
  UringAcceptor(UringAcceptor&&) noexcept;
  auto operator=(UringAcceptor&&) noexcept -> UringAcceptor&;
  UringAcceptor(const UringAcceptor&) = delete;
  auto operator=(const UringAcceptor&) -> UringAcceptor& = delete;

  [[nodiscard]] static auto Listen(std::span<const std::byte> address, int backlog = 128) noexcept
      -> std::expected<UringAcceptor, Status>;
  [[nodiscard]] auto Accept(ProactorService& proactor,
                            ProactorService::CompletionCallback callback) noexcept
      -> std::expected<OperationId, Status>;
  [[nodiscard]] auto Fd() const noexcept -> int { return socket_.Fd(); }
  [[nodiscard]] auto LocalAddress(std::span<std::byte> output) const noexcept
      -> std::expected<std::size_t, Status>;
  void Close() noexcept { socket_.Close(); }

 private:
  UringSocket socket_;
};

}  // namespace grpcxx
