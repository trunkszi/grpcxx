#include "grpcxx/uring/uring_acceptor.h"

#include <netinet/in.h>
#include <sys/un.h>

#include <cstring>
#include <limits>
#include <utility>

namespace grpcxx {

UringAcceptor::~UringAcceptor() = default;
UringAcceptor::UringAcceptor(UringAcceptor&&) noexcept = default;
auto UringAcceptor::operator=(UringAcceptor&&) noexcept -> UringAcceptor& = default;

auto UringAcceptor::Listen(std::span<const std::byte> address,
                           int backlog) noexcept -> std::expected<UringAcceptor, Status> {
  if (address.size() < sizeof(sa_family_t) || address.size() > sizeof(sockaddr_storage) ||
      backlog <= 0) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "invalid listen address or backlog"});
  }
  sockaddr_storage storage{};
  std::memcpy(&storage, address.data(), address.size());
  const auto family = storage.ss_family;
  if ((family == AF_INET && address.size() != sizeof(sockaddr_in)) ||
      (family == AF_INET6 && address.size() != sizeof(sockaddr_in6)) ||
      (family == AF_UNIX && address.size() <= offsetof(sockaddr_un, sun_path)) ||
      (family != AF_INET && family != AF_INET6 && family != AF_UNIX)) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "unsupported listen address"});
  }
  auto socket = family == AF_UNIX ? UringSocket::CreateUnix() : UringSocket::CreateTcp(family);
  if (!socket) {
    return std::unexpected(socket.error());
  }
  if (family == AF_INET || family == AF_INET6) {
    const int enabled = 1;
    if (::setsockopt(socket->Fd(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0 ||
        ::setsockopt(socket->Fd(), SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)) < 0) {
      return std::unexpected(
          Status{StatusCode::kUnavailable, "listener reuse configuration failed"});
    }
  }
  if (::bind(socket->Fd(), reinterpret_cast<const sockaddr*>(&storage),
             static_cast<socklen_t>(address.size())) < 0 ||
      ::listen(socket->Fd(), backlog) < 0) {
    return std::unexpected(Status{StatusCode::kUnavailable, "socket bind or listen failed"});
  }
  UringAcceptor acceptor;
  acceptor.socket_ = std::move(*socket);
  return acceptor;
}

auto UringAcceptor::Accept(ProactorService& proactor,
                           ProactorService::CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return proactor.SubmitAccept(Fd(), std::move(callback));
}

auto UringAcceptor::LocalAddress(std::span<std::byte> output) const noexcept
    -> std::expected<std::size_t, Status> {
  if (Fd() < 0 || output.size() > std::numeric_limits<socklen_t>::max()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "invalid address output"});
  }
  sockaddr_storage storage{};
  socklen_t size = sizeof(storage);
  if (::getsockname(Fd(), reinterpret_cast<sockaddr*>(&storage), &size) < 0) {
    return std::unexpected(Status{StatusCode::kUnavailable, "local address query failed"});
  }
  if (size > output.size()) {
    return std::unexpected(Status{StatusCode::kResourceExhausted, "address output is too small"});
  }
  std::memcpy(output.data(), &storage, size);
  return size;
}

}  // namespace grpcxx
