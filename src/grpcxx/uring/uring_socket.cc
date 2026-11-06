#include "grpcxx/uring/uring_socket.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <utility>

namespace grpcxx {
namespace {
auto SocketError() noexcept -> Status { return Status{StatusCode::kUnavailable}; }
}  // namespace

UringSocket::~UringSocket() { Close(); }
UringSocket::UringSocket(UringSocket&& other) noexcept : fd_(other.Release()) {}
auto UringSocket::operator=(UringSocket&& other) noexcept -> UringSocket& {
  if (this != &other) {
    Close();
    fd_ = other.Release();
  }
  return *this;
}

auto UringSocket::CreateTcp() noexcept -> std::expected<UringSocket, Status> {
  return CreateTcp(AF_INET);
}

auto UringSocket::CreateTcp(int family) noexcept -> std::expected<UringSocket, Status> {
  if (family != AF_INET && family != AF_INET6) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "TCP requires IPv4 or IPv6"});
  }
  return Adopt(::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP));
}

auto UringSocket::CreateUnix() noexcept -> std::expected<UringSocket, Status> {
  return Adopt(::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
}

auto UringSocket::Adopt(int fd) noexcept -> std::expected<UringSocket, Status> {
  if (fd < 0) {
    return std::unexpected(SocketError());
  }
  UringSocket socket{fd};
  const auto flags = ::fcntl(fd, F_GETFL);
  const auto descriptor_flags = ::fcntl(fd, F_GETFD);
  if (flags < 0 || descriptor_flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
      ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
    return std::unexpected(SocketError());
  }
  sockaddr_storage address{};
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
    return std::unexpected(SocketError());
  }
  if (address.ss_family == AF_INET || address.ss_family == AF_INET6) {
    const int enabled = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) < 0) {
      return std::unexpected(SocketError());
    }
  }
  return socket;
}

auto UringSocket::Connect(ProactorService& proactor, std::span<const std::byte> address,
                          ProactorService::CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return proactor.SubmitConnect(fd_, address, std::move(callback));
}
auto UringSocket::Recv(ProactorService& proactor, std::span<std::byte> buffer,
                       ProactorService::CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return proactor.SubmitRecv(fd_, buffer, std::move(callback));
}
auto UringSocket::Send(ProactorService& proactor, std::span<const std::byte> buffer,
                       ProactorService::CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return proactor.SubmitSend(fd_, buffer, std::move(callback));
}
auto UringSocket::PollAdd(ProactorService& proactor, unsigned events,
                          ProactorService::CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return proactor.SubmitPoll(fd_, events, std::move(callback));
}
auto UringSocket::Cancel(ProactorService& proactor,
                         OperationId operation) noexcept -> std::expected<void, Status> {
  return proactor.Cancel(operation);
}

auto UringSocket::Release() noexcept -> int { return std::exchange(fd_, -1); }
void UringSocket::Close() noexcept {
  if (fd_ >= 0) {
    /** Linux releases the descriptor even when close reports EINTR. */
    ::close(std::exchange(fd_, -1));
  }
}

}  // namespace grpcxx
