#include "grpcxx/uring/uring_ring.h"

#include <liburing.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <new>
#include <ranges>

namespace grpcxx {
namespace {
auto RingError(int error) noexcept -> Status {
  if (error == ENOMEM || error == EBUSY || error == ENOSPC || error == EAGAIN) {
    return Status{StatusCode::kResourceExhausted};
  }
  return Status{StatusCode::kUnavailable};
}
}  // namespace

struct UringRing::Impl {
  io_uring ring{};
  std::unique_ptr<__kernel_timespec[]> timeouts;
  unsigned timeout_slots{};
  bool open{};
  ~Impl() {
    if (open) {
      io_uring_queue_exit(&ring);
    }
  }
};

UringRing::UringRing() noexcept = default;
UringRing::~UringRing() = default;
UringRing::UringRing(UringRing&&) noexcept = default;
auto UringRing::operator=(UringRing&&) noexcept -> UringRing& = default;

auto UringRing::Open(unsigned entries,
                     unsigned timeout_slots) noexcept -> std::expected<void, Status> {
  if (impl_ || entries < 2 || timeout_slots == 0) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  auto impl = std::unique_ptr<Impl>{new (std::nothrow) Impl{}};
  if (!impl) {
    return std::unexpected(RingError(ENOMEM));
  }
  impl->timeouts.reset(new (std::nothrow) __kernel_timespec[timeout_slots]{});
  if (!impl->timeouts) {
    return std::unexpected(RingError(ENOMEM));
  }
  io_uring_params parameters{};
  /** SINGLE_ISSUER does not create an SQ polling kernel thread. */
  parameters.flags = IORING_SETUP_SINGLE_ISSUER;
  const auto result = io_uring_queue_init_params(entries, &impl->ring, &parameters);
  if (result < 0) {
    return std::unexpected(RingError(-result));
  }
  impl->open = true;
  /** Completion retention is required to release every borrowed operation buffer safely. */
  if ((parameters.features & IORING_FEAT_NODROP) == 0) {
    return std::unexpected(Status{StatusCode::kUnavailable});
  }
  impl->timeout_slots = timeout_slots;
  impl_ = std::move(impl);
  return {};
}

auto UringRing::Queue(const Request& request) noexcept -> std::expected<void, Status> {
  if (!impl_) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  const bool linked = request.timeout_ns >= 0 && request.opcode != Opcode::kCancel;
  if (linked && request.timeout_slot >= impl_->timeout_slots) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  if (request.receive.size() > std::numeric_limits<unsigned>::max() ||
      request.transmit.size() > std::numeric_limits<unsigned>::max()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  if (io_uring_sq_space_left(&impl_->ring) < (linked ? 2U : 1U)) {
    return std::unexpected(RingError(ENOSPC));
  }
  auto* sqe = io_uring_get_sqe(&impl_->ring);
  switch (request.opcode) {
    case Opcode::kAccept:
      io_uring_prep_accept(sqe, request.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      break;
    case Opcode::kConnect:
      io_uring_prep_connect(sqe, request.fd,
                            reinterpret_cast<const sockaddr*>(request.transmit.data()),
                            static_cast<socklen_t>(request.transmit.size()));
      break;
    case Opcode::kRecv:
      io_uring_prep_recv(sqe, request.fd, request.receive.data(),
                         static_cast<unsigned>(request.receive.size()), 0);
      break;
    case Opcode::kSend:
      io_uring_prep_send(sqe, request.fd, request.transmit.data(),
                         static_cast<unsigned>(request.transmit.size()), MSG_NOSIGNAL);
      break;
    case Opcode::kPoll:
      io_uring_prep_poll_add(sqe, request.fd, request.poll_events);
      break;
    case Opcode::kCancel:
      io_uring_prep_cancel64(sqe, request.cancel_token, 0);
      break;
  }
  io_uring_sqe_set_data64(sqe, request.token);
  if (linked) {
    sqe->flags |= IOSQE_IO_LINK;
    auto& timeout = impl_->timeouts[request.timeout_slot];
    timeout.tv_sec = request.timeout_ns / 1'000'000'000;
    timeout.tv_nsec = request.timeout_ns % 1'000'000'000;
    auto* timeout_sqe = io_uring_get_sqe(&impl_->ring);
    io_uring_prep_link_timeout(timeout_sqe, &timeout, 0);
    io_uring_sqe_set_data64(timeout_sqe, request.token | kTimeoutTag);
  }
  return {};
}

auto UringRing::Submit() noexcept -> std::expected<void, Status> {
  if (!impl_) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  const auto result = io_uring_submit(&impl_->ring);
  /** Backpressure is retried by the next caller tick; no wait is introduced. */
  if (result < 0 && result != -EAGAIN && result != -EBUSY && result != -EINTR) {
    return std::unexpected(RingError(-result));
  }
  return {};
}

auto UringRing::Peek(std::span<Completion> output) noexcept -> std::size_t {
  if (!impl_ || output.empty()) {
    return 0;
  }
  std::array<io_uring_cqe*, 64> cqes{};
  const auto count = io_uring_peek_batch_cqe(
      &impl_->ring, cqes.data(), static_cast<unsigned>(std::min(output.size(), cqes.size())));
  for (const auto index : std::views::iota(0U, count)) {
    output[index] = {io_uring_cqe_get_data64(cqes[index]), cqes[index]->res};
  }
  io_uring_cq_advance(&impl_->ring, count);
  return count;
}

void UringRing::Close() noexcept { impl_.reset(); }
auto UringRing::IsOpen() const noexcept -> bool { return impl_ != nullptr; }

}  // namespace grpcxx
