#include "grpcxx/uring/uring_ring.h"

#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <liburing/liburing.hpp>
#include <limits>
#include <new>
#include <optional>

namespace grpcxx {
namespace {
auto RingError(liburing::UringErrorCode error) noexcept -> Status {
  using enum liburing::UringErrorCode;
  if (error == kNoMemory || error == kBusy || error == kNoSpace || error == kAgain ||
      error == kSqRingFull)
    return Status{StatusCode::kResourceExhausted};
  return Status{StatusCode::kUnavailable};
}
void Encode(const UringRing::Request& request, liburing::abi::FlatSqe& sqe) noexcept {
  using namespace liburing;
  const auto token = UserDataToken::from_raw(request.token);
  switch (request.opcode) {
    case UringRing::Opcode::kAccept:
      ops::PrepareAccept(request.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC)
          .WithToken(token)
          .Encode(sqe);
      break;
    case UringRing::Opcode::kConnect:
      ops::PrepareConnect(request.fd, request.transmit.data(),
                          static_cast<std::uint32_t>(request.transmit.size()))
          .WithToken(token)
          .Encode(sqe);
      break;
    case UringRing::Opcode::kRecv:
      ops::PrepareRecv(request.fd, request.receive.data(),
                       static_cast<std::uint32_t>(request.receive.size()))
          .WithToken(token)
          .Encode(sqe);
      break;
    case UringRing::Opcode::kSend:
      ops::PrepareSend(request.fd, request.transmit.data(),
                       static_cast<std::uint32_t>(request.transmit.size()), MSG_NOSIGNAL)
          .WithToken(token)
          .Encode(sqe);
      break;
    case UringRing::Opcode::kPoll:
      ops::PreparePollAdd(request.fd, request.poll_events).WithToken(token).Encode(sqe);
      break;
    case UringRing::Opcode::kCancel:
      ops::PrepareAsyncCancel(UserDataToken::from_raw(request.cancel_token))
          .WithToken(token)
          .Encode(sqe);
      break;
  }
}
}  // namespace

struct UringRing::Impl {
  liburing::ring::KernelRing ring;
  std::unique_ptr<liburing::abi::KernelTimespec[]> timeouts;
  unsigned timeout_slots{};
  std::optional<Status> progress_error;
  ~Impl() { ring = liburing::ring::KernelRing{}; }
};

UringRing::UringRing() noexcept = default;
UringRing::~UringRing() = default;
UringRing::UringRing(UringRing&&) noexcept = default;
auto UringRing::operator=(UringRing&&) noexcept -> UringRing& = default;

auto UringRing::Open(unsigned entries,
                     unsigned timeout_slots) noexcept -> std::expected<void, Status> {
  if (impl_ || entries < 2 || timeout_slots == 0)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto impl = std::unique_ptr<Impl>{new (std::nothrow) Impl{}};
  if (!impl) return std::unexpected(Status{StatusCode::kResourceExhausted});
  impl->timeouts.reset(new (std::nothrow) liburing::abi::KernelTimespec[timeout_slots]{});
  if (!impl->timeouts) return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto ring = liburing::ring::KernelRing::Create(entries, liburing::ring::RingMode::kStandard64_16);
  if (!ring) return std::unexpected(RingError(ring.error()));
  if ((ring->Params().features & liburing::abi::kFeatNoDrop) == 0)
    return std::unexpected(Status{StatusCode::kUnavailable});
  impl->ring = std::move(*ring);
  impl->timeout_slots = timeout_slots;
  impl_ = std::move(impl);
  return {};
}

auto UringRing::Queue(const Request& request) noexcept -> std::expected<void, Status> {
  if (!impl_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  const bool linked = request.timeout_ns >= 0 && request.opcode != Opcode::kCancel;
  if ((linked && request.timeout_slot >= impl_->timeout_slots) ||
      request.receive.size() > std::numeric_limits<unsigned>::max() ||
      request.transmit.size() > std::numeric_limits<unsigned>::max())
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  // Reserve the whole transaction before encoding either member or borrowing the timespec.
  liburing::ring::SqeBatch batch{impl_->ring.Sq()};
  auto primary = batch.Reserve64();
  if (!primary) return std::unexpected(RingError(primary.error()));
  if (linked) {
    auto timeout_sqe = batch.Reserve64();
    if (!timeout_sqe) return std::unexpected(RingError(timeout_sqe.error()));
    auto& timeout = impl_->timeouts[request.timeout_slot];
    timeout.tv_sec = request.timeout_ns / 1'000'000'000;
    timeout.tv_nsec = request.timeout_ns % 1'000'000'000;
    Encode(request, **primary);
    (**primary).flags |= liburing::abi::kSqeIoLink;
    liburing::ops::PrepareLinkTimeout(&timeout, 0)
        .WithToken(liburing::UserDataToken::from_raw(request.token | kTimeoutTag))
        .Encode(**timeout_sqe);
  } else {
    Encode(request, **primary);
  }
  batch.Commit();
  return {};
}

auto UringRing::Submit() noexcept -> std::expected<void, Status> {
  if (!impl_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (impl_->progress_error) {
    auto error = std::move(*impl_->progress_error);
    impl_->progress_error.reset();
    return std::unexpected(std::move(error));
  }
  // Tail minus kernel head also retains partially submitted SQEs for retry.
  if (impl_->ring.Sq().InFlightCount() == 0U) return {};
  auto submitted = impl_->ring.SubmitAndWait(0U);
  if (!submitted && submitted.error() != liburing::UringErrorCode::kAgain &&
      submitted.error() != liburing::UringErrorCode::kBusy &&
      submitted.error() != liburing::UringErrorCode::kInterrupted)
    return std::unexpected(RingError(submitted.error()));
  return {};
}

auto UringRing::Peek(std::span<Completion> output) noexcept -> std::size_t {
  if (!impl_ || output.empty()) return 0;
  if ((impl_->ring.Sq().Flags() & (liburing::abi::kSqTaskrun | liburing::abi::kSqCqOverflow)) !=
      0) {
    auto progress = impl_->ring.PollKernelCompletions(0U);
    if (!progress && progress.error() != liburing::UringErrorCode::kInterrupted &&
        progress.error() != liburing::UringErrorCode::kAgain)
      impl_->progress_error = RingError(progress.error());
  }
  std::array<liburing::Completion, 64> batch{};
  const auto count =
      impl_->ring.Cq().ReapBatch(std::span{batch}.first(std::min(output.size(), batch.size())));
  for (std::size_t i = 0; i < count; ++i) output[i] = {batch[i].token.raw(), batch[i].result};
  return count;
}

void UringRing::Close() noexcept { impl_.reset(); }
auto UringRing::IsOpen() const noexcept -> bool { return impl_ != nullptr; }
}  // namespace grpcxx
