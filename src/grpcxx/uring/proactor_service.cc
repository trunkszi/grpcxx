#include "grpcxx/uring/proactor_service.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <utility>

namespace grpcxx {
namespace {
auto CapacityError() noexcept -> Status { return Status{StatusCode::kResourceExhausted}; }
constexpr auto kGenerationMask = (std::uint32_t{1} << 30) - 1;
}  // namespace

struct ProactorService::Impl {
  enum class State { kRunning, kDraining, kStopped };
  struct Slot {
    CompletionCallback callback;
    sockaddr_storage address{};
    UringRing::Request request{};
    MonotonicTimestamp deadline{};
    std::uint32_t generation{};
    int result{};
    bool occupied{};
    bool timer{};
    bool cancelled{};
    bool submitted{};
    bool cancel_submitted{};
    bool main_done{};
    bool timeout_done{true};
    bool timed_out{};
  };
  UringRing ring;
  std::unique_ptr<Slot[]> slots;
  std::uint32_t io_capacity{};
  std::uint32_t timer_capacity{};
  std::uint32_t next_operation{};
  std::size_t pending{};
  std::size_t cancel_completions{};
  State state{State::kRunning};
  std::optional<MonotonicTimestamp> shutdown_deadline;
  bool polling{};

  /** Kernel references to operation addresses must end before slot destruction. */
  ~Impl() { ring.Close(); }

  auto Transition(State next) noexcept -> bool {
    bool allowed = false;
    switch (state) {
      case State::kRunning:
        switch (next) {
          case State::kRunning:
            break;
          case State::kDraining:
            allowed = true;
            break;
          case State::kStopped:
            break;
        }
        break;
      case State::kDraining:
        switch (next) {
          case State::kRunning:
            break;
          case State::kDraining:
            allowed = true;
            break;
          case State::kStopped:
            allowed = true;
            break;
        }
        break;
      case State::kStopped:
        switch (next) {
          case State::kRunning:
            break;
          case State::kDraining:
            break;
          case State::kStopped:
            break;
        }
        break;
    }
    if (allowed) {
      state = next;
    }
    return allowed;
  }

  auto Token(std::uint32_t index) const noexcept -> OperationId {
    return {(std::uint64_t{slots[index].generation} << 32) | index};
  }
  auto Find(OperationId id) noexcept -> Slot* {
    const auto index = static_cast<std::uint32_t>(id.value);
    if (index >= io_capacity + timer_capacity) {
      return nullptr;
    }
    auto& slot = slots[index];
    return slot.occupied && Token(index) == id ? &slot : nullptr;
  }
  auto Reserve(std::uint32_t first, std::uint32_t count) noexcept -> std::uint32_t {
    for (const auto index : std::views::iota(first, first + count)) {
      auto& slot = slots[index];
      /** Exhausted capabilities retire permanently rather than alias old handles. */
      if (!slot.occupied && slot.generation < kGenerationMask) {
        ++slot.generation;
        slot.occupied = true;
        slot.timer = first == io_capacity;
        slot.cancelled = false;
        slot.submitted = false;
        slot.cancel_submitted = false;
        slot.main_done = false;
        slot.timeout_done = true;
        slot.timed_out = false;
        ++pending;
        return index;
      }
    }
    return io_capacity + timer_capacity;
  }
  void Complete(Slot& slot, int result) noexcept {
    auto callback = std::move(slot.callback);
    slot.occupied = false;
    --pending;
    callback(result);
  }
  void RequestCancel(std::uint32_t index) noexcept { slots[index].cancelled = true; }
};

ProactorService::ProactorService(ProactorOptions options) noexcept : options_(options) {}
ProactorService::ProactorService(ProactorOptions options, const void* context,
                                 std::uint64_t (*read_identity)(const void*) noexcept) noexcept
    : options_(options), identity_context_(context), read_identity_(read_identity) {}
ProactorService::~ProactorService() {
  if (owner_ && owner_ != CallerThreadId()) {
    std::abort();
  }
  if (impl_ && (impl_->polling || impl_->pending != 0 || impl_->cancel_completions != 0)) {
    std::abort();
  }
}

auto ProactorService::Start(ProactorOptions options) noexcept -> std::expected<void, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (impl_) {
    return std::unexpected(RejectTransition());
  }
  options_ = options;
  return Start();
}

auto ProactorService::Start() noexcept -> std::expected<void, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  /** The first start attempt binds ownership, including configuration failures. */
  const auto identity = CallerThreadId();
  if (identity == 0) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  owner_ = identity;
  if (impl_ || options_.ring_entries < 2 || options_.max_operations == 0 ||
      options_.max_operations > 1'048'576 || options_.max_timers > 1'048'576) {
    return std::unexpected(RejectTransition());
  }
  auto impl = std::unique_ptr<Impl>{new (std::nothrow) Impl{}};
  if (!impl) {
    return std::unexpected(CapacityError());
  }
  impl->slots.reset(new (std::nothrow) Impl::Slot[options_.max_operations + options_.max_timers]);
  if (!impl->slots) {
    return std::unexpected(CapacityError());
  }
  auto opened = impl->ring.Open(options_.ring_entries, options_.max_operations);
  if (!opened) {
    return opened;
  }
  impl->io_capacity = options_.max_operations;
  impl->timer_capacity = options_.max_timers;
  impl_ = std::move(impl);
  return {};
}

auto ProactorService::Queue(UringRing::Request request, CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || impl_->state != Impl::State::kRunning) {
    return std::unexpected(RejectTransition());
  }
  if (request.fd < 0 || !callback ||
      request.receive.size() > std::numeric_limits<unsigned>::max() ||
      request.transmit.size() > std::numeric_limits<unsigned>::max()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  const auto index = impl_->Reserve(0, impl_->io_capacity);
  if (index == impl_->io_capacity + impl_->timer_capacity) {
    return std::unexpected(CapacityError());
  }
  auto& slot = impl_->slots[index];
  request.token = impl_->Token(index).value;
  request.timeout_slot = index;
  if (request.opcode == UringRing::Opcode::kConnect) {
    if (request.transmit.size() < sizeof(sa_family_t) ||
        request.transmit.size() > sizeof(slot.address)) {
      slot.occupied = false;
      --impl_->pending;
      return std::unexpected(Status{StatusCode::kInvalidArgument});
    }
    std::memcpy(&slot.address, request.transmit.data(), request.transmit.size());
    request.transmit = std::as_bytes(std::span{&slot.address, 1}).first(request.transmit.size());
  }
  slot.request = request;
  slot.callback = std::move(callback);
  slot.timeout_done = request.timeout_ns < 0;
  return impl_->Token(index);
}

auto ProactorService::SubmitAccept(int fd, CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return Queue({.opcode = UringRing::Opcode::kAccept, .fd = fd}, std::move(callback));
}
auto ProactorService::SubmitConnect(
    int fd, std::span<const std::byte> address, CompletionCallback callback,
    std::chrono::nanoseconds timeout) noexcept -> std::expected<OperationId, Status> {
  return Queue({.opcode = UringRing::Opcode::kConnect,
                .fd = fd,
                .transmit = address,
                .timeout_ns = timeout.count()},
               std::move(callback));
}
auto ProactorService::SubmitRecv(int fd, std::span<std::byte> buffer, CompletionCallback callback,
                                 std::chrono::nanoseconds timeout) noexcept
    -> std::expected<OperationId, Status> {
  return Queue({.opcode = UringRing::Opcode::kRecv,
                .fd = fd,
                .receive = buffer,
                .timeout_ns = timeout.count()},
               std::move(callback));
}
auto ProactorService::SubmitSend(
    int fd, std::span<const std::byte> buffer, CompletionCallback callback,
    std::chrono::nanoseconds timeout) noexcept -> std::expected<OperationId, Status> {
  return Queue({.opcode = UringRing::Opcode::kSend,
                .fd = fd,
                .transmit = buffer,
                .timeout_ns = timeout.count()},
               std::move(callback));
}
auto ProactorService::SubmitPoll(int fd, unsigned events, CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  return Queue({.opcode = UringRing::Opcode::kPoll, .fd = fd, .poll_events = events},
               std::move(callback));
}

auto ProactorService::ScheduleTimer(MonotonicTimestamp deadline,
                                    CompletionCallback callback) noexcept
    -> std::expected<OperationId, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || impl_->state != Impl::State::kRunning) {
    return std::unexpected(RejectTransition());
  }
  if (!callback) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  const auto index = impl_->Reserve(impl_->io_capacity, impl_->timer_capacity);
  if (index == impl_->io_capacity + impl_->timer_capacity) {
    return std::unexpected(CapacityError());
  }
  auto& slot = impl_->slots[index];
  slot.deadline = deadline;
  slot.callback = std::move(callback);
  return impl_->Token(index);
}

auto ProactorService::Cancel(OperationId operation) noexcept -> std::expected<void, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || !impl_->Find(operation)) {
    return std::unexpected(Status{StatusCode::kNotFound});
  }
  impl_->RequestCancel(static_cast<std::uint32_t>(operation.value));
  return {};
}

auto ProactorService::PollOnce(PollBudget budget, MonotonicTimestamp now) noexcept
    -> std::expected<PollResult, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!budget.Valid()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  if (!impl_ || impl_->state == Impl::State::kStopped || impl_->polling) {
    return std::unexpected(RejectTransition());
  }
  impl_->polling = true;
  struct PollGuard {
    bool& polling;
    ~PollGuard() { polling = false; }
  } guard{impl_->polling};
  PollResult result{};
  std::optional<Status> failure;
  /** Staging is bounded by configured slot capacity; command emission obeys this tick's budget. */
  const auto first_operation = impl_->next_operation;
  for (std::uint32_t checked = 0; checked < impl_->io_capacity; ++checked) {
    const auto index = (first_operation + checked) % impl_->io_capacity;
    auto& slot = impl_->slots[index];
    if (!slot.occupied) {
      continue;
    }
    if (impl_->state == Impl::State::kDraining) {
      slot.cancelled = true;
    }
    if (slot.cancelled && !slot.submitted) {
      if (result.completions < budget.completions && result.socket_events < budget.socket_events) {
        ++result.completions;
        ++result.socket_events;
        impl_->next_operation = (index + 1) % impl_->io_capacity;
        impl_->Complete(slot, -ECANCELED);
      }
      continue;
    }
    if (result.commands >= budget.commands || (slot.submitted && !slot.cancelled) ||
        slot.cancel_submitted) {
      continue;
    }
    const auto token = impl_->Token(index).value;
    const UringRing::Request cancel{.opcode = UringRing::Opcode::kCancel,
                                    .token = token | UringRing::kCancelTag,
                                    .cancel_token = token};
    auto queued = impl_->ring.Queue(slot.cancelled ? cancel : slot.request);
    if (!queued) {
      if (queued.error().Code() == StatusCode::kResourceExhausted) {
        /** SQ pressure leaves staged operations available for the next caller tick. */
        continue;
      }
      return std::unexpected(queued.error());
    }
    ++result.commands;
    impl_->next_operation = (index + 1) % impl_->io_capacity;
    if (slot.cancelled) {
      slot.cancel_submitted = true;
      ++impl_->cancel_completions;
    } else {
      slot.submitted = true;
    }
  }
  auto submitted = impl_->ring.Submit();
  if (!submitted) {
    return std::unexpected(submitted.error());
  }
  std::array<UringRing::Completion, 64> completions{};
  std::uint32_t remaining = std::min(budget.completions - result.completions,
                                     budget.socket_events - result.socket_events);
  while (remaining > 0) {
    const auto count = impl_->ring.Peek(
        std::span{completions}.first(std::min<std::size_t>(remaining, completions.size())));
    if (count == 0) {
      break;
    }
    remaining -= static_cast<std::uint32_t>(count);
    result.completions += static_cast<std::uint32_t>(count);
    for (const auto& completion : std::span{completions}.first(count)) {
      if ((completion.token & UringRing::kCancelTag) != 0) {
        if (impl_->cancel_completions > 0) {
          --impl_->cancel_completions;
        }
        if (completion.result < 0 && completion.result != -ENOENT &&
            completion.result != -EALREADY) {
          const auto id = OperationId{completion.token & ~UringRing::kCancelTag};
          if (auto* slot = impl_->Find(id)) {
            slot->cancel_submitted = false;
          }
          failure = Status{StatusCode::kUnavailable};
        }
        continue;
      }
      const bool timeout = (completion.token & UringRing::kTimeoutTag) != 0;
      const auto id = OperationId{completion.token & ~UringRing::kTimeoutTag};
      auto* slot = impl_->Find(id);
      if (!slot) {
        failure = Status{StatusCode::kInternal};
        continue;
      }
      if (timeout) {
        slot->timeout_done = true;
        slot->timed_out = completion.result == -ETIME;
      } else {
        slot->main_done = true;
        slot->result = completion.result;
      }
      if (slot->main_done && slot->timeout_done) {
        ++result.socket_events;
        impl_->Complete(*slot,
                        slot->timed_out && slot->result == -ECANCELED ? -ETIMEDOUT : slot->result);
      }
    }
    if (failure) {
      return std::unexpected(*failure);
    }
  }
  for (const auto index :
       std::views::iota(impl_->io_capacity, impl_->io_capacity + impl_->timer_capacity)) {
    if (result.timers >= budget.timers) {
      break;
    }
    auto& slot = impl_->slots[index];
    if (impl_->state == Impl::State::kDraining) {
      slot.cancelled = true;
    }
    if (slot.occupied && (slot.cancelled || slot.deadline <= now)) {
      ++result.timers;
      impl_->Complete(slot, slot.cancelled ? -ECANCELED : 0);
    }
  }
  result.pending = impl_->pending != 0 || impl_->cancel_completions != 0;
  for (const auto index :
       std::views::iota(impl_->io_capacity, impl_->io_capacity + impl_->timer_capacity)) {
    const auto& slot = impl_->slots[index];
    if (slot.occupied && !slot.cancelled &&
        (!result.deadline || slot.deadline < *result.deadline)) {
      result.deadline = slot.deadline;
    }
  }
  return result;
}

auto ProactorService::BeginShutdown(MonotonicTimestamp deadline) noexcept
    -> std::expected<void, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || impl_->state == Impl::State::kStopped) {
    return std::unexpected(RejectTransition());
  }
  if (!impl_->Transition(Impl::State::kDraining)) {
    return std::unexpected(RejectTransition());
  }
  if (!impl_->shutdown_deadline || deadline < *impl_->shutdown_deadline) {
    impl_->shutdown_deadline = deadline;
  }
  return {};
}

auto ProactorService::PollShutdown(PollBudget budget, MonotonicTimestamp now) noexcept
    -> std::expected<PollResult, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || impl_->state != Impl::State::kDraining) {
    return std::unexpected(RejectTransition());
  }
  auto result = PollOnce(budget, now);
  if (!result) {
    return std::unexpected(result.error());
  }
  if (result->pending && impl_->shutdown_deadline && now >= *impl_->shutdown_deadline) {
    return std::unexpected(Status{StatusCode::kDeadlineExceeded});
  }
  return result;
}

auto ProactorService::Shutdown() noexcept -> std::expected<void, Status> {
  if (owner_ && owner_ != CallerThreadId()) {
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  }
  if (!impl_ || impl_->polling || impl_->state != Impl::State::kDraining || impl_->pending != 0 ||
      impl_->cancel_completions != 0) {
    return std::unexpected(RejectTransition());
  }
  if (!impl_->Transition(Impl::State::kStopped)) {
    return std::unexpected(RejectTransition());
  }
  impl_->ring.Close();
  return {};
}

auto ProactorService::State() const noexcept -> ServiceState {
  if (owner_ && owner_ != CallerThreadId()) {
    return ServiceState::kFailed;
  }
  if (!impl_) {
    return ServiceState::kStopped;
  }
  switch (impl_->state) {
    case Impl::State::kRunning:
      return ServiceState::kRunning;
    case Impl::State::kDraining:
      return ServiceState::kStopping;
    case Impl::State::kStopped:
      return ServiceState::kStopped;
  }
  std::unreachable();
}

auto ProactorService::Stopped() const noexcept -> bool {
  return (!owner_ || owner_ == CallerThreadId()) &&
         (!impl_ || impl_->state == Impl::State::kStopped ||
          (impl_->state == Impl::State::kDraining && impl_->pending == 0 &&
           impl_->cancel_completions == 0));
}

auto ProactorService::RejectTransition() noexcept -> Status {
  if (rejected_transitions_ != std::numeric_limits<std::uint64_t>::max()) {
    ++rejected_transitions_;
  }
  return Status{StatusCode::kFailedPrecondition};
}

auto ProactorService::RejectedTransitions() const noexcept -> std::uint64_t {
  return !owner_ || owner_ == CallerThreadId() ? rejected_transitions_ : 0;
}

auto ProactorService::PendingOperations() const noexcept -> std::size_t {
  return owner_ == CallerThreadId() && impl_ ? impl_->pending : 0;
}

auto ProactorService::CallerThreadId() const noexcept -> std::uint64_t {
  return read_identity_ ? read_identity_(identity_context_)
                        : static_cast<std::uint64_t>(::gettid());
}

}  // namespace grpcxx
