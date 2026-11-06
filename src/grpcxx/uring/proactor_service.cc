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
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {
auto CapacityError() noexcept -> Status { return Status{StatusCode::kResourceExhausted}; }
constexpr auto kGenerationMask = (std::uint32_t{1} << 30) - 1;
class SlotBitmap {
  std::unique_ptr<uarch::Bitmap64[]> words_;
  std::array<std::uint32_t, 5> offsets_{}, counts_{};
  unsigned levels_{};
  std::uint32_t capacity_{};
  auto Find(unsigned level, std::uint32_t first) const noexcept -> std::optional<std::uint32_t> {
    const auto word = first >> 6;
    if (word >= counts_[level]) return std::nullopt;
    if (auto bit = words_[offsets_[level] + word].next_one_from(first & 63U))
      return word * 64U + *bit;
    if (level + 1 == levels_) return std::nullopt;
    auto next = Find(level + 1, word + 1);
    if (!next) return std::nullopt;
    return *next * 64U + *words_[offsets_[level] + *next].lowest_one();
  }

 public:
  bool Initialize(std::uint32_t capacity, bool full) noexcept {
    capacity_ = capacity;
    if (capacity == 0) return true;
    auto count = (capacity + 63U) >> 6;
    std::uint32_t total = 0;
    do {
      offsets_[levels_] = total;
      counts_[levels_++] = count;
      total += count;
      if (count == 1) break;
      count = (count + 63U) >> 6;
    } while (levels_ < counts_.size());
    words_.reset(new (std::nothrow) uarch::Bitmap64[total]{});
    if (!words_) return false;
    if (full)
      for (std::uint32_t i = 0; i < capacity; ++i) Insert(i);
    return true;
  }
  void Insert(std::uint32_t index) noexcept {
    for (unsigned level = 0; level < levels_; ++level) {
      const auto word = index >> 6;
      auto& bits = words_[offsets_[level] + word];
      const bool empty = bits.is_empty();
      bits = bits.set(index & 63U);
      if (!empty) break;
      index = word;
    }
  }
  void Erase(std::uint32_t index) noexcept {
    for (unsigned level = 0; level < levels_; ++level) {
      const auto word = index >> 6;
      auto& bits = words_[offsets_[level] + word];
      bits = bits.clear(index & 63U);
      if (!bits.is_empty()) break;
      index = word;
    }
  }
  auto Next(std::uint32_t first) const noexcept -> std::optional<std::uint32_t> {
    return first < capacity_ ? Find(0, first) : std::nullopt;
  }
};
struct ProactorSlotMetaSpec {
  using Word = std::uint64_t;
  bool occupied[[= uarch::bits::Flag{0}]];
  bool timer[[= uarch::bits::Flag{1}]];
  bool cancelled[[= uarch::bits::Flag{2}]];
  bool submitted[[= uarch::bits::Flag{3}]];
  bool cancel_submitted[[= uarch::bits::Flag{4}]];
  bool main_done[[= uarch::bits::Flag{5}]];
  bool timeout_done[[= uarch::bits::Flag{6}]];
  bool timed_out[[= uarch::bits::Flag{7}]];
  std::uint8_t opcode[[= uarch::bits::Field{8, 3}]];
  std::uint32_t generation[[= uarch::bits::Field{32, 30}]];
};
using ProactorSlotMeta = uarch::BitWord<ProactorSlotMetaSpec>;
static_assert(uarch::NoPadding<ProactorSlotMeta> && uarch::RegisterPassable<ProactorSlotMeta>);
}  // namespace

struct ProactorService::Impl {
  enum class State { kRunning, kDraining, kStopped };
  struct Slot {
    ProactorSlotMeta meta{};
    MonotonicTimestamp deadline{};
    std::int32_t result{};
    std::uint32_t reserved{};
    auto Generation() const noexcept -> std::uint32_t {
      return meta.get<^^ProactorSlotMetaSpec::generation>();
    }
    bool Occupied() const noexcept { return meta.test<^^ProactorSlotMetaSpec::occupied>(); }
    void SetOccupied(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::occupied>(value);
    }
    bool Timer() const noexcept { return meta.test<^^ProactorSlotMetaSpec::timer>(); }
    void SetTimer(bool value) noexcept { meta = meta.with<^^ProactorSlotMetaSpec::timer>(value); }
    bool Cancelled() const noexcept { return meta.test<^^ProactorSlotMetaSpec::cancelled>(); }
    void SetCancelled(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::cancelled>(value);
    }
    bool Submitted() const noexcept { return meta.test<^^ProactorSlotMetaSpec::submitted>(); }
    void SetSubmitted(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::submitted>(value);
    }
    bool CancelSubmitted() const noexcept {
      return meta.test<^^ProactorSlotMetaSpec::cancel_submitted>();
    }
    void SetCancelSubmitted(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::cancel_submitted>(value);
    }
    bool MainDone() const noexcept { return meta.test<^^ProactorSlotMetaSpec::main_done>(); }
    void SetMainDone(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::main_done>(value);
    }
    bool TimeoutDone() const noexcept { return meta.test<^^ProactorSlotMetaSpec::timeout_done>(); }
    void SetTimeoutDone(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::timeout_done>(value);
    }
    bool TimedOut() const noexcept { return meta.test<^^ProactorSlotMetaSpec::timed_out>(); }
    void SetTimedOut(bool value) noexcept {
      meta = meta.with<^^ProactorSlotMetaSpec::timed_out>(value);
    }
  };
  static_assert(sizeof(Slot) == 24 && uarch::NoPadding<Slot>);
  struct ColdSlot {
    CompletionCallback callback;
    sockaddr_storage address{};
    UringRing::Request request{};
  };
  UringRing ring;
  std::unique_ptr<Slot[]> slots;
  std::unique_ptr<ColdSlot[]> cold;
  SlotBitmap free_io, free_timers, active_io, active_timers, staged_ops;
  uarch::FastDivisorU32 io_divisor = uarch::arithmetic::divisor_u32<1>();
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
        allowed = next == State::kDraining;
        break;
      case State::kDraining:
        allowed = next == State::kDraining || next == State::kStopped;
        break;
      case State::kStopped:
        break;
    }
    if (allowed) state = next;
    return allowed;
  }

  auto Token(std::uint32_t index) const noexcept -> OperationId {
    return {(std::uint64_t{slots[index].Generation()} << 32) | index};
  }
  auto Find(OperationId id) noexcept -> Slot* {
    const auto index = static_cast<std::uint32_t>(id.value);
    if (index >= io_capacity + timer_capacity) {
      return nullptr;
    }
    auto& slot = slots[index];
    constexpr auto occupied =
        uarch::BitPattern<ProactorSlotMetaSpec>{}.flag_set<^^ProactorSlotMetaSpec::occupied>();
    return occupied.matches(slot.meta) && Token(index) == id ? &slot : nullptr;
  }
  auto Reserve(std::uint32_t first, std::uint32_t) noexcept -> std::uint32_t {
    auto& free = first == 0 ? free_io : free_timers;
    auto local = free.Next(0);
    if (!local) return io_capacity + timer_capacity;
    free.Erase(*local);
    const auto index = first + *local;
    auto& slot = slots[index];
    const auto generation = slot.Generation() + 1U;
    const auto initial = uarch::BitPatch<ProactorSlotMetaSpec>{}
                             .set_flag<^^ProactorSlotMetaSpec::occupied>(true)
                             .set_flag<^^ProactorSlotMetaSpec::timer>(first == io_capacity)
                             .set_flag<^^ProactorSlotMetaSpec::timeout_done>(true);
    slot.meta =
        initial.apply(ProactorSlotMeta{}.with<^^ProactorSlotMetaSpec::generation>(generation));
    (first == 0 ? active_io : active_timers).Insert(*local);
    ++pending;
    return index;
  }
  void Release(std::uint32_t index) noexcept {
    auto& slot = slots[index];
    const bool timer = slot.Timer();
    const auto local = timer ? index - io_capacity : index;
    (timer ? active_timers : active_io).Erase(local);
    if (!timer) staged_ops.Erase(local);
    slot.SetOccupied(false);
    if (slot.Generation() < kGenerationMask) (timer ? free_timers : free_io).Insert(local);
    --pending;
  }
  void Complete(Slot& slot, int result) noexcept {
    const auto index = static_cast<std::uint32_t>(&slot - slots.get());
    auto callback = std::move(cold[index].callback);
    Release(index);
    callback(result);
  }
  void RequestCancel(std::uint32_t index) noexcept {
    auto& slot = slots[index];
    slot.SetCancelled(true);
    if (!slot.Timer() && !slot.CancelSubmitted()) staged_ops.Insert(index);
  }
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
  impl->cold.reset(new (std::nothrow)
                       Impl::ColdSlot[options_.max_operations + options_.max_timers]);
  if (!impl->slots || !impl->cold || !impl->free_io.Initialize(options_.max_operations, true) ||
      !impl->free_timers.Initialize(options_.max_timers, true) ||
      !impl->active_io.Initialize(options_.max_operations, false) ||
      !impl->staged_ops.Initialize(options_.max_operations, false) ||
      !impl->active_timers.Initialize(options_.max_timers, false)) {
    return std::unexpected(CapacityError());
  }
  auto opened = impl->ring.Open(options_.ring_entries, options_.max_operations);
  if (!opened) {
    return opened;
  }
  impl->io_divisor = *uarch::FastDivisorU32::try_new(options_.max_operations);
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
  auto& cold = impl_->cold[index];
  request.token = impl_->Token(index).value;
  request.timeout_slot = index;
  if (request.opcode == UringRing::Opcode::kConnect) {
    if (request.transmit.size() < sizeof(sa_family_t) ||
        request.transmit.size() > sizeof(cold.address)) {
      impl_->Release(index);
      return std::unexpected(Status{StatusCode::kInvalidArgument});
    }
    std::memcpy(&cold.address, request.transmit.data(), request.transmit.size());
    request.transmit = std::as_bytes(std::span{&cold.address, 1}).first(request.transmit.size());
  }
  slot.meta =
      slot.meta.with<^^ProactorSlotMetaSpec::opcode>(static_cast<std::uint8_t>(request.opcode));
  cold.request = request;
  impl_->cold[index].callback = std::move(callback);
  slot.SetTimeoutDone(request.timeout_ns < 0);
  impl_->staged_ops.Insert(index);
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
  impl_->cold[index].callback = std::move(callback);
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
  const auto first_operation = impl_->io_divisor.rem(impl_->next_operation);
  for (unsigned pass = 0; pass < 2; ++pass) {
    const auto lower = pass == 0 ? first_operation : 0U;
    const auto upper = pass == 0 ? impl_->io_capacity : first_operation;
    for (auto found = impl_->staged_ops.Next(lower); found && *found < upper;
         found = impl_->staged_ops.Next(*found + 1U)) {
      const auto index = *found;
      auto& slot = impl_->slots[index];
      if (!slot.Occupied()) {
        continue;
      }
      if (slot.Cancelled() && !slot.Submitted()) {
        if (result.completions < budget.completions &&
            result.socket_events < budget.socket_events) {
          ++result.completions;
          ++result.socket_events;
          impl_->next_operation = index + 1U == impl_->io_capacity ? 0U : index + 1U;
          impl_->Complete(slot, -ECANCELED);
        }
        continue;
      }
      if (result.commands >= budget.commands || (slot.Submitted() && !slot.Cancelled()) ||
          slot.CancelSubmitted()) {
        continue;
      }
      const auto token = impl_->Token(index).value;
      const UringRing::Request cancel{.opcode = UringRing::Opcode::kCancel,
                                      .token = token | UringRing::kCancelTag,
                                      .cancel_token = token};
      auto queued = impl_->ring.Queue(slot.Cancelled() ? cancel : impl_->cold[index].request);
      if (!queued) {
        if (queued.error().Code() == StatusCode::kResourceExhausted) {
          /** SQ pressure leaves staged operations available for the next caller tick. */
          continue;
        }
        return std::unexpected(queued.error());
      }
      ++result.commands;
      impl_->staged_ops.Erase(index);
      impl_->next_operation = index + 1U == impl_->io_capacity ? 0U : index + 1U;
      if (slot.Cancelled()) {
        slot.SetCancelSubmitted(true);
        ++impl_->cancel_completions;
      } else {
        slot.SetSubmitted(true);
      }
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
            slot->SetCancelSubmitted(false);
            impl_->RequestCancel(static_cast<std::uint32_t>(id.value));
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
        slot->SetTimeoutDone(true);
        slot->SetTimedOut(completion.result == -ETIME);
      } else {
        slot->SetMainDone(true);
        slot->result = completion.result;
      }
      if (slot->MainDone() && slot->TimeoutDone()) {
        ++result.socket_events;
        impl_->Complete(*slot,
                        slot->TimedOut() && slot->result == -ECANCELED ? -ETIMEDOUT : slot->result);
      }
    }
    if (failure) {
      return std::unexpected(*failure);
    }
  }
  for (auto local = impl_->active_timers.Next(0); local;
       local = impl_->active_timers.Next(*local + 1U)) {
    const auto index = impl_->io_capacity + *local;
    if (result.timers >= budget.timers) {
      break;
    }
    auto& slot = impl_->slots[index];
    if (impl_->state == Impl::State::kDraining) {
      slot.SetCancelled(true);
    }
    if (slot.Occupied() && (slot.Cancelled() || slot.deadline <= now)) {
      ++result.timers;
      impl_->Complete(slot, slot.Cancelled() ? -ECANCELED : 0);
    }
  }
  result.pending = impl_->pending != 0 || impl_->cancel_completions != 0;
  for (auto local = impl_->active_timers.Next(0); local;
       local = impl_->active_timers.Next(*local + 1U)) {
    const auto index = impl_->io_capacity + *local;
    const auto& slot = impl_->slots[index];
    if (slot.Occupied() && !slot.Cancelled() &&
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
  for (auto index = impl_->active_io.Next(0); index; index = impl_->active_io.Next(*index + 1U))
    impl_->RequestCancel(*index);
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
  rejected_transitions_ =
      uarch::arithmetic::saturating_add(rejected_transitions_, std::uint64_t{1});
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
