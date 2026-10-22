#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/core/service.h"
#include "grpcxx/core/status.h"

namespace {

/**
 * Allocation counters owned by this test binary.
 *
 * GoogleTest allocates while registering and reporting tests, so allocation-free scenarios
 * reset the probe immediately before the code under test and read it immediately after, with no
 * framework call in between. Only the replaceable allocation functions are instrumented; a
 * direct `std::malloc`/`aligned_alloc` call inside library code would not be counted.
 */
struct AllocationStats {
  std::uint64_t calls{0U};
  std::uint64_t bytes{0U};
};

AllocationStats g_allocation_stats{};

[[nodiscard]] auto Allocate(std::size_t size, std::size_t alignment) noexcept -> void* {
  void* memory = nullptr;
  if (alignment <= alignof(std::max_align_t)) {
    memory = std::malloc(size == 0U ? 1U : size);
  } else {
    // aligned_alloc requires the size to be a multiple of the alignment.
    const std::size_t rounded = ((size + alignment - 1U) / alignment) * alignment;
    memory = std::aligned_alloc(alignment, rounded == 0U ? alignment : rounded);
  }
  if (memory != nullptr) {
    ++g_allocation_stats.calls;
    g_allocation_stats.bytes += size;
  }
  return memory;
}

[[nodiscard]] auto RequireAllocation(void* memory) -> void* {
  if (memory == nullptr) {
    std::abort();
  }
  return memory;
}

void ResetAllocationProbe() noexcept { g_allocation_stats = AllocationStats{}; }

[[nodiscard]] auto ReadAllocationProbe() noexcept -> AllocationStats { return g_allocation_stats; }

}  // namespace

void* operator new(std::size_t size) {
  return RequireAllocation(Allocate(size, alignof(std::max_align_t)));
}

void* operator new[](std::size_t size) {
  return RequireAllocation(Allocate(size, alignof(std::max_align_t)));
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  return Allocate(size, alignof(std::max_align_t));
}

void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return Allocate(size, alignof(std::max_align_t));
}

void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
  return Allocate(size, static_cast<std::size_t>(alignment));
}

void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
  return Allocate(size, static_cast<std::size_t>(alignment));
}

void* operator new(std::size_t size, std::align_val_t alignment) {
  return RequireAllocation(Allocate(size, static_cast<std::size_t>(alignment)));
}

void* operator new[](std::size_t size, std::align_val_t alignment) {
  return operator new(size, alignment);
}

void operator delete(void* memory) noexcept { std::free(memory); }

void operator delete[](void* memory) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

void operator delete(void* memory, std::align_val_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::align_val_t) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }

void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }

namespace grpcxx {
namespace {

using IntDelegate = InplaceDelegate<int(int) noexcept>;

/** Callable that cannot fit; the constructor constraint must reject it before its body. */
struct OversizedCallable final {
  std::array<std::byte, IntDelegate::kCapacity + 1U> state{};

  auto operator()(int value) noexcept -> int { return value; }
};

/** Callable that fits the capacity exactly; the boundary itself must be accepted. */
struct ExactCapacityCallable final {
  std::array<std::byte, IntDelegate::kCapacity> state{};

  auto operator()(int value) noexcept -> int { return value; }
};

/** Callable whose alignment exceeds the storage alignment. */
struct OverAlignedCallable final {
  alignas(2U * alignof(std::max_align_t)) int value{0};

  auto operator()(int) noexcept -> int { return value; }
};

static_assert(IntDelegate::kCapacity == 64U);
static_assert(sizeof(OversizedCallable) > IntDelegate::kCapacity);
static_assert(sizeof(ExactCapacityCallable) == IntDelegate::kCapacity);
static_assert(alignof(OverAlignedCallable) > alignof(std::max_align_t));
static_assert(!std::is_constructible_v<IntDelegate, OversizedCallable>);
static_assert(!std::is_constructible_v<IntDelegate, OverAlignedCallable>);
static_assert(std::is_constructible_v<IntDelegate, ExactCapacityCallable>);
static_assert(std::is_nothrow_move_constructible_v<IntDelegate>);
static_assert(std::is_nothrow_destructible_v<IntDelegate>);
static_assert(std::is_move_assignable_v<IntDelegate>);
static_assert(!std::is_copy_constructible_v<IntDelegate>);
static_assert(!std::is_copy_assignable_v<IntDelegate>);

/** Move-only callable that records how often it was invoked. */
struct MoveOnlyAccumulator final {
  int base{0};
  int* calls{nullptr};

  MoveOnlyAccumulator(int initial, int* counter) noexcept : base(initial), calls(counter) {}

  MoveOnlyAccumulator(const MoveOnlyAccumulator&) = delete;
  MoveOnlyAccumulator& operator=(const MoveOnlyAccumulator&) = delete;

  MoveOnlyAccumulator(MoveOnlyAccumulator&& other) noexcept : base(other.base), calls(other.calls) {
    other.calls = nullptr;
  }

  auto operator()(int value) noexcept -> int {
    ++(*calls);
    return base + value;
  }
};

static_assert(!std::is_copy_constructible_v<MoveOnlyAccumulator>);
static_assert(std::is_nothrow_move_constructible_v<MoveOnlyAccumulator>);
// The delegate copy-constructs an lvalue target, so a move-only callable must be rejected as an
// lvalue but accepted as an rvalue.
static_assert(!std::is_constructible_v<IntDelegate, MoveOnlyAccumulator&>);
static_assert(std::is_constructible_v<IntDelegate, MoveOnlyAccumulator&&>);

/** Tracks live instances so construction, moves and destruction are observable. */
struct LifecycleProbe final {
  static inline int live{0};

  int value{0};

  explicit LifecycleProbe(int initial) noexcept : value(initial) { ++live; }

  LifecycleProbe(const LifecycleProbe&) = delete;

  LifecycleProbe(LifecycleProbe&& other) noexcept : value(other.value) { ++live; }

  ~LifecycleProbe() noexcept { --live; }

  auto operator()() noexcept -> int { return value; }
};
// ---------------------------------------------------------------------------
// InplaceDelegate
// ---------------------------------------------------------------------------
TEST(InplaceDelegateTest, InvokesCapturedStateMoveOnlyAndVoidCallables) {
  int offset = 40;
  IntDelegate delegate([offset](int value) noexcept { return value + offset; });
  EXPECT_TRUE(static_cast<bool>(delegate));
  EXPECT_EQ(delegate(2), 42);
  EXPECT_EQ(delegate(-40), 0);

  int calls = 0;
  IntDelegate accumulator(MoveOnlyAccumulator{100, &calls});
  EXPECT_EQ(accumulator(5), 105);
  EXPECT_EQ(accumulator(7), 107);
  EXPECT_EQ(calls, 2);

  int observed = 0;
  InplaceDelegate<void(int*) noexcept> writer(
      [&observed](int* value) noexcept { observed = *value; });
  int source = 17;
  writer(&source);
  EXPECT_EQ(observed, 17);

  // A target that fills the storage exactly must still be storable and invocable.
  IntDelegate full(ExactCapacityCallable{});
  EXPECT_EQ(full(9), 9);
}
TEST(InplaceDelegateTest, MoveOperationsTransferTargetAndEmptyTheSource) {
  IntDelegate source([](int value) noexcept { return value * 3; });
  IntDelegate moved(std::move(source));
  EXPECT_FALSE(static_cast<bool>(source));
  EXPECT_TRUE(static_cast<bool>(moved));
  EXPECT_EQ(moved(4), 12);

  IntDelegate first([](int value) noexcept { return value + 1; });
  first = std::move(moved);
  EXPECT_FALSE(static_cast<bool>(moved));
  EXPECT_EQ(first(5), 15);

  IntDelegate empty;
  IntDelegate empty_move(std::move(empty));
  EXPECT_FALSE(static_cast<bool>(empty));
  EXPECT_FALSE(static_cast<bool>(empty_move));

  first = std::move(empty_move);
  EXPECT_FALSE(static_cast<bool>(first));
  EXPECT_FALSE(static_cast<bool>(empty_move));
}
TEST(InplaceDelegateTest, MoveAssignmentDestroysPreviouslyHeldTarget) {
  using NoArgDelegate = InplaceDelegate<int() noexcept>;

  LifecycleProbe::live = 0;
  {
    NoArgDelegate first(LifecycleProbe{1});
    NoArgDelegate second(LifecycleProbe{2});
    ASSERT_EQ(LifecycleProbe::live, 2);

    first = std::move(second);

    // The old target in first is destroyed; the target moved out of second is constructed
    // in first's storage, so exactly one target remains alive.
    EXPECT_EQ(LifecycleProbe::live, 1);
    EXPECT_FALSE(static_cast<bool>(second));
    EXPECT_TRUE(static_cast<bool>(first));
    EXPECT_EQ(first(), 2);
  }
  EXPECT_EQ(LifecycleProbe::live, 0);
}
TEST(InplaceDelegateTest, MoveRelocatesTheTargetInsteadOfCopyingBytes) {
  // A correct move constructs the target in its new storage and re-points its self reference;
  // a byte-wise relocation would leave self pointing at the source storage.
  struct SelfReferential final {
    const SelfReferential* self{this};

    SelfReferential() noexcept = default;
    SelfReferential(SelfReferential&& other) noexcept : self(this) { (void)other; }

    auto operator()() noexcept -> bool { return self == this; }
  };

  using NoArgDelegate = InplaceDelegate<bool() noexcept>;
  NoArgDelegate source(SelfReferential{});
  EXPECT_TRUE(source());

  NoArgDelegate moved(std::move(source));
  EXPECT_TRUE(moved());

  NoArgDelegate assigned;
  assigned = std::move(moved);
  EXPECT_TRUE(assigned());
}
TEST(InplaceDelegateTest, ConstructMoveAndInvokeAllocateNothing) {
  int offset = 5;
  int first_result = 0;
  int second_result = 0;
  int third_result = 0;
  IntDelegate source([offset](int value) noexcept { return value + offset; });

  ResetAllocationProbe();
  {
    IntDelegate delegate([offset](int value) noexcept { return value + offset; });
    first_result = delegate(1);

    IntDelegate moved(std::move(delegate));
    second_result = moved(2);

    IntDelegate assigned([](int value) noexcept { return value; });
    assigned = std::move(moved);
    third_result = assigned(3);
  }
  const auto stats = ReadAllocationProbe();

  EXPECT_EQ(source(1), 6);
  EXPECT_EQ(first_result, 6);
  EXPECT_EQ(second_result, 7);
  // assigned took over the captured-offset target from moved, not the value-returning lambda
  // that occupied it before the move assignment.
  EXPECT_EQ(third_result, 8);
  EXPECT_EQ(stats.calls, 0U);
  EXPECT_EQ(stats.bytes, 0U);
}

// ---------------------------------------------------------------------------
// Service lifecycle contract
// ---------------------------------------------------------------------------

template <typename ShutdownResult = std::expected<bool, Status>>
class DummyService {
 public:
  [[nodiscard]] std::expected<void, Status> Start() noexcept {
    state_ = ServiceState::kRunning;
    return {};
  }
  [[nodiscard]] ServiceState State() const noexcept { return state_; }
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept {
    PollResult result;
    result.frames = budget.frames;
    result.pending = now > 0U;
    return result;
  }
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp) noexcept {
    state_ = ServiceState::kStopping;
    return {};
  }
  [[nodiscard]] ShutdownResult PollShutdown(PollBudget, MonotonicTimestamp) noexcept {
    if constexpr (std::same_as<ShutdownResult, std::expected<bool, Status>>) {
      return true;
    } else {
      return PollResult{};
    }
  }
  [[nodiscard]] bool Stopped() const noexcept { return state_ == ServiceState::kStopped; }
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept {
    state_ = ServiceState::kStopped;
    return {};
  }

 private:
  ServiceState state_{ServiceState::kStopped};
};

/** Negative control: no PollOnce member. */
class BrokenService final {
 public:
  [[nodiscard]] std::expected<void, Status> Start() noexcept { return {}; }
  [[nodiscard]] ServiceState State() const noexcept { return ServiceState::kStopped; }
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp) noexcept {
    return {};
  }
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget, MonotonicTimestamp) noexcept {
    return true;
  }
  [[nodiscard]] bool Stopped() const noexcept { return true; }
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept { return {}; }
};

// Each derived type hides exactly one member of the valid service so a single clause of the
// concept decides the result; without these, loosening a clause would go unnoticed.
struct ThrowingStartService : DummyService<> {
  [[nodiscard]] std::expected<void, Status> Start() { return {}; }
};
struct WrongPollOnceResultService : DummyService<> {
  [[nodiscard]] std::expected<void, Status> PollOnce(PollBudget, MonotonicTimestamp) noexcept {
    return {};
  }
};
struct WrongShutdownResultService : DummyService<> {
  [[nodiscard]] std::expected<void, Status> PollShutdown(PollBudget, MonotonicTimestamp) noexcept {
    return {};
  }
};
struct NonNoexceptPollOnceService : DummyService<> {
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget, MonotonicTimestamp) {
    return PollResult{};
  }
};

static_assert(ServiceConcept<DummyService<>>);
static_assert(ServiceConcept<DummyService<std::expected<PollResult, Status>>>);
static_assert(!ServiceConcept<BrokenService>);
static_assert(!ServiceConcept<ThrowingStartService>);
static_assert(!ServiceConcept<WrongPollOnceResultService>);
static_assert(!ServiceConcept<WrongShutdownResultService>);
static_assert(!ServiceConcept<NonNoexceptPollOnceService>);

TEST(PollBudgetTest, DefaultsAndBoundariesFollowTheValidityRule) {
  const PollBudget budget{};
  EXPECT_EQ(budget.commands, 32U);
  EXPECT_EQ(budget.socket_events, 64U);
  EXPECT_EQ(budget.completions, 64U);
  EXPECT_EQ(budget.frames, 64U);
  EXPECT_EQ(budget.timers, 32U);
  EXPECT_TRUE(budget.Valid());

  for (const auto member :
       std::array{&PollBudget::commands, &PollBudget::socket_events, &PollBudget::completions,
                  &PollBudget::frames, &PollBudget::timers}) {
    PollBudget modified{};
    modified.*member = 0U;
    EXPECT_FALSE(modified.Valid());
  }

  PollBudget minimum{};
  minimum.commands = 1U;
  minimum.socket_events = 1U;
  minimum.completions = 1U;
  minimum.frames = 1U;
  minimum.timers = 1U;
  EXPECT_TRUE(minimum.Valid());

  const PollResult result{};
  EXPECT_EQ(result.commands, 0U);
  EXPECT_EQ(result.socket_events, 0U);
  EXPECT_EQ(result.completions, 0U);
  EXPECT_EQ(result.frames, 0U);
  EXPECT_EQ(result.timers, 0U);
  EXPECT_FALSE(result.pending);
  EXPECT_FALSE(result.deadline.has_value());

  PollResult scheduled{};
  scheduled.pending = true;
  scheduled.deadline = MonotonicTimestamp{123'456U};
  ASSERT_TRUE(scheduled.deadline.has_value());
  EXPECT_EQ(*scheduled.deadline, 123'456U);
}

}  // namespace
}  // namespace grpcxx
