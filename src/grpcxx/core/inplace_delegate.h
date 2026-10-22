#pragma once

#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace grpcxx {

/**
 * Move-only callable wrapper that stores its target inside fixed inline storage.
 *
 * `InplaceDelegate` carries a callback without ever allocating on the heap: the target object
 * is constructed inside an `alignas(std::max_align_t)` byte buffer of `Capacity` bytes, and
 * destruction, move construction and invocation all go through function pointers selected at
 * compile time. The wrapper therefore replaces heap-allocating type-erased callback wrappers
 * on hot paths where an allocation or a virtual call is unacceptable.
 *
 * Construction is constrained to targets that fit the storage, respect its alignment, and are
 * nothrow constructible from the argument and nothrow move constructible. A target that
 * violates any limit is not constructible, so the failure surfaces through
 * `std::is_constructible` and through concepts as well as at the construction site. Copying is
 * deleted. Moving transfers the target and leaves the source empty, so `operator bool` reports
 * whether a target is present.
 *
 * Invoking an empty delegate is a contract violation and undefined behavior: the implementation
 * calls `std::unreachable()`, which emits a trap on supported targets but carries no portable
 * guarantee.
 */
template <typename Signature, std::size_t Capacity = 64>
class InplaceDelegate;

/**
 * `InplaceDelegate` specialization for a possibly-throwing call signature.
 *
 * The stored target must be invocable with `Args...` and return a value convertible to `R`.
 * The call operator itself is `noexcept`: a target that throws terminates the process, which
 * is the only option when building with exceptions disabled.
 */
template <typename R, typename... Args, std::size_t Capacity>
class InplaceDelegate<R(Args...), Capacity> final {
 public:
  /** Size in bytes of the inline target storage. */
  static constexpr std::size_t kCapacity = Capacity;

  /** Creates an empty delegate. */
  constexpr InplaceDelegate() noexcept = default;

  InplaceDelegate(const InplaceDelegate&) = delete;
  InplaceDelegate& operator=(const InplaceDelegate&) = delete;

  /** Moves the target out of `other`; `other` becomes empty. */
  InplaceDelegate(InplaceDelegate&& other) noexcept { MoveFrom(other); }

  /**
   * Replaces the target with the one owned by `other`.
   *
   * The previously held target is destroyed before the new one is moved in; `other` becomes
   * empty afterwards.
   */
  InplaceDelegate& operator=(InplaceDelegate&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    MoveFrom(other);
    return *this;
  }

  /** Destroys the held target, if any. */
  ~InplaceDelegate() noexcept { Reset(); }

  /**
   * Constructs a target from `fn` inside the inline storage.
   *
   * The target type must fit in `Capacity` bytes, respect `alignof(std::max_align_t)`, be
   * nothrow move constructible and be invocable with `Args...`.
   */
  template <typename F>
    requires(!std::same_as<std::remove_cvref_t<F>, InplaceDelegate> &&
             std::is_nothrow_move_constructible_v<std::decay_t<F>> &&
             std::is_nothrow_constructible_v<std::decay_t<F>, F &&> &&
             (sizeof(std::decay_t<F>) <= Capacity) &&
             (alignof(std::decay_t<F>) <= alignof(std::max_align_t)) &&
             std::is_invocable_r_v<R, std::decay_t<F>&, Args...>)
  InplaceDelegate(F&& fn) noexcept {
    using Target = std::decay_t<F>;
    static_assert(sizeof(Target) <= Capacity, "callable does not fit InplaceDelegate capacity");
    static_assert(alignof(Target) <= alignof(std::max_align_t),
                  "callable alignment exceeds InplaceDelegate storage alignment");

    std::construct_at(reinterpret_cast<Target*>(storage_), std::forward<F>(fn));
    invoke_ = &Invoke<Target>;
    destroy_ = &Destroy<Target>;
    move_ = &MoveTarget<Target>;
  }

  /** Reports whether a target is present. */
  [[nodiscard]] constexpr explicit operator bool() const noexcept { return invoke_ != nullptr; }

  /**
   * Invokes the held target.
   *
   * Invoking an empty delegate is undefined behavior; see the class comment.
   */
  R operator()(Args... args) noexcept {
    if (invoke_ == nullptr) {
      std::unreachable();
    }
    return invoke_(storage_, std::forward<Args>(args)...);
  }

 private:
  template <typename F>
  static R Invoke(void* storage, Args... args) noexcept {
    return std::invoke(*std::launder(reinterpret_cast<F*>(storage)), std::forward<Args>(args)...);
  }

  template <typename F>
  static void Destroy(void* storage) noexcept {
    std::destroy_at(std::launder(reinterpret_cast<F*>(storage)));
  }

  template <typename F>
  static void MoveTarget(void* destination, void* source) noexcept {
    auto* target = std::launder(reinterpret_cast<F*>(source));
    std::construct_at(reinterpret_cast<F*>(destination), std::move(*target));
    std::destroy_at(target);
  }

  void MoveFrom(InplaceDelegate& other) noexcept {
    if (other.invoke_ == nullptr) {
      return;
    }

    other.move_(storage_, other.storage_);
    invoke_ = other.invoke_;
    destroy_ = other.destroy_;
    move_ = other.move_;
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  void Reset() noexcept {
    if (destroy_ != nullptr) {
      destroy_(storage_);
    }
    invoke_ = nullptr;
    destroy_ = nullptr;
    move_ = nullptr;
  }

  alignas(std::max_align_t) std::byte storage_[Capacity]{};
  R (*invoke_)(void*, Args...) noexcept {nullptr};
  void (*destroy_)(void*) noexcept {nullptr};
  void (*move_)(void*, void*) noexcept {nullptr};
};

/**
 * `InplaceDelegate` specialization for a `noexcept` call signature.
 *
 * The stored target must be nothrow invocable with `Args...` and return a value convertible to
 * `R`. Requiring a `noexcept` target at the type level keeps callbacks on the stepper hot path
 * from accidentally depending on exception propagation.
 */
template <typename R, typename... Args, std::size_t Capacity>
class InplaceDelegate<R(Args...) noexcept, Capacity> final {
 public:
  /** Size in bytes of the inline target storage. */
  static constexpr std::size_t kCapacity = Capacity;

  /** Creates an empty delegate. */
  constexpr InplaceDelegate() noexcept = default;

  InplaceDelegate(const InplaceDelegate&) = delete;
  InplaceDelegate& operator=(const InplaceDelegate&) = delete;

  /** Moves the target out of `other`; `other` becomes empty. */
  InplaceDelegate(InplaceDelegate&& other) noexcept { MoveFrom(other); }

  /**
   * Replaces the target with the one owned by `other`.
   *
   * The previously held target is destroyed before the new one is moved in; `other` becomes
   * empty afterwards.
   */
  InplaceDelegate& operator=(InplaceDelegate&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    MoveFrom(other);
    return *this;
  }

  /** Destroys the held target, if any. */
  ~InplaceDelegate() noexcept { Reset(); }

  /**
   * Constructs a target from `fn` inside the inline storage.
   *
   * The target type must fit in `Capacity` bytes, respect `alignof(std::max_align_t)`, be
   * nothrow move constructible and be nothrow invocable with `Args...`.
   */
  template <typename F>
    requires(!std::same_as<std::remove_cvref_t<F>, InplaceDelegate> &&
             std::is_nothrow_move_constructible_v<std::decay_t<F>> &&
             std::is_nothrow_constructible_v<std::decay_t<F>, F &&> &&
             (sizeof(std::decay_t<F>) <= Capacity) &&
             (alignof(std::decay_t<F>) <= alignof(std::max_align_t)) &&
             std::is_nothrow_invocable_r_v<R, std::decay_t<F>&, Args...>)
  InplaceDelegate(F&& fn) noexcept {
    using Target = std::decay_t<F>;
    static_assert(sizeof(Target) <= Capacity, "callable does not fit InplaceDelegate capacity");
    static_assert(alignof(Target) <= alignof(std::max_align_t),
                  "callable alignment exceeds InplaceDelegate storage alignment");

    std::construct_at(reinterpret_cast<Target*>(storage_), std::forward<F>(fn));
    invoke_ = &Invoke<Target>;
    destroy_ = &Destroy<Target>;
    move_ = &MoveTarget<Target>;
  }

  /** Reports whether a target is present. */
  [[nodiscard]] constexpr explicit operator bool() const noexcept { return invoke_ != nullptr; }

  /**
   * Invokes the held target.
   *
   * Invoking an empty delegate is undefined behavior; see the class comment.
   */
  R operator()(Args... args) noexcept {
    if (invoke_ == nullptr) {
      std::unreachable();
    }
    return invoke_(storage_, std::forward<Args>(args)...);
  }

 private:
  template <typename F>
  static R Invoke(void* storage, Args... args) noexcept {
    return std::invoke(*std::launder(reinterpret_cast<F*>(storage)), std::forward<Args>(args)...);
  }

  template <typename F>
  static void Destroy(void* storage) noexcept {
    std::destroy_at(std::launder(reinterpret_cast<F*>(storage)));
  }

  template <typename F>
  static void MoveTarget(void* destination, void* source) noexcept {
    auto* target = std::launder(reinterpret_cast<F*>(source));
    std::construct_at(reinterpret_cast<F*>(destination), std::move(*target));
    std::destroy_at(target);
  }

  void MoveFrom(InplaceDelegate& other) noexcept {
    if (other.invoke_ == nullptr) {
      return;
    }

    other.move_(storage_, other.storage_);
    invoke_ = other.invoke_;
    destroy_ = other.destroy_;
    move_ = other.move_;
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  void Reset() noexcept {
    if (destroy_ != nullptr) {
      destroy_(storage_);
    }
    invoke_ = nullptr;
    destroy_ = nullptr;
    move_ = nullptr;
  }

  alignas(std::max_align_t) std::byte storage_[Capacity]{};
  R (*invoke_)(void*, Args...) noexcept {nullptr};
  void (*destroy_)(void*) noexcept {nullptr};
  void (*move_)(void*, void*) noexcept {nullptr};
};

}  // namespace grpcxx
