#pragma once

#include <array>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <type_traits>
#include <uarch/uarch.hpp>
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
 * Invoking an empty delegate is a contract violation. That path calls `std::unreachable()`,
 * so the compiler may assume a target is present and may delete the empty check.
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

  InplaceDelegate(const InplaceDelegate&) =
      delete("InplaceDelegate is move-only; copying is disabled");
  InplaceDelegate& operator=(const InplaceDelegate&) =
      delete("InplaceDelegate is move-only; copying is disabled");

  /** Moves the target out of `other`; `other` becomes empty. */
  constexpr InplaceDelegate(InplaceDelegate&& other) noexcept { MoveFrom(other); }

  /**
   * Replaces the target with the one owned by `other`.
   *
   * The held target is destroyed before the incoming one is moved in; `other` is empty
   * afterwards.
   */
  constexpr InplaceDelegate& operator=(InplaceDelegate&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    MoveFrom(other);
    return *this;
  }

  /** Destroys the held target, if any. */
  constexpr ~InplaceDelegate() noexcept { Reset(); }

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
  constexpr InplaceDelegate(F&& fn) noexcept {
    using Target = std::decay_t<F>;
    static_assert(sizeof(Target) <= Capacity, "callable does not fit InplaceDelegate capacity");
    static_assert(alignof(Target) <= alignof(std::max_align_t),
                  "callable alignment exceeds InplaceDelegate storage alignment");

    if constexpr (std::is_empty_v<Target> && std::is_trivially_default_constructible_v<Target> &&
                  std::is_trivially_destructible_v<Target>) {
      if !consteval {
        std::construct_at(static_cast<Target*>(static_cast<void*>(storage_.data())),
                          std::forward<F>(fn));
      }
      invoke_ = &InvokeStateless<Target>;
      destroy_ = nullptr;
      move_ = &MoveStateless;
    } else {
      std::construct_at(static_cast<Target*>(static_cast<void*>(storage_.data())),
                        std::forward<F>(fn));
      invoke_ = &Invoke<Target>;
      destroy_ = &Destroy<Target>;
      move_ = &MoveTarget<Target>;
    }
  }

  /** Reports whether a target is present. */
  [[nodiscard, gnu::always_inline]] constexpr explicit operator bool() const noexcept {
    return invoke_ != nullptr;
  }

  /**
   * Invokes the held target.
   *
   * Invoking an empty delegate is undefined behavior; see the class comment.
   */
  [[gnu::always_inline]] R operator()(Args... args) noexcept {
    if (uarch::unlikely(invoke_ == nullptr)) {
      std::unreachable();
    }
    return invoke_(storage_.data(), std::forward<Args>(args)...);
  }

 private:
  template <typename F>
  static R InvokeStateless(void*, Args... args) noexcept {
    F target{};
    return std::invoke(target, std::forward<Args>(args)...);
  }

  static void MoveStateless(void*, void*) noexcept {}

  template <typename F>
  static R Invoke(void* storage, Args... args) noexcept {
    return std::invoke(*std::launder(static_cast<F*>(storage)), std::forward<Args>(args)...);
  }

  template <typename F>
  static void Destroy(void* storage) noexcept {
    std::destroy_at(std::launder(static_cast<F*>(storage)));
  }

  template <typename F>
  static void MoveTarget(void* destination, void* source) noexcept {
    auto* target = std::launder(static_cast<F*>(source));
    std::construct_at(static_cast<F*>(destination), std::move(*target));
    std::destroy_at(target);
  }

  constexpr void MoveFrom(InplaceDelegate& other) noexcept {
    if (other.invoke_ == nullptr) {
      return;
    }

    other.move_(storage_.data(), other.storage_.data());
    invoke_ = other.invoke_;
    destroy_ = other.destroy_;
    move_ = other.move_;
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  constexpr void Reset() noexcept {
    if (destroy_ != nullptr) {
      destroy_(storage_.data());
    }
    invoke_ = nullptr;
    destroy_ = nullptr;
    move_ = nullptr;
  }

  alignas(std::max_align_t) std::array<std::byte, Capacity> storage_{};
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

  InplaceDelegate(const InplaceDelegate&) =
      delete("InplaceDelegate is move-only; copying is disabled");
  InplaceDelegate& operator=(const InplaceDelegate&) =
      delete("InplaceDelegate is move-only; copying is disabled");

  /** Moves the target out of `other`; `other` becomes empty. */
  constexpr InplaceDelegate(InplaceDelegate&& other) noexcept { MoveFrom(other); }

  /**
   * Replaces the target with the one owned by `other`.
   *
   * The held target is destroyed before the incoming one is moved in; `other` is empty
   * afterwards.
   */
  constexpr InplaceDelegate& operator=(InplaceDelegate&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    MoveFrom(other);
    return *this;
  }

  /** Destroys the held target, if any. */
  constexpr ~InplaceDelegate() noexcept { Reset(); }

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
  constexpr InplaceDelegate(F&& fn) noexcept {
    using Target = std::decay_t<F>;
    static_assert(sizeof(Target) <= Capacity, "callable does not fit InplaceDelegate capacity");
    static_assert(alignof(Target) <= alignof(std::max_align_t),
                  "callable alignment exceeds InplaceDelegate storage alignment");

    if constexpr (std::is_empty_v<Target> && std::is_trivially_default_constructible_v<Target> &&
                  std::is_trivially_destructible_v<Target>) {
      if !consteval {
        std::construct_at(static_cast<Target*>(static_cast<void*>(storage_.data())),
                          std::forward<F>(fn));
      }
      invoke_ = &InvokeStateless<Target>;
      destroy_ = nullptr;
      move_ = &MoveStateless;
    } else {
      std::construct_at(static_cast<Target*>(static_cast<void*>(storage_.data())),
                        std::forward<F>(fn));
      invoke_ = &Invoke<Target>;
      destroy_ = &Destroy<Target>;
      move_ = &MoveTarget<Target>;
    }
  }

  /** Reports whether a target is present. */
  [[nodiscard, gnu::always_inline]] constexpr explicit operator bool() const noexcept {
    return invoke_ != nullptr;
  }

  /**
   * Invokes the held target.
   *
   * Invoking an empty delegate is undefined behavior; see the class comment.
   */
  [[gnu::always_inline]] R operator()(Args... args) noexcept {
    if (uarch::unlikely(invoke_ == nullptr)) {
      std::unreachable();
    }
    return invoke_(storage_.data(), std::forward<Args>(args)...);
  }

 private:
  template <typename F>
  static R InvokeStateless(void*, Args... args) noexcept {
    F target{};
    return std::invoke(target, std::forward<Args>(args)...);
  }

  static void MoveStateless(void*, void*) noexcept {}

  template <typename F>
  static R Invoke(void* storage, Args... args) noexcept {
    return std::invoke(*std::launder(static_cast<F*>(storage)), std::forward<Args>(args)...);
  }

  template <typename F>
  static void Destroy(void* storage) noexcept {
    std::destroy_at(std::launder(static_cast<F*>(storage)));
  }

  template <typename F>
  static void MoveTarget(void* destination, void* source) noexcept {
    auto* target = std::launder(static_cast<F*>(source));
    std::construct_at(static_cast<F*>(destination), std::move(*target));
    std::destroy_at(target);
  }

  constexpr void MoveFrom(InplaceDelegate& other) noexcept {
    if (other.invoke_ == nullptr) {
      return;
    }

    other.move_(storage_.data(), other.storage_.data());
    invoke_ = other.invoke_;
    destroy_ = other.destroy_;
    move_ = other.move_;
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  constexpr void Reset() noexcept {
    if (destroy_ != nullptr) {
      destroy_(storage_.data());
    }
    invoke_ = nullptr;
    destroy_ = nullptr;
    move_ = nullptr;
  }

  alignas(std::max_align_t) std::array<std::byte, Capacity> storage_{};
  R (*invoke_)(void*, Args...) noexcept {nullptr};
  void (*destroy_)(void*) noexcept {nullptr};
  void (*move_)(void*, void*) noexcept {nullptr};
};

}  // namespace grpcxx
