#pragma once

#include <nghttp2/nghttp2.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <uarch/uarch.hpp>

namespace grpcxx::h2_detail {

/**
 * Preallocated power-of-two slab pool backing `nghttp2_mem` callbacks.
 *
 * Slabs reside in an uninitialized 116 KiB storage array inside the session `Impl` and are
 * recycled in O(1) via `uarch::Bitmap64` bit-scans and power-of-two address shifts, eliminating
 * steady-state heap allocations while keeping `Reuse()` construction limited to four 64-bit
 * bitmap stores.
 */
class H2SlabPool final {
 public:
  static constexpr std::size_t kTier0Size = 64U;
  static constexpr std::size_t kTier0Shift = 6U;
  static constexpr std::size_t kTier0Count = 64U;
  static constexpr std::size_t kTier0Bytes = kTier0Size * kTier0Count;
  static constexpr std::size_t kTier0Offset = 0U;

  static constexpr std::size_t kTier1Size = 256U;
  static constexpr std::size_t kTier1Shift = 8U;
  static constexpr std::size_t kTier1Count = 64U;
  static constexpr std::size_t kTier1Bytes = kTier1Size * kTier1Count;
  static constexpr std::size_t kTier1Offset = kTier0Offset + kTier0Bytes;

  static constexpr std::size_t kTier2Size = 1024U;
  static constexpr std::size_t kTier2Shift = 10U;
  static constexpr std::size_t kTier2Count = 32U;
  static constexpr std::size_t kTier2Bytes = kTier2Size * kTier2Count;
  static constexpr std::size_t kTier2Offset = kTier1Offset + kTier1Bytes;

  static constexpr std::size_t kTier3Size = 16384U;
  static constexpr std::size_t kTier3Shift = 14U;
  static constexpr std::size_t kTier3Count = 4U;
  static constexpr std::size_t kTier3Bytes = kTier3Size * kTier3Count;
  static constexpr std::size_t kTier3Offset = kTier2Offset + kTier2Bytes;

  static constexpr std::size_t kTotalStorageBytes = kTier3Offset + kTier3Bytes;

  H2SlabPool() noexcept {}
  H2SlabPool(const H2SlabPool&) =
      delete("H2SlabPool owns in-place slab storage; copying is disabled");
  H2SlabPool& operator=(const H2SlabPool&) =
      delete("H2SlabPool owns in-place slab storage; copying is disabled");

  [[nodiscard]] nghttp2_mem MakeNghttp2Mem() noexcept {
    return nghttp2_mem{
        .mem_user_data = this,
        .malloc = &MallocCallback,
        .free = &FreeCallback,
        .calloc = &CallocCallback,
        .realloc = &ReallocCallback,
    };
  }

  [[nodiscard]] void* Allocate(std::size_t size) noexcept {
    const std::size_t effective = size == 0U ? 1U : size;
    if (effective <= kTier0Size) {
      if (const auto bit = free_tier0_.pop_lowest_one()) {
        return storage_ + kTier0Offset + (static_cast<std::size_t>(*bit) << kTier0Shift);
      }
    }
    if (effective <= kTier1Size) {
      if (const auto bit = free_tier1_.pop_lowest_one()) {
        return storage_ + kTier1Offset + (static_cast<std::size_t>(*bit) << kTier1Shift);
      }
    }
    if (effective <= kTier2Size) {
      if (const auto bit = free_tier2_.pop_lowest_one()) {
        return storage_ + kTier2Offset + (static_cast<std::size_t>(*bit) << kTier2Shift);
      }
    }
    if (effective <= kTier3Size) {
      if (const auto bit = free_tier3_.pop_lowest_one()) {
        return storage_ + kTier3Offset + (static_cast<std::size_t>(*bit) << kTier3Shift);
      }
    }
    return std::malloc(effective);
  }

  void Deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }
    const auto addr = reinterpret_cast<std::uintptr_t>(ptr);
    const auto base = reinterpret_cast<std::uintptr_t>(storage_);
    if (addr < base || addr >= base + kTotalStorageBytes) {
      std::free(ptr);
      return;
    }
    const std::size_t offset = static_cast<std::size_t>(addr - base);
    if (offset < kTier1Offset) {
      const auto idx = static_cast<std::uint32_t>((offset - kTier0Offset) >> kTier0Shift);
      free_tier0_ = free_tier0_.set(idx);
      return;
    }
    if (offset < kTier2Offset) {
      const auto idx = static_cast<std::uint32_t>((offset - kTier1Offset) >> kTier1Shift);
      free_tier1_ = free_tier1_.set(idx);
      return;
    }
    if (offset < kTier3Offset) {
      const auto idx = static_cast<std::uint32_t>((offset - kTier2Offset) >> kTier2Shift);
      free_tier2_ = free_tier2_.set(idx);
      return;
    }
    const auto idx = static_cast<std::uint32_t>((offset - kTier3Offset) >> kTier3Shift);
    free_tier3_ = free_tier3_.set(idx);
  }

  [[nodiscard]] void* Reallocate(void* ptr, std::size_t size) noexcept {
    if (ptr == nullptr) {
      return Allocate(size);
    }
    if (size == 0U) {
      Deallocate(ptr);
      return nullptr;
    }
    const std::size_t old_cap = SlabCapacityOf(ptr);
    if (old_cap == 0U) {
      return std::realloc(ptr, size);
    }
    if (size <= old_cap) {
      return ptr;
    }
    void* next = Allocate(size);
    if (uarch::unlikely(next == nullptr)) {
      return nullptr;
    }
    std::memcpy(next, ptr, old_cap);
    Deallocate(ptr);
    return next;
  }

 private:
  [[nodiscard]] std::size_t SlabCapacityOf(const void* ptr) const noexcept {
    const auto addr = reinterpret_cast<std::uintptr_t>(ptr);
    const auto base = reinterpret_cast<std::uintptr_t>(storage_);
    if (addr < base || addr >= base + kTotalStorageBytes) {
      return 0U;
    }
    const std::size_t offset = static_cast<std::size_t>(addr - base);
    if (offset < kTier1Offset) return kTier0Size;
    if (offset < kTier2Offset) return kTier1Size;
    if (offset < kTier3Offset) return kTier2Size;
    return kTier3Size;
  }

  static void* MallocCallback(std::size_t size, void* user_data) noexcept {
    return static_cast<H2SlabPool*>(user_data)->Allocate(size);
  }

  static void FreeCallback(void* ptr, void* user_data) noexcept {
    static_cast<H2SlabPool*>(user_data)->Deallocate(ptr);
  }

  static void* CallocCallback(std::size_t nmemb, std::size_t size, void* user_data) noexcept {
    std::size_t total = 0U;
    if (uarch::unlikely(__builtin_mul_overflow(nmemb, size, &total))) {
      return nullptr;
    }
    void* ptr = static_cast<H2SlabPool*>(user_data)->Allocate(total);
    if (ptr != nullptr && total != 0U) {
      std::memset(ptr, 0, total);
    }
    return ptr;
  }

  static void* ReallocCallback(void* ptr, std::size_t size, void* user_data) noexcept {
    return static_cast<H2SlabPool*>(user_data)->Reallocate(ptr, size);
  }

  uarch::Bitmap64 free_tier0_{uarch::Bitmap64::all()};
  uarch::Bitmap64 free_tier1_{uarch::Bitmap64::all()};
  uarch::Bitmap64 free_tier2_{uarch::Bitmap64::from_raw((1ULL << kTier2Count) - 1ULL)};
  uarch::Bitmap64 free_tier3_{uarch::Bitmap64::from_raw((1ULL << kTier3Count) - 1ULL)};
  alignas(std::max_align_t) std::byte storage_[kTotalStorageBytes];
};

using Nghttp2SessionMemPool = H2SlabPool;

}  // namespace grpcxx::h2_detail
