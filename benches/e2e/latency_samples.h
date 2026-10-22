#pragma once
#include <benchmark/benchmark.h>
#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ranges>
#include <span>
#include <vector>

namespace e2e_bench {
using Clock = std::chrono::steady_clock;
/** Storage is allocated before iteration timing; each completed message contributes one sample. */
class Samples final {
 public:
  explicit Samples(const benchmark::State& state)
      : ns_(static_cast<std::size_t>(state.max_iterations)) {}
  void Begin() noexcept {
    started_ = Clock::now();
    _mm_lfence();
    cycles_start_ = __rdtsc();
  }
  void End() noexcept {
    _mm_lfence();
    cycles_ += __rdtsc() - cycles_start_;
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started_).count();
    ns_[count_++] = static_cast<std::uint64_t>(elapsed);
  }
  void Report(benchmark::State& state, std::size_t bytes) {
    auto samples = std::span{ns_}.first(count_);
    std::ranges::sort(samples);
    const auto percentile = [&](double fraction) {
      if (samples.empty()) return 0.0;
      const auto rank = static_cast<std::size_t>(std::ceil(fraction * static_cast<double>(count_)));
      return static_cast<double>(samples[std::min(count_ - 1, rank - 1)]);
    };
    state.counters["p50_ns"] = percentile(.5);
    state.counters["p99_ns"] = percentile(.99);
    state.counters["p999_ns"] = percentile(.999);
    state.counters["cycles_per_msg"] =
        count_ ? static_cast<double>(cycles_) / static_cast<double>(count_) : 0;
    state.SetItemsProcessed(static_cast<std::int64_t>(count_));
    state.SetBytesProcessed(static_cast<std::int64_t>(count_ * bytes));
  }

 private:
  std::vector<std::uint64_t> ns_;
  Clock::time_point started_{};
  std::uint64_t cycles_start_{};
  std::uint64_t cycles_{};
  std::size_t count_{};
};

}  // namespace e2e_bench
