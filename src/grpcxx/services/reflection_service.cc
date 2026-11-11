#include "grpcxx/services/reflection_service.h"

#ifndef FMT_USE_EXCEPTIONS
#define FMT_USE_EXCEPTIONS 0
#endif
#include <fmt/format.h>

#include <algorithm>
#include <string>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

constexpr auto kStartPatch =
    uarch::BitPatch<ReflectionServiceMetaSpec>{}
        .set_field<^^ReflectionServiceMetaSpec::state>(ServiceState::kRunning)
        .set_flag<^^ReflectionServiceMetaSpec::quiescent>(false);

constexpr auto kDrainedStoppingPattern =
    uarch::BitPattern<ReflectionServiceMetaSpec>{}
        .field_eq<^^ReflectionServiceMetaSpec::state>(ServiceState::kStopping)
        .flag_set<^^ReflectionServiceMetaSpec::quiescent>();

constexpr auto kShutdownResetPatch =
    uarch::BitPatch<ReflectionServiceMetaSpec>{}
        .set_field<^^ReflectionServiceMetaSpec::state>(ServiceState::kStopped)
        .set_flag<^^ReflectionServiceMetaSpec::quiescent>(true);

[[nodiscard]] constexpr std::string_view ServiceStateName(ServiceState state) noexcept {
  switch (state) {
    case ServiceState::kStopped:
      return "stopped";
    case ServiceState::kStarting:
      return "starting";
    case ServiceState::kRunning:
      return "running";
    case ServiceState::kStopping:
      return "stopping";
    case ServiceState::kFailed:
      return "failed";
  }
  std::unreachable();
}

[[nodiscard]] constexpr std::string_view RpcKindName(RpcKind kind) noexcept {
  switch (kind) {
    case RpcKind::kUnary:
      return "unary";
    case RpcKind::kServerStreaming:
      return "server_streaming";
    case RpcKind::kClientStreaming:
      return "client_streaming";
    case RpcKind::kBidiStreaming:
      return "bidi_streaming";
  }
  std::unreachable();
}

template <typename... Args>
[[nodiscard, gnu::cold, gnu::noinline]] Status ErrorStatus(StatusCode code,
                                                           fmt::format_string<Args...> format,
                                                           Args&&... args) noexcept {
  uarch::cold_path();
  std::array<char, 192> buffer{};
  const auto written =
      fmt::format_to_n(buffer.data(), buffer.size() - 1, format, std::forward<Args>(args)...);
  const auto length = static_cast<std::size_t>(written.out - buffer.data());
  return Status{code, std::string(buffer.data(), length)};
}

}  // namespace

ReflectionService::ReflectionService() noexcept = default;

std::expected<void, Status> ReflectionService::Start() noexcept {
  const auto state = meta_.get<^^ReflectionServiceMetaSpec::state>();
  if (uarch::unlikely(state != ServiceState::kStopped)) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot start",
                                       ServiceStateName(state)));
  }
  meta_ = kStartPatch.apply(meta_);
  return {};
}

ServiceState ReflectionService::State() const noexcept {
  return meta_.get<^^ReflectionServiceMetaSpec::state>();
}

std::expected<PollResult, Status> ReflectionService::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp now) noexcept {
  if (auto running = RequireRunning(); uarch::unlikely(!running)) {
    return std::unexpected(running.error());
  }
  last_poll_ = now;
  static_cast<void>(budget);
  return PollResult{};
}

std::expected<void, Status> ReflectionService::BeginShutdown(MonotonicTimestamp now) noexcept {
  const auto state = meta_.get<^^ReflectionServiceMetaSpec::state>();
  if (state == ServiceState::kStopping) {
    return {};
  }
  if (uarch::unlikely(state != ServiceState::kRunning)) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot drain",
                                       ServiceStateName(state)));
  }
  static_cast<void>(now);
  meta_ = meta_.with<^^ReflectionServiceMetaSpec::state>(ServiceState::kStopping);
  return {};
}

std::expected<bool, Status> ReflectionService::PollShutdown(PollBudget budget,
                                                            MonotonicTimestamp now) noexcept {
  const auto state = meta_.get<^^ReflectionServiceMetaSpec::state>();
  if (state == ServiceState::kStopped) {
    return true;
  }
  if (uarch::unlikely(state != ServiceState::kStopping)) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot poll shutdown",
                                       ServiceStateName(state)));
  }
  last_poll_ = now;
  static_cast<void>(budget);
  meta_ = meta_.with_flags<^^ReflectionServiceMetaSpec::quiescent>();
  return true;
}

bool ReflectionService::Stopped() const noexcept {
  return uarch::eager_any(meta_.get<^^ReflectionServiceMetaSpec::state>() == ServiceState::kStopped,
                          kDrainedStoppingPattern.matches(meta_));
}

std::expected<void, Status> ReflectionService::Shutdown() noexcept {
  const auto state = meta_.get<^^ReflectionServiceMetaSpec::state>();
  if (uarch::eager_any(state == ServiceState::kStopped, state == ServiceState::kFailed)) {
    ClearRegistry();
    meta_ = kShutdownResetPatch.apply(meta_);
    return {};
  }
  if (uarch::unlikely(!kDrainedStoppingPattern.matches(meta_))) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and is not drained",
                                       ServiceStateName(state)));
  }
  ClearRegistry();
  meta_ = kShutdownResetPatch.apply(meta_);
  return {};
}

std::expected<std::size_t, Status> ReflectionService::ListServices(
    std::span<std::string_view> out) noexcept {
  if (auto running = RequireRunning(); uarch::unlikely(!running)) {
    return std::unexpected(running.error());
  }
  const std::size_t count = slots_used_.count();
  if (uarch::unlikely(out.size() < count)) {
    return std::unexpected(ErrorStatus(StatusCode::kResourceExhausted,
                                       "service span {} is shorter than {}", out.size(), count));
  }
  std::size_t index = 0;
  for (const auto idx : slots_used_ | uarch::views::ones) {
    out[index] = slots_[idx].name;
    ++index;
  }
  return count;
}

std::expected<std::size_t, Status> ReflectionService::ListMethods(
    std::string_view symbol, std::span<ContractMethod> out) noexcept {
  if (auto running = RequireRunning(); uarch::unlikely(!running)) {
    return std::unexpected(running.error());
  }
  if (const Slot* service = Find(symbol); service != nullptr) {
    return WriteMethods(*service, out);
  }
  const auto slash = symbol.rfind('/');
  const auto dot = symbol.rfind('.');
  const bool slash_found = slash != std::string_view::npos;
  const auto split = slash_found ? slash : dot;
  if (uarch::unlikely(split == std::string_view::npos || split == 0 ||
                      split + 1 >= symbol.size())) {
    return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown service {}", symbol));
  }
  const auto service_name = symbol.substr(0, split);
  const auto method_name = symbol.substr(split + 1);
  const Slot* service = Find(service_name);
  if (uarch::unlikely(service == nullptr)) {
    return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown service {}", symbol));
  }
  const auto methods = std::span<const ContractMethod>{service->methods, service->method_count};
  if (auto found = std::ranges::find_if(
          methods,
          [method_name](const ContractMethod& method) { return method.name == method_name; });
      found != std::ranges::end(methods)) {
    if (uarch::unlikely(out.empty())) {
      return std::unexpected(ErrorStatus(StatusCode::kResourceExhausted,
                                         "method span is too small for {}",
                                         RpcKindName(found->kind)));
    }
    out[0] = *found;
    return 1U;
  }
  return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown service {}", service_name));
}

std::expected<void, Status> ReflectionService::RequireRunning() const noexcept {
  const auto state = meta_.get<^^ReflectionServiceMetaSpec::state>();
  if (uarch::unlikely(state != ServiceState::kRunning)) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} not running",
                                       ServiceStateName(state)));
  }
  return {};
}

ReflectionService::Slot* ReflectionService::FindMutable(std::string_view name) noexcept {
  const std::uint64_t target_hash = uarch::fnv1a64(name);
  for (const auto idx : slots_used_ | uarch::views::ones) {
    Slot& slot = slots_[idx];
    if (uarch::eager_all(slot.name_hash == target_hash, slot.name.size() == name.size()) &&
        slot.name == name) {
      return &slot;
    }
  }
  return nullptr;
}

const ReflectionService::Slot* ReflectionService::Find(std::string_view name) const noexcept {
  const std::uint64_t target_hash = uarch::fnv1a64(name);
  for (const auto idx : slots_used_ | uarch::views::ones) {
    const Slot& slot = slots_[idx];
    if (uarch::eager_all(slot.name_hash == target_hash, slot.name.size() == name.size()) &&
        slot.name == name) {
      return &slot;
    }
  }
  return nullptr;
}

std::expected<std::size_t, Status> ReflectionService::WriteMethods(
    const Slot& slot, std::span<ContractMethod> out) const noexcept {
  const auto count = static_cast<std::size_t>(slot.method_count);
  if (uarch::unlikely(out.size() < count)) {
    return std::unexpected(ErrorStatus(StatusCode::kResourceExhausted,
                                       "method span {} is shorter than {}", out.size(), count));
  }
  const auto methods = std::span<const ContractMethod>{slot.methods, count};
  std::size_t index = 0;
  for (const ContractMethod& method : methods) {
    out[index] = method;
    ++index;
  }
  return count;
}

void ReflectionService::ClearRegistry() noexcept {
  for (const auto idx : slots_used_ | uarch::views::ones) {
    slots_[idx] = Slot{};
  }
  slots_used_ = uarch::Bitmap64::empty();
}

}  // namespace grpcxx
