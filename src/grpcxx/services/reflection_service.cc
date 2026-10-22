#include "grpcxx/services/reflection_service.h"

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#endif
#ifndef FMT_USE_EXCEPTIONS
#define FMT_USE_EXCEPTIONS 0
#endif
#include <fmt/format.h>

#include <algorithm>
#include <string>
#include <utility>

namespace grpcxx {
namespace {

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
[[nodiscard]] Status ErrorStatus(StatusCode code, fmt::format_string<Args...> format,
                                 Args&&... args) noexcept {
  std::array<char, 192> buffer{};
  const auto written =
      fmt::format_to_n(buffer.data(), buffer.size() - 1, format, std::forward<Args>(args)...);
  const auto length = static_cast<std::size_t>(written.out - buffer.data());
  return Status{code, std::string(buffer.data(), length)};
}

}  // namespace

ReflectionService::ReflectionService() noexcept = default;

std::expected<void, Status> ReflectionService::Start() noexcept {
  if (state_ != ServiceState::kStopped) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot start",
                                       ServiceStateName(state_)));
  }
  state_ = ServiceState::kStarting;
  quiescent_ = false;
  state_ = ServiceState::kRunning;
  return {};
}

ServiceState ReflectionService::State() const noexcept { return state_; }

std::expected<PollResult, Status> ReflectionService::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp now) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  last_poll_ = now;
  static_cast<void>(budget);
  return PollResult{};
}

std::expected<void, Status> ReflectionService::BeginShutdown(MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopping) {
    return {};
  }
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot drain",
                                       ServiceStateName(state_)));
  }
  static_cast<void>(now);
  state_ = ServiceState::kStopping;
  return {};
}

std::expected<bool, Status> ReflectionService::PollShutdown(PollBudget budget,
                                                            MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopped) {
    return true;
  }
  if (state_ != ServiceState::kStopping) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and cannot poll shutdown",
                                       ServiceStateName(state_)));
  }
  last_poll_ = now;
  static_cast<void>(budget);
  quiescent_ = true;
  return true;
}

bool ReflectionService::Stopped() const noexcept {
  return state_ == ServiceState::kStopped || (state_ == ServiceState::kStopping && quiescent_);
}

std::expected<void, Status> ReflectionService::Shutdown() noexcept {
  if (state_ == ServiceState::kStopped || state_ == ServiceState::kFailed) {
    ClearRegistry();
    state_ = ServiceState::kStopped;
    quiescent_ = true;
    return {};
  }
  if (state_ != ServiceState::kStopping || !quiescent_) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} and is not drained",
                                       ServiceStateName(state_)));
  }
  ClearRegistry();
  state_ = ServiceState::kStopped;
  quiescent_ = true;
  return {};
}

std::expected<std::size_t, Status> ReflectionService::ListServices(
    std::span<std::string_view> out) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  auto live = std::span<const Slot>{slots_.data(), slot_count_};
  const auto count = static_cast<std::size_t>(
      std::ranges::count_if(live, [](const Slot& slot) { return slot.used; }));
  if (out.size() < count) {
    return std::unexpected(ErrorStatus(StatusCode::kResourceExhausted,
                                       "service span {} is shorter than {}", out.size(), count));
  }
  std::size_t index = 0;
  for (const Slot& slot : live | std::views::filter([](const Slot& item) { return item.used; })) {
    out[index] = slot.name;
    ++index;
  }
  return count;
}

std::expected<std::size_t, Status> ReflectionService::ListMethods(
    std::string_view symbol, std::span<ContractMethod> out) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  if (const Slot* service = Find(symbol); service != nullptr) {
    return WriteMethods(*service, out);
  }
  const auto slash = symbol.rfind('/');
  const auto dot = symbol.rfind('.');
  const bool slash_found = slash != std::string_view::npos;
  const auto split = slash_found ? slash : dot;
  if (split == std::string_view::npos || split == 0 || split + 1 >= symbol.size()) {
    return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown service {}", symbol));
  }
  const auto service_name = symbol.substr(0, split);
  const auto method_name = symbol.substr(split + 1);
  const Slot* service = Find(service_name);
  if (service == nullptr) {
    return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown service {}", symbol));
  }
  const auto methods = std::span<const ContractMethod>{service->methods, service->method_count};
  if (auto found = std::ranges::find_if(
          methods,
          [method_name](const ContractMethod& method) { return method.name == method_name; });
      found != std::ranges::end(methods)) {
    if (out.empty()) {
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
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "ReflectionService is {} not running",
                                       ServiceStateName(state_)));
  }
  return {};
}

ReflectionService::Slot* ReflectionService::FindMutable(std::string_view name) noexcept {
  auto live = std::span<Slot>{slots_.data(), slot_count_};
  if (auto found =
          std::ranges::find_if(live, [name](const Slot& slot) { return slot.name == name; });
      found != std::ranges::end(live)) {
    return &*found;
  }
  return nullptr;
}

const ReflectionService::Slot* ReflectionService::Find(std::string_view name) const noexcept {
  auto live = std::span<const Slot>{slots_.data(), slot_count_};
  if (auto found =
          std::ranges::find_if(live, [name](const Slot& slot) { return slot.name == name; });
      found != std::ranges::end(live)) {
    return &*found;
  }
  return nullptr;
}

std::expected<std::size_t, Status> ReflectionService::WriteMethods(
    const Slot& slot, std::span<ContractMethod> out) const noexcept {
  const auto count = static_cast<std::size_t>(slot.method_count);
  if (out.size() < count) {
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
  for (Slot& slot : slots_) {
    slot = Slot{};
  }
  slot_count_ = 0;
}

}  // namespace grpcxx
