#include "grpcxx/services/health_service.h"

#ifndef FMT_USE_EXCEPTIONS
#define FMT_USE_EXCEPTIONS 0
#endif
#include <fmt/format.h>

#include <algorithm>
#include <ranges>
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

struct CopyNameResult {
  std::uint8_t length{0};
  bool overflow{false};
};

/**
 * Copies `text` into `dest`, reserving one byte so the buffer can stay bounded.
 *
 * Constant evaluation uses an index loop. The runtime path uses `copy_n`.
 */
[[nodiscard]] constexpr CopyNameResult CopyName(std::string_view text,
                                                std::span<char> dest) noexcept {
  if (dest.empty() || text.size() >= dest.size()) {
    return CopyNameResult{0, true};
  }
  if consteval {
    for (std::size_t index = 0; index < text.size(); ++index) {
      dest[index] = text[index];
    }
  } else {
    if (const auto bytes = std::span<const char>{text}; !bytes.empty()) {
      std::ranges::copy(bytes, dest.data());
    }
  }
  return CopyNameResult{static_cast<std::uint8_t>(text.size()), false};
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

void HealthService::Watcher::Push(ServingStatus status) noexcept {
  static_assert(uarch::PowerOfTwo<3>::value == kQueueCapacity);
  if (count == kQueueCapacity) {
    head =
        static_cast<std::uint8_t>(uarch::PowerOfTwo<3>::rem(static_cast<std::uint64_t>(head) + 1U));
    --count;
  }
  const auto index = static_cast<std::uint8_t>(
      uarch::PowerOfTwo<3>::rem(static_cast<std::uint64_t>(head) + count));
  events[index] = status;
  ++count;
}

std::size_t HealthService::Watcher::Drain(std::span<ServingStatus> out) noexcept {
  const auto copies = std::min(out.size(), static_cast<std::size_t>(count));
  for (const auto index : std::views::iota(std::size_t{0}, copies)) {
    const auto slot = static_cast<std::uint8_t>(
        uarch::PowerOfTwo<3>::rem(static_cast<std::uint64_t>(head) + index));
    out[index] = events[slot];
  }
  head = static_cast<std::uint8_t>(
      uarch::PowerOfTwo<3>::rem(static_cast<std::uint64_t>(head) + copies));
  count = static_cast<std::uint8_t>(count - copies);
  return copies;
}

HealthService::HealthService() noexcept = default;

std::expected<void, Status> HealthService::Start() noexcept {
  if (state_ != ServiceState::kStopped) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "HealthService is {} and cannot start",
                                       ServiceStateName(state_)));
  }
  state_ = ServiceState::kStarting;
  quiescent_ = false;
  state_ = ServiceState::kRunning;
  return {};
}

ServiceState HealthService::State() const noexcept { return state_; }

std::expected<PollResult, Status> HealthService::PollOnce(PollBudget budget,
                                                          MonotonicTimestamp now) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  last_poll_ = now;
  static_cast<void>(budget);
  return PollResult{};
}

std::expected<void, Status> HealthService::BeginShutdown(MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopping) {
    return {};
  }
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "HealthService is {} and cannot drain",
                                       ServiceStateName(state_)));
  }
  static_cast<void>(now);
  state_ = ServiceState::kStopping;
  return {};
}

std::expected<bool, Status> HealthService::PollShutdown(PollBudget budget,
                                                        MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopped) {
    return true;
  }
  if (state_ != ServiceState::kStopping) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "HealthService is {} and cannot poll shutdown",
                                       ServiceStateName(state_)));
  }
  last_poll_ = now;
  static_cast<void>(budget);
  for (Watcher& watcher : watchers_) {
    watcher = Watcher{};
  }
  watchers_used_ = uarch::Bitmap64::empty();
  quiescent_ = true;
  return true;
}

bool HealthService::Stopped() const noexcept {
  return state_ == ServiceState::kStopped || (state_ == ServiceState::kStopping && quiescent_);
}

std::expected<void, Status> HealthService::Shutdown() noexcept {
  if (state_ == ServiceState::kStopped || state_ == ServiceState::kFailed) {
    ClearRuntime();
    state_ = ServiceState::kStopped;
    quiescent_ = true;
    return {};
  }
  if (state_ != ServiceState::kStopping || !quiescent_) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "HealthService is {} and is not drained",
                                       ServiceStateName(state_)));
  }
  ClearRuntime();
  state_ = ServiceState::kStopped;
  quiescent_ = true;
  return {};
}

std::expected<ServingStatus, Status> HealthService::Check(std::string_view service) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  if (service.empty()) {
    return overall_;
  }
  if (auto found = FindService(service); found != nullptr) {
    return found->status;
  }
  return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown health service {}", service));
}

std::expected<std::uint32_t, Status> HealthService::Watch(std::string_view service) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  const auto slot_index = watchers_used_.lowest_zero();
  if (!slot_index.has_value() || *slot_index >= kMaxWatchers) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "health watcher capacity exhausted"});
  }
  Watcher& slot = watchers_[*slot_index];
  const auto copied = CopyName(service, slot.service.chars);
  if (copied.overflow) {
    return std::unexpected(ErrorStatus(StatusCode::kInvalidArgument,
                                       "health service name length {} exceeds {}", service.size(),
                                       kNameCapacity - 1));
  }
  slot.service.length = copied.length;
  slot.head = 0;
  slot.count = 0;
  slot.id = next_watcher_id_;
  if (next_watcher_id_ == std::uint32_t{0xffffffffU}) {
    next_watcher_id_ = 1;
  } else {
    ++next_watcher_id_;
  }
  watchers_used_ = watchers_used_.set(*slot_index);
  ServingStatus current = ServingStatus::kServiceUnknown;
  if (service.empty()) {
    current = overall_;
  } else if (auto found = FindService(service); found != nullptr) {
    current = found->status;
  }
  slot.Push(current);
  return slot.id;
}

std::expected<std::size_t, Status> HealthService::DrainWatch(
    std::uint32_t watcher, std::span<ServingStatus> out) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  for (const auto index : watchers_used_ | uarch::views::ones) {
    if (watchers_[index].id == watcher) {
      return watchers_[index].Drain(out);
    }
  }
  return std::unexpected(ErrorStatus(StatusCode::kNotFound, "unknown health watcher {}", watcher));
}

std::expected<void, Status> HealthService::SetServingStatus(std::string_view service,
                                                            ServingStatus status) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  if (service.empty()) {
    overall_ = status;
    Notify(service, status);
    return {};
  }
  if (auto found = FindService(service); found != nullptr) {
    found->status = status;
    Notify(service, status);
    return {};
  }
  const auto slot = services_used_.lowest_zero();
  if (!slot.has_value() || *slot >= kMaxServices) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "health service capacity exhausted"});
  }
  ServiceRecord& record = services_[*slot];
  const auto copied = CopyName(service, record.name.chars);
  if (copied.overflow) {
    return std::unexpected(ErrorStatus(StatusCode::kInvalidArgument,
                                       "health service name length {} exceeds {}", service.size(),
                                       kNameCapacity - 1));
  }
  record.name.length = copied.length;
  record.status = status;
  services_used_ = services_used_.set(*slot);
  Notify(service, status);
  return {};
}

std::expected<void, Status> HealthService::RequireRunning() const noexcept {
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(ErrorStatus(StatusCode::kFailedPrecondition,
                                       "HealthService is {} not running",
                                       ServiceStateName(state_)));
  }
  return {};
}

HealthService::ServiceRecord* HealthService::FindService(std::string_view service) noexcept {
  for (const auto index : services_used_ | uarch::views::ones) {
    if (services_[index].name.View() == service) {
      return &services_[index];
    }
  }
  return nullptr;
}

void HealthService::Notify(std::string_view service, ServingStatus status) noexcept {
  for (const auto index : watchers_used_ | uarch::views::ones) {
    if (watchers_[index].service.View() == service) {
      watchers_[index].Push(status);
    }
  }
}

void HealthService::ClearRuntime() noexcept {
  for (ServiceRecord& record : services_) {
    record = ServiceRecord{};
  }
  services_used_ = uarch::Bitmap64::empty();
  for (Watcher& watcher : watchers_) {
    watcher = Watcher{};
  }
  watchers_used_ = uarch::Bitmap64::empty();
  overall_ = ServingStatus::kServing;
  next_watcher_id_ = 1;
}

}  // namespace grpcxx
