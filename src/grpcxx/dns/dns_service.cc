#include "grpcxx/dns/dns_service.h"

namespace grpcxx {

DnsService::DnsService() noexcept = default;

DnsService::~DnsService() = default;

std::expected<void, Status> DnsService::RequireRunning() const noexcept {
  if (state_ != ServiceState::kRunning) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not running"});
  }
  return {};
}

std::expected<void, Status> DnsService::Start() noexcept {
  if (state_ != ServiceState::kStopped) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not stopped"});
  }
  state_ = ServiceState::kStarting;
  if (auto opened = channel_.Open(); !opened) {
    state_ = ServiceState::kFailed;
    return opened;
  }
  quiescent_ = false;
  state_ = ServiceState::kRunning;
  return {};
}

ServiceState DnsService::State() const noexcept { return state_; }

std::expected<PollResult, Status> DnsService::PollOnce(PollBudget budget,
                                                       MonotonicTimestamp now) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  return channel_.Process(budget, now);
}

std::expected<void, Status> DnsService::BeginShutdown(MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopping) {
    return {};
  }
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  static_cast<void>(now);
  state_ = ServiceState::kStopping;
  quiescent_ = false;
  channel_.Close();
  return {};
}

std::expected<bool, Status> DnsService::PollShutdown(PollBudget budget,
                                                     MonotonicTimestamp now) noexcept {
  if (state_ == ServiceState::kStopped) {
    return true;
  }
  if (state_ != ServiceState::kStopping) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "dns service is not shutting down"});
  }
  if (channel_.HasPending()) {
    if (auto processed = channel_.Process(budget, now); !processed) {
      return std::unexpected(processed.error());
    }
  }
  quiescent_ = !channel_.HasPending();
  return quiescent_;
}

bool DnsService::Stopped() const noexcept {
  return state_ == ServiceState::kStopped || (state_ == ServiceState::kStopping && quiescent_);
}

std::expected<void, Status> DnsService::Shutdown() noexcept {
  if (state_ == ServiceState::kStopped || state_ == ServiceState::kFailed) {
    channel_.Close();
    state_ = ServiceState::kStopped;
    quiescent_ = true;
    return {};
  }
  if (state_ != ServiceState::kStopping || !quiescent_) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not drained"});
  }
  channel_.Close();
  state_ = ServiceState::kStopped;
  quiescent_ = true;
  return {};
}

std::expected<void, Status> DnsService::Resolve(std::string_view host, std::uint16_t port,
                                                ResolveCallback callback, void* user) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  return channel_.Resolve(host, port, callback, user);
}

std::size_t DnsService::PendingResolutions() const noexcept { return channel_.PendingCount(); }

}  // namespace grpcxx
