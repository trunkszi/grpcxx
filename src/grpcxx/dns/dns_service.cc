#include "grpcxx/dns/dns_service.h"

namespace grpcxx {

DnsService::DnsService() noexcept = default;

DnsService::~DnsService() = default;

ServiceState DnsService::Lifecycle() const noexcept {
  return life_.get<^^DnsServiceMetaSpec::state>();
}

void DnsService::SetLifecycle(ServiceState state) noexcept {
  life_ = life_.with<^^DnsServiceMetaSpec::state>(state);
}

bool DnsService::Quiescent() const noexcept {
  return life_.test<^^DnsServiceMetaSpec::quiescent>();
}

void DnsService::SetQuiescent(bool quiescent) noexcept {
  life_ = life_.with<^^DnsServiceMetaSpec::quiescent>(quiescent);
}

std::expected<void, Status> DnsService::RequireRunning() const noexcept {
  if (Lifecycle() != ServiceState::kRunning) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not running"});
  }
  return {};
}

std::expected<void, Status> DnsService::Start() noexcept {
  if (Lifecycle() != ServiceState::kStopped) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not stopped"});
  }
  SetLifecycle(ServiceState::kStarting);
  if (auto opened = channel_.Open(); !opened) {
    SetLifecycle(ServiceState::kFailed);
    return opened;
  }
  SetQuiescent(false);
  SetLifecycle(ServiceState::kRunning);
  return {};
}

ServiceState DnsService::State() const noexcept { return Lifecycle(); }

std::expected<PollResult, Status> DnsService::PollOnce(PollBudget budget,
                                                       MonotonicTimestamp now) noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  return channel_.Process(budget, now);
}

std::expected<void, Status> DnsService::BeginShutdown(MonotonicTimestamp now) noexcept {
  if (Lifecycle() == ServiceState::kStopping) {
    return {};
  }
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  static_cast<void>(now);
  SetLifecycle(ServiceState::kStopping);
  SetQuiescent(false);
  channel_.Close();
  return {};
}

std::expected<bool, Status> DnsService::PollShutdown(PollBudget budget,
                                                     MonotonicTimestamp now) noexcept {
  if (Lifecycle() == ServiceState::kStopped) {
    return true;
  }
  if (Lifecycle() != ServiceState::kStopping) {
    return std::unexpected(
        Status{StatusCode::kFailedPrecondition, "dns service is not shutting down"});
  }
  if (channel_.HasPending()) {
    if (auto processed = channel_.Process(budget, now); !processed) {
      return std::unexpected(processed.error());
    }
  }
  SetQuiescent(!channel_.HasPending());
  return Quiescent();
}

bool DnsService::Stopped() const noexcept {
  return Lifecycle() == ServiceState::kStopped ||
         (Lifecycle() == ServiceState::kStopping && Quiescent());
}

std::expected<void, Status> DnsService::Shutdown() noexcept {
  if (Lifecycle() == ServiceState::kStopped || Lifecycle() == ServiceState::kFailed) {
    channel_.Close();
    SetLifecycle(ServiceState::kStopped);
    SetQuiescent(true);
    return {};
  }
  if (Lifecycle() != ServiceState::kStopping || !Quiescent()) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "dns service is not drained"});
  }
  channel_.Close();
  SetLifecycle(ServiceState::kStopped);
  SetQuiescent(true);
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
