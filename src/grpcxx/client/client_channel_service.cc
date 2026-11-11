#include <grpcxx/client/client_channel_service_detail.h>

namespace grpcxx {
ClientChannelService::ClientChannelService(uring::Proactor& p, nexora::CompletionRoutes<>& routes,
                                           Options options) noexcept
    : proactor_(p), routes_(routes), impl_(std::make_unique<Impl>(options)) {}

ClientChannelService::ClientChannelService(uring::Proactor& p, nexora::CompletionRoutes<>& routes,
                                           cares::Resolver<>& resolver, Options options) noexcept
    : ClientChannelService(p, routes, options) {
  impl_->resolver = &resolver;
}

ClientChannelService::~ClientChannelService() {
  if ((owner_ && owner_ != CallerIdentity()) || advancing_) std::abort();
  if (impl_->query && impl_->resolver)
    static_cast<void>(impl_->resolver->CancelQuery(*impl_->query));
  if (impl_->socket && impl_->socket->InFlightOps()) std::abort();
  if (impl_->flags.Test<^^h2_detail::RuntimeFlags::registered>())
    static_cast<void>(routes_.Unregister(this));
}

std::uint64_t ClientChannelService::CallerIdentity() const noexcept {
  return h2_detail::CachedThreadId();
}

Status ClientChannelService::RejectTransition() noexcept {
  ++rejected_transitions_;
  return Status{StatusCode::kFailedPrecondition};
}

std::expected<void, Status> ClientChannelService::Start() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (impl_->state != ChannelState::kIdle) return std::unexpected(RejectTransition());
  owner_ = CallerIdentity();
  if (!impl_->options.target.empty() && !impl_->resolver)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (!routes_.Register(nexora::CompletionRoute::Bind(*this)))
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(true);
  impl_->state = ChannelState::kConnecting;
  if (!impl_->options.target.empty()) {
    auto submitted =
        impl_->resolver->SubmitEndpoints(impl_->options.target, impl_->options.port,
                                         cares::EndpointsCallback{&Impl::Resolved, impl_.get()});
    if (!submitted) {
      static_cast<void>(routes_.Unregister(this));
      impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(false);
      impl_->state = ChannelState::kIdle;
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
    if (!impl_->resolution) impl_->query = *submitted;
    return {};
  }
  const auto list = !impl_->options.endpoints.empty()
                        ? impl_->options.endpoints
                        : std::span<const cares::Endpoint>(&impl_->options.endpoint, 1);
  if (!impl_->SeedEndpoints(list)) {
    static_cast<void>(routes_.Unregister(this));
    impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(false);
    impl_->state = ChannelState::kIdle;
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  }
  return impl_->ConnectNext(proactor_, routes_, h2_detail::SteadyNowNs());
}

std::expected<void, Status> ClientChannelService::ReconcileEndpoints(
    std::span<const cares::Endpoint> endpoints) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || impl_->state == ChannelState::kIdle ||
      impl_->state == ChannelState::kShuttingDown || impl_->state == ChannelState::kClosed)
    return std::unexpected(RejectTransition());
  if (!impl_->SeedEndpoints(endpoints))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (!impl_->socket || !impl_->endpoints.FindEndpoint(impl_->active_endpoint).has_value())
    return impl_->RotateEndpoint(proactor_, routes_, h2_detail::SteadyNowNs());
  return {};
}

std::expected<void, Status> ClientChannelService::ReResolve() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || impl_->state == ChannelState::kIdle ||
      impl_->state == ChannelState::kShuttingDown || impl_->state == ChannelState::kClosed)
    return std::unexpected(RejectTransition());
  if (impl_->options.target.empty() || !impl_->resolver)
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (impl_->query) return {};
  impl_->resolution.reset();
  auto submitted =
      impl_->resolver->SubmitEndpoints(impl_->options.target, impl_->options.port,
                                       cares::EndpointsCallback{&Impl::Resolved, impl_.get()});
  if (!submitted) return std::unexpected(Status{StatusCode::kUnavailable});
  if (!impl_->resolution) impl_->query = *submitted;
  return {};
}

bool ClientChannelService::CanRoute(std::uint8_t id) const noexcept {
  return id == nexora::kSocketSubsystemId || (impl_->resolver && impl_->resolver->CanRoute(id));
}

uring::Result<void> ClientChannelService::Dispatch(const uring::Delivery& delivery) noexcept {
  auto& i = *impl_;
  if (i.resolver && i.resolver->CanRoute(uring::Subsystem(delivery.completion.token))) {
    i.resolver->OnCompletions(std::span<const uring::Delivery>(&delivery, 1));
    return {};
  }
  if (!i.socket) return std::unexpected(uring::UringErrorCode::kNoEntry);
  const auto token = delivery.completion.token;
  const bool rx = i.socket->RxToken() == token, tx = i.socket->TxToken() == token,
             connect = i.socket->ConnectToken() == token;
  auto handled = i.socket->OnCompletion(delivery);
  if (!handled || !*handled) return std::unexpected(uring::UringErrorCode::kNoEntry);
  i.flags.Set<^^h2_detail::RuntimeFlags::rx_ready>(
      i.flags.Test<^^h2_detail::RuntimeFlags::rx_ready>() | rx);
  i.flags.Set<^^h2_detail::RuntimeFlags::tx_ready>(
      i.flags.Test<^^h2_detail::RuntimeFlags::tx_ready>() | tx);
  i.flags.Set<^^h2_detail::RuntimeFlags::connect_ready>(
      i.flags.Test<^^h2_detail::RuntimeFlags::connect_ready>() | connect);
  return {};
}

std::expected<PollResult, Status> ClientChannelService::Pump(PollBudget budget,
                                                             MonotonicTimestamp now,
                                                             PollResult progress) noexcept {
  auto& i = *impl_;
  const auto clock_now = h2_detail::ClockNow(now);
  const bool output_drained = i.state != ChannelState::kShuttingDown ||
                              (!i.session->HasPendingOutput() && i.output_offset == i.output_size &&
                               (!i.socket || !i.socket->TxToken()) &&
                               !i.flags.Test<^^h2_detail::RuntimeFlags::tx_ready>() &&
                               (!i.tls || !i.tls->HasPendingCiphertextOut()));
  if (i.state == ChannelState::kShuttingDown &&
      ((i.active.is_empty() && output_drained) || now >= i.deadline)) {
    if (!i.active.is_empty()) i.FailActiveCalls(4);
    if (i.query) [[unlikely]]
      i.CancelResolver();
    if (i.socket) {
      static_cast<void>(i.socket->SubmitCancel());
      if (!i.socket->InFlightOps()) {
        i.socket.reset();
        i.tls.reset();
        i.state = ChannelState::kClosed;
      }
    } else {
      i.state = ChannelState::kClosed;
    }
  } else if (i.state != ChannelState::kClosed) {
    if (auto advanced = i.Advance(proactor_, routes_, clock_now); !advanced) {
      i.TearDownAndFailover(proactor_, routes_, clock_now, 14);
    }
  }
  for (const auto slot : i.active | uarch::views::ones) {
    if (progress.completions >= budget.completions) break;
    auto& call = i.calls[slot];
    if (!call.flags.Test<^^h2_detail::RuntimeFlags::ready>()) continue;
    auto completion = std::move(call.completion);
    i.active = i.active.clear(slot);
    call.flags.Set<^^h2_detail::RuntimeFlags::ready>(false);
    ++progress.completions;
    if (completion) completion(call.stream, call.status, call.response.RawBytes());
  }
  progress.pending =
      !i.active.is_empty() || (i.state == ChannelState::kShuttingDown && i.socket.has_value());
  if (i.state == ChannelState::kShuttingDown) progress.deadline = i.deadline;
  return progress;
}

std::expected<PollResult, Status> ClientChannelService::PollOnce(PollBudget budget,
                                                                 MonotonicTimestamp now) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || !budget.Valid()) return std::unexpected(RejectTransition());
  h2_detail::DriveScope scope(advancing_);
  if (impl_->resolver && (impl_->query || impl_->state != ChannelState::kReady)) [[unlikely]] {
    impl_->StepResolver();
  }
  return Pump(budget, now);
}

std::expected<void, Status> ClientChannelService::BeginShutdown(
    MonotonicTimestamp deadline) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  if (impl_->state == ChannelState::kIdle) {
    impl_->state = ChannelState::kClosed;
    return {};
  }
  if (impl_->state == ChannelState::kReady && impl_->session)
    static_cast<void>(impl_->session->GoAway());
  impl_->deadline = deadline;
  impl_->state = ChannelState::kShuttingDown;
  return {};
}

std::expected<PollResult, Status> ClientChannelService::PollShutdown(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  return PollOnce(budget, now);
}

std::expected<void, Status> ClientChannelService::Shutdown() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || !Stopped()) return std::unexpected(RejectTransition());
  if (impl_->flags.Test<^^h2_detail::RuntimeFlags::registered>() && !routes_.Unregister(this))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(false);
  return {};
}

ClientChannelService::ChannelState ClientChannelService::GetState() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ChannelState::kTransientFailure;
  return impl_->state;
}

std::string_view ClientChannelService::LastStatusMessage() const noexcept {
  return {impl_->last_message.data(), impl_->last_message_size};
}

ServiceState ClientChannelService::State() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ServiceState::kFailed;
  switch (impl_->state) {
    case ChannelState::kIdle:
    case ChannelState::kClosed:
      return ServiceState::kStopped;
    case ChannelState::kConnecting:
      return ServiceState::kStarting;
    case ChannelState::kReady:
      return ServiceState::kRunning;
    case ChannelState::kTransientFailure:
      return ServiceState::kFailed;
    case ChannelState::kShuttingDown:
      return ServiceState::kStopping;
  }
  std::unreachable();
}

bool ClientChannelService::Stopped() const noexcept { return State() == ServiceState::kStopped; }
std::uint64_t ClientChannelService::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

std::expected<std::int32_t, Status> ClientChannelService::CallUnary(
    std::string_view path, std::span<const std::byte> payload,
    UnaryCompletion completion) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!completion) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (impl_->state == ChannelState::kConnecting || impl_->state == ChannelState::kTransientFailure)
    return std::unexpected(Status{StatusCode::kUnavailable});
  if (impl_->state != ChannelState::kReady || !impl_->session)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  const auto slot = impl_->active.lowest_zero();
  if (!slot || *slot >= impl_->calls.size())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto& call = impl_->calls[*slot];
  auto submitted = impl_->session->Submit(path, payload, &call, &Impl::Call::Done);
  if (!submitted) return std::unexpected(submitted.error());
  call.completion = std::move(completion);
  call.message = {};
  call.stream = *submitted;
  call.flags.Set<^^h2_detail::RuntimeFlags::ready>(false);
  impl_->active = impl_->active.set(*slot);
  return *submitted;
}

std::expected<std::int32_t, Status> ClientChannelService::CallUnary(
    std::string_view path, std::span<const std::byte> payload, void* context,
    Completion completion) noexcept {
  if (!completion) return std::unexpected(Status{StatusCode::kInvalidArgument});
  return CallUnary(path, payload,
                   UnaryCompletion{[context, completion](std::int32_t id, int status,
                                                         std::span<const std::byte> data) noexcept {
                     completion(context, id, status, data);
                   }});
}

std::expected<std::int32_t, Status> ClientChannelService::OpenStream(
    std::string_view path, MessageCompletion message, UnaryCompletion completion) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!message || !completion) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  if (impl_->state == ChannelState::kConnecting || impl_->state == ChannelState::kTransientFailure)
    return std::unexpected(Status{StatusCode::kUnavailable});
  if (impl_->state != ChannelState::kReady || !impl_->session)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  const auto slot = impl_->active.lowest_zero();
  if (!slot || *slot >= impl_->calls.size())
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto& call = impl_->calls[*slot];
  auto opened = impl_->session->Open(path, &call, &Impl::Call::Message, &Impl::Call::Done);
  if (!opened) return opened;
  call.message = std::move(message);
  call.completion = std::move(completion);
  call.stream = *opened;
  call.flags.Set<^^h2_detail::RuntimeFlags::ready>(false);
  impl_->active = impl_->active.set(*slot);
  return opened;
}

std::expected<void, Status> ClientChannelService::SendMessage(
    std::int32_t id, std::span<const std::byte> payload) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!impl_->session) return std::unexpected(Status{StatusCode::kUnavailable});
  return impl_->session->SendMessage(id, payload);
}

std::expected<void, Status> ClientChannelService::HalfClose(std::int32_t id) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!impl_->session) return std::unexpected(Status{StatusCode::kUnavailable});
  return impl_->session->HalfClose(id);
}

std::expected<void, Status> ClientChannelService::Cancel(std::int32_t id) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!impl_->session) return std::unexpected(Status{StatusCode::kNotFound});
  auto canceled = impl_->session->Cancel(id);
  if (!canceled) return canceled;
  for (const auto slot : impl_->active | uarch::views::ones) {
    auto& call = impl_->calls[slot];
    if (call.stream == id) {
      call.status = 1;
      call.response.Reset();
      call.flags.Set<^^h2_detail::RuntimeFlags::ready>(true);
      break;
    }
  }
  return {};
}
}  // namespace grpcxx
