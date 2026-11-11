#include <grpcxx/server/grpc_server_service_detail.h>

namespace grpcxx {
GrpcServerService::GrpcServerService(uring::Proactor& p, nexora::CompletionRoutes<>& routes,
                                     Options options) noexcept
    : proactor_(p), routes_(routes), impl_(std::make_unique<Impl>(*this, options)) {}

GrpcServerService::~GrpcServerService() {
  if ((owner_ && owner_ != CallerIdentity()) || advancing_) std::abort();
  if (impl_->accepted && *impl_->accepted >= 0) {
    static_cast<void>(::close(*impl_->accepted));
    impl_->accepted.reset();
  }
  if (impl_->Pending()) std::abort();
  if (impl_->flags.Test<^^h2_detail::RuntimeFlags::registered>())
    static_cast<void>(routes_.Unregister(this));
}

std::uint64_t GrpcServerService::CallerIdentity() const noexcept {
  return h2_detail::CachedThreadId();
}

Status GrpcServerService::RejectTransition() noexcept {
  ++rejected_transitions_;
  return Status{StatusCode::kFailedPrecondition};
}

bool GrpcServerService::Impl::AdoptAccepted(int fd, uring::Proactor& proactor,
                                            nexora::CompletionRoutes<>& routes,
                                            std::uint64_t now_ns) noexcept {
  const auto available = active | exhausted;
  for (const auto slot : ~available | uarch::views::ones) {
    auto& connection = connections[slot];
    const int one = 1;
    static_cast<void>(::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)));
    if (!connection.scratch) connection.scratch = std::make_unique_for_overwrite<IoScratch>();
    ++connection.generation;
    active = active.set(slot);
    if (connection.generation == std::numeric_limits<std::uint32_t>::max())
      exhausted = exhausted.set(slot);
    connection.session = std::make_unique<H2ServerSession>();
    if (options.stream_handler)
      connection.session->SetStreamingHandler(&connection, &Impl::Connection::StreamMessage);
    else
      connection.session->SetRequestHandler(&connection, &Impl::Connection::Request);
    connection.socket.emplace(nexora::IoUringSocket::FromRawFd(fd, nexora::SocketWord::OpenWord()));
    connection.socket->BindProactor(proactor, routes);
    connection.output_size = connection.output_offset = connection.tx_size = 0;
    connection.accepted_at = connection.last_active_at = now_ns;
    connection.bytes_received = connection.peer_eof_at = 0;
    connection.flags.Reset();
    connection.flags.Set<^^h2_detail::RuntimeFlags::established>(options.tls == nullptr);
    connection.tls.reset();
    if (options.tls) {
      auto session = nexora::TlsSession::CreateServer(*options.tls);
      if (session)
        connection.tls.emplace(std::move(*session));
      else
        connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
    }
    return true;
  }
  return false;
}

bool GrpcServerService::Impl::EvictOneForAccept() noexcept {
  bool already_closing = false;
  Connection* best_silent = nullptr;
  Connection* best_idle = nullptr;
  if (!(active | exhausted).is_full()) return true;
  for (const auto slot : active | uarch::views::ones) {
    auto& connection = connections[slot];
    if (connection.flags.Test<^^h2_detail::RuntimeFlags::closing>()) {
      already_closing = true;
      continue;
    }
    if (connection.flags.Test<^^h2_detail::RuntimeFlags::peer_eof>() ||
        !connection.flags.Test<^^h2_detail::RuntimeFlags::handshake_done>()) {
      if (!best_silent || connection.accepted_at < best_silent->accepted_at)
        best_silent = &connection;
    } else if (connection.session && connection.session->ActiveStreams() == 0 &&
               !connection.session->HasPendingOutput()) {
      if (!best_idle || connection.last_active_at < best_idle->last_active_at)
        best_idle = &connection;
    }
  }
  if (Connection* victim = best_silent ? best_silent : best_idle) {
    victim->flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
    return true;
  }
  return already_closing;
}

std::expected<void, Status> GrpcServerService::Start() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (impl_->state != Lifecycle::kCreated) return std::unexpected(RejectTransition());
  owner_ = CallerIdentity();
  auto listener = nexora::TcpListener::Bind(impl_->options.endpoint);
  if (!listener) return std::unexpected(Status{StatusCode::kUnavailable});
  if (!routes_.Register(nexora::CompletionRoute::Bind(*this)))
    return std::unexpected(Status{StatusCode::kResourceExhausted});
  impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(true);
  impl_->listener.emplace(std::move(*listener));
  impl_->state = Lifecycle::kRunning;
  return {};
}

bool GrpcServerService::CanRoute(std::uint8_t id) const noexcept {
  return id == nexora::kSocketSubsystemId || id == nexora::kServerSubsystemId;
}

uring::Result<void> GrpcServerService::Dispatch(const uring::Delivery& delivery) noexcept {
  auto& i = *impl_;
  const auto token = delivery.completion.token;
  if (i.accepting == token && delivery.key.ring_instance == proactor_.Instance()) {
    i.accepted = delivery.completion.result;
    i.accepting.reset();
    return {};
  }
  for (const auto slot : i.active | uarch::views::ones) {
    auto& connection = i.connections[slot];
    const bool rx = connection.socket->RxToken() == token,
               tx = connection.socket->TxToken() == token;
    auto handled = connection.socket->OnCompletion(delivery);
    if (handled && *handled) {
      connection.flags.Set<^^h2_detail::RuntimeFlags::rx_ready>(
          connection.flags.Test<^^h2_detail::RuntimeFlags::rx_ready>() | rx);
      connection.flags.Set<^^h2_detail::RuntimeFlags::tx_ready>(
          connection.flags.Test<^^h2_detail::RuntimeFlags::tx_ready>() | tx);
      return {};
    }
  }
  return std::unexpected(uring::UringErrorCode::kNoEntry);
}

std::expected<PollResult, Status> GrpcServerService::Pump(PollBudget budget, MonotonicTimestamp now,
                                                          PollResult progress) noexcept {
  auto& i = *impl_;
  const auto clock_now = h2_detail::ClockNow(now);

  for (const auto slot : i.active | uarch::views::ones) {
    auto& connection = i.connections[slot];
    if (connection.flags.Test<^^h2_detail::RuntimeFlags::closing>()) continue;
    if (connection.flags.Test<^^h2_detail::RuntimeFlags::peer_eof>() &&
        clock_now >= connection.peer_eof_at &&
        clock_now - connection.peer_eof_at >= i.options.peer_eof_drain_timeout_ns) {
      connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
      continue;
    }
    if (!connection.flags.Test<^^h2_detail::RuntimeFlags::handshake_done>()) {
      if (i.options.handshake_timeout_ns != 0 && clock_now >= connection.accepted_at &&
          clock_now - connection.accepted_at >= i.options.handshake_timeout_ns)
        connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
    } else if (connection.session && connection.session->ActiveStreams() == 0 &&
               !connection.session->HasPendingOutput() && i.options.idle_timeout_ns != 0 &&
               clock_now >= connection.last_active_at &&
               clock_now - connection.last_active_at >= i.options.idle_timeout_ns) {
      connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
    }
  }

  if (i.accepted) {
    const auto fd = *i.accepted;
    if (fd < 0) {
      i.accepted.reset();
      ++progress.socket_events;
    } else if (i.state != Lifecycle::kRunning) {
      static_cast<void>(::close(fd));
      i.accepted.reset();
      ++progress.socket_events;
    } else if (i.AdoptAccepted(fd, proactor_, routes_, clock_now)) {
      i.accepted.reset();
      ++progress.socket_events;
    } else if (!i.EvictOneForAccept()) {
      static_cast<void>(::close(fd));
      i.accepted.reset();
      ++progress.socket_events;
    }
  }

  const auto cursor = i.poll_cursor;
  const auto rotated =
      uarch::Bitmap64::from_raw(std::rotr(i.active.raw(), static_cast<int>(cursor)));
  for (const auto offset : rotated | uarch::views::ones) {
    if (progress.socket_events >= budget.socket_events) break;
    const auto idx = (cursor + offset) & (kMaxConnections - 1U);
    auto& connection = i.connections[idx];
    if (i.state == Lifecycle::kDraining && (now >= i.deadline || connection.Drained()))
      connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
    if (!connection.NeedsWork()) continue;
    ++progress.socket_events;
    i.poll_cursor = (idx + 1U) & (kMaxConnections - 1U);
    if (auto advanced = connection.Advance(clock_now); !advanced) {
      connection.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
      static_cast<void>(connection.Advance(clock_now));
    }
  }

  if (i.accepted && i.state == Lifecycle::kRunning && *i.accepted >= 0 &&
      i.AdoptAccepted(*i.accepted, proactor_, routes_, clock_now)) {
    i.accepted.reset();
  }

  if (i.state == Lifecycle::kRunning && !i.accepting && !i.accepted &&
      progress.commands < budget.commands) {
    auto accepted = i.listener->SubmitAccept(proactor_);
    if (accepted) {
      i.accepting = *accepted;
      ++progress.commands;
    }
  }
  if (i.state == Lifecycle::kDraining) {
    if (i.accepting) static_cast<void>(proactor_.Cancel(*i.accepting));
    if (!i.Pending()) {
      i.listener.reset();
      i.state = Lifecycle::kQuiescent;
    }
    progress.deadline = i.deadline;
  }
  progress.pending = i.Pending();
  return progress;
}

std::expected<PollResult, Status> GrpcServerService::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp now) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || !budget.Valid()) return std::unexpected(RejectTransition());
  h2_detail::DriveScope scope(advancing_);
  return Pump(budget, now);
}

std::expected<void, Status> GrpcServerService::BeginShutdown(MonotonicTimestamp deadline) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  if (impl_->state == Lifecycle::kCreated) {
    impl_->state = Lifecycle::kClosed;
    return {};
  }
  for (const auto slot : impl_->active | uarch::views::ones) {
    auto& connection = impl_->connections[slot];
    if (connection.session && connection.flags.Test<^^h2_detail::RuntimeFlags::established>())
      static_cast<void>(connection.session->GoAway());
  }
  impl_->deadline = deadline;
  impl_->state = Lifecycle::kDraining;
  return {};
}

std::expected<PollResult, Status> GrpcServerService::PollShutdown(PollBudget budget,
                                                                  MonotonicTimestamp now) noexcept {
  return PollOnce(budget, now);
}

std::expected<void, Status> GrpcServerService::Shutdown() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_ || !Stopped()) return std::unexpected(RejectTransition());
  impl_->state = Lifecycle::kClosed;
  for (auto& connection : impl_->connections) connection.scratch.reset();
  if (impl_->flags.Test<^^h2_detail::RuntimeFlags::registered>() && !routes_.Unregister(this))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  impl_->flags.Set<^^h2_detail::RuntimeFlags::registered>(false);
  return {};
}

std::expected<cares::Endpoint, Status> GrpcServerService::LocalAddress() const noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!impl_->listener) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return impl_->listener->LocalEndpoint();
}

bool GrpcServerService::IsCurrent(std::size_t connection, std::int32_t stream,
                                  std::uint64_t generation) const noexcept {
  if (owner_ != CallerIdentity() || connection >= impl_->connections.size()) return false;
  const auto& c = impl_->connections[connection];
  return c.session && c.generation == (generation >> 32U) &&
         c.session->IsCurrent(stream, generation & Impl::kStreamGenMask);
}

std::string_view GrpcServerService::StreamScheme(std::size_t connection, std::int32_t stream,
                                                 std::uint64_t generation) const noexcept {
  if (!IsCurrent(connection, stream, generation)) return {};
  return impl_->connections[connection].session->StreamScheme(stream);
}

std::string_view GrpcServerService::StreamAuthority(std::size_t connection, std::int32_t stream,
                                                    std::uint64_t generation) const noexcept {
  if (!IsCurrent(connection, stream, generation)) return {};
  return impl_->connections[connection].session->StreamAuthority(stream);
}

std::uint64_t GrpcServerService::StreamTimeoutNs(std::size_t connection, std::int32_t stream,
                                                 std::uint64_t generation) const noexcept {
  if (!IsCurrent(connection, stream, generation)) return 0;
  return impl_->connections[connection].session->StreamTimeoutNs(stream);
}

std::expected<void, Status> GrpcServerService::Complete(std::size_t connection, std::int32_t stream,
                                                        std::uint64_t generation,
                                                        std::span<const std::byte> payload,
                                                        int status,
                                                        std::string_view grpc_message) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!IsCurrent(connection, stream, generation))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return impl_->connections[connection].session->Complete(stream, generation & Impl::kStreamGenMask,
                                                          payload, status, {}, grpc_message);
}

ServiceState GrpcServerService::State() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ServiceState::kFailed;
  switch (impl_->state) {
    case Lifecycle::kCreated:
    case Lifecycle::kQuiescent:
    case Lifecycle::kClosed:
      return ServiceState::kStopped;
    case Lifecycle::kRunning:
      return ServiceState::kRunning;
    case Lifecycle::kDraining:
      return ServiceState::kStopping;
  }
  std::unreachable();
}

std::size_t GrpcServerService::ActiveConnections() const noexcept {
  if (owner_ != CallerIdentity()) return 0;
  return impl_->active.count();
}
bool GrpcServerService::Stopped() const noexcept { return State() == ServiceState::kStopped; }
std::uint64_t GrpcServerService::RejectedTransitions() const noexcept {
  return rejected_transitions_;
}

std::expected<void, Status> GrpcServerService::SendMessage(
    std::size_t connection, std::int32_t stream, std::uint64_t generation,
    std::span<const std::byte> payload) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!IsCurrent(connection, stream, generation))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return impl_->connections[connection].session->SendMessage(
      stream, generation & Impl::kStreamGenMask, payload);
}

std::expected<void, Status> GrpcServerService::Finish(std::size_t connection, std::int32_t stream,
                                                      std::uint64_t generation, int status,
                                                      std::string_view grpc_message) noexcept {
  if (owner_ != CallerIdentity()) return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!IsCurrent(connection, stream, generation))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return impl_->connections[connection].session->Finish(stream, generation & Impl::kStreamGenMask,
                                                        status, grpc_message);
}
}  // namespace grpcxx
