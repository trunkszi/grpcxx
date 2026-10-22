#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/server/grpc_server_service.h>
#include <grpcxx/uring/uring_acceptor.h>
#include <grpcxx/uring/uring_socket.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>
#include <optional>

namespace grpcxx {
namespace {
/** Rejects recursive lifecycle progress without affecting unary delivery from handlers. */
struct AdvanceGuard {
  bool& active;
  explicit AdvanceGuard(bool& flag) noexcept : active(flag) { active = true; }
  ~AdvanceGuard() { active = false; }
};
}  // namespace
struct GrpcServerService::Impl {
  /** Tracks wire boundaries while nghttp2 validates frame semantics. */
  struct FrameCursor {
    std::size_t preface{}, header{}, payload{};
    std::array<std::byte, 3> length{};
    std::span<const std::byte> Chunk(std::span<const std::byte> bytes) const noexcept {
      const auto remaining = preface ? preface : header < 9 ? 9 - header : payload;
      return bytes.first(std::min(bytes.size(), remaining));
    }
    bool Consume(std::span<const std::byte> bytes) noexcept {
      if (preface) {
        preface -= bytes.size();
        return false;
      }
      if (header < 9) {
        for (std::size_t n = 0; n < bytes.size() && header + n < 3; ++n)
          length[header + n] = bytes[n];
        header += bytes.size();
        if (header != 9) return false;
        payload = (std::to_integer<std::size_t>(length[0]) << 16) |
                  (std::to_integer<std::size_t>(length[1]) << 8) |
                  std::to_integer<std::size_t>(length[2]);
        if (payload != 0) return false;
      } else {
        payload -= bytes.size();
        if (payload != 0) return false;
      }
      header = 0;
      return true;
    }
  };

  static constexpr std::size_t kConnections = 16;
  struct Connection {
    Nghttp2ServerSession session;
    FrameCursor ingress{.preface = 24};
    std::size_t received_size{}, received_offset{};
    bool output_empty{};
    UringSocket socket;
    Impl* owner{};
    std::size_t index{};
    std::uint64_t generation_offset{}, last_generation{};
    std::array<std::byte, 16384> receive{};
    std::array<std::byte, 131072> transmit{};
    std::size_t transmit_size{}, sent{};
    std::optional<OperationId> reading, writing;
    bool used{}, read_done{}, write_done{}, cancelled{}, closed{}, write_cancelled{};
    int read_result{}, write_result{};
    bool Pending() const noexcept { return reading || writing; }
  };
  ProactorService& proactor;
  GrpcServerService& service;
  Options options;
  PollResult* request_progress{};
  Lifecycle lifecycle{Lifecycle::kCreated};
  MonotonicTimestamp deadline{};
  std::array<std::byte, sizeof(sockaddr_storage)> address{};
  std::size_t address_size{}, next_connection{};
  std::array<Connection, kConnections> connections;
  UringAcceptor acceptor;
  std::optional<OperationId> accepting;
  int accept_result{};
  bool accept_done{}, accept_cancelled{}, force{};
  Impl(ProactorService& p, GrpcServerService& s, Options o) noexcept
      : proactor(p), service(s), options(o) {
    if (o.address.size() <= address.size()) {
      address_size = o.address.size();
      std::ranges::copy(o.address, address.begin());
    }
    options.address = {};
    for (std::size_t n = 0; n < connections.size(); ++n) {
      auto& c = connections[n];
      c.owner = this;
      c.index = n;
      c.session.SetRequestHandler(&c, &OnRequest);
    }
  }
  static void OnRequest(void* context, std::int32_t stream, std::uint64_t generation,
                        std::string_view path, std::span<const std::byte> payload) noexcept {
    auto& connection = *static_cast<Connection*>(context);
    auto& owner = *connection.owner;
    if (generation > std::numeric_limits<std::uint64_t>::max() - connection.generation_offset) {
      connection.closed = true;
      return;
    }
    const auto external_generation = generation + connection.generation_offset;
    connection.last_generation = std::max(connection.last_generation, external_generation);
    if (owner.options.handler) {
      if (owner.request_progress) ++owner.request_progress->completions;
      owner.options.handler(owner.options.context, owner.service, connection.index, stream,
                            external_generation, path, payload);
    } else {
      auto rejected = connection.session.Complete(stream, generation, {}, 12);
      if (!rejected) connection.closed = true;
    }
  }
  /** A replacement socket starts a fresh HTTP/2 session after every old I/O CQE retires.
   * Native session initialization is connection setup and may allocate inside nghttp2.
   * Exposed generations remain monotonic across replacements, invalidating old responders.
   */
  std::expected<void, Status> Reuse(Connection& c) noexcept {
    if (!c.used) return {};
    c.socket.Close();
    std::destroy_at(&c.session);
    std::construct_at(&c.session);
    c.generation_offset = c.last_generation;
    c.session.SetRequestHandler(&c, &OnRequest);
    c.closed = c.cancelled = c.write_cancelled = c.read_done = c.write_done = false;
    c.sent = c.transmit_size = 0;
    c.ingress = FrameCursor{.preface = 24};
    c.received_size = c.received_offset = 0;
    c.output_empty = false;
    if (!c.session.Ready()) {
      c.closed = true;
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
    return {};
  }
  bool Pending() const noexcept {
    if (accepting) return true;
    return std::ranges::any_of(connections, [](const auto& c) { return c.Pending(); });
  }
  std::expected<void, Status> Accept() noexcept {
    if (accepting || lifecycle != Lifecycle::kRunning) return {};
    auto result = proactor.SubmitAccept(acceptor.Fd(), [this](int n) noexcept {
      accept_result = n;
      accept_done = true;
    });
    if (!result) return std::unexpected(result.error());
    accepting = *result;
    return {};
  }
  std::expected<void, Status> Pump(Connection& c, PollResult& progress,
                                   PollBudget budget) noexcept {
    if (c.write_done && progress.socket_events < budget.socket_events) {
      ++progress.socket_events;
      c.write_done = false;
      c.writing.reset();
      if (c.write_result <= 0)
        c.closed = true;
      else {
        c.sent += static_cast<std::size_t>(c.write_result);
        if (c.sent == c.transmit_size) c.sent = c.transmit_size = 0;
      }
    }
    if (c.read_done && progress.socket_events < budget.socket_events) {
      ++progress.socket_events;
      c.read_done = false;
      c.reading.reset();
      if (c.read_result <= 0)
        c.closed = true;
      else {
        c.received_size = static_cast<std::size_t>(c.read_result);
        c.received_offset = 0;
      }
    }
    while (!c.closed && c.received_offset < c.received_size && progress.frames < budget.frames &&
           progress.completions < budget.completions) {
      auto chunk =
          c.ingress.Chunk(std::span{c.receive}.first(c.received_size).subspan(c.received_offset));
      c.output_empty = false;
      request_progress = &progress;
      auto received = c.session.Receive(chunk);
      request_progress = nullptr;
      if (!received || *received == 0 || *received > chunk.size()) {
        c.closed = true;
        break;
      }
      c.received_offset += *received;
      if (c.ingress.Consume(chunk.first(*received))) ++progress.frames;
    }
    if (c.closed) {
      if (force && c.writing && !c.write_done && !c.write_cancelled &&
          progress.commands < budget.commands) {
        auto result = proactor.Cancel(*c.writing);
        if (!result) return std::unexpected(result.error());
        c.write_cancelled = true;
        ++progress.commands;
      }
      if (c.reading && !c.read_done && !c.cancelled && progress.commands < budget.commands) {
        auto result = proactor.Cancel(*c.reading);
        if (!result) return std::unexpected(result.error());
        c.cancelled = true;
        ++progress.commands;
      }
      if (!c.Pending()) {
        c.socket.Close();
        c.sent = c.transmit_size = 0;
      }
      return {};
    }
    if (!c.writing && c.transmit_size == 0 && progress.frames < budget.frames) {
      auto outbound = c.session.Send();
      if (!outbound) return std::unexpected(Status{StatusCode::kInternal});
      if (outbound->size() > c.transmit.size())
        return std::unexpected(Status{StatusCode::kResourceExhausted});
      std::ranges::copy(*outbound, c.transmit.begin());
      c.transmit_size = outbound->size();
      c.output_empty = outbound->empty();
      if (!outbound->empty()) ++progress.frames;
    }
    if (!c.writing && c.transmit_size != 0 && progress.commands < budget.commands) {
      auto sent = proactor.SubmitSend(c.socket.Fd(),
                                      std::span{c.transmit}.first(c.transmit_size).subspan(c.sent),
                                      [&c](int n) noexcept {
                                        c.write_result = n;
                                        c.write_done = true;
                                      });
      if (!sent) return std::unexpected(sent.error());
      c.writing = *sent;
      ++progress.commands;
    }
    if (!c.reading && !c.read_done && !c.cancelled && c.received_offset == c.received_size &&
        progress.commands < budget.commands) {
      auto received = proactor.SubmitRecv(c.socket.Fd(), c.receive, [&c](int n) noexcept {
        c.read_result = n;
        c.read_done = true;
      });
      if (!received) return std::unexpected(received.error());
      c.reading = *received;
      ++progress.commands;
    }
    return {};
  }
};
GrpcServerService::GrpcServerService(ProactorService& p, Options options) noexcept
    : proactor_(p), impl_(new(std::nothrow) Impl(p, *this, options)) {}
GrpcServerService::~GrpcServerService() {
  if (owner_ && owner_ != CallerIdentity()) std::abort();
  if (advancing_) std::abort();
  if (impl_ && impl_->Pending()) std::abort();
}
Status GrpcServerService::RejectTransition() noexcept {
  if (rejected_transitions_ != std::numeric_limits<std::uint64_t>::max()) ++rejected_transitions_;
  return Status{StatusCode::kFailedPrecondition};
}
std::uint64_t GrpcServerService::RejectedTransitions() const noexcept {
  return (!owner_ || owner_ == CallerIdentity()) ? rejected_transitions_ : 0;
}
std::expected<void, Status> GrpcServerService::Start() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  const auto identity = CallerIdentity();
  if (identity == 0) return std::unexpected(Status{StatusCode::kInvalidArgument});
  owner_ = identity;
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto& i = *impl_;
  if (i.lifecycle != Lifecycle::kCreated) return std::unexpected(RejectTransition());
  if (i.address_size < sizeof(sa_family_t))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  for (const auto& c : i.connections)
    if (!c.session.Ready()) return std::unexpected(Status{StatusCode::kUnavailable});
  auto acceptor = UringAcceptor::Listen(std::span{i.address}.first(i.address_size));
  if (!acceptor) return std::unexpected(acceptor.error());
  i.acceptor = std::move(*acceptor);
  if (!Transition(Lifecycle::kRunning))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return i.Accept();
}
std::expected<std::size_t, Status> GrpcServerService::LocalAddress(
    std::span<std::byte> out) const noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});

  if (!impl_ || impl_->lifecycle == Lifecycle::kCreated)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return impl_->acceptor.LocalAddress(out);
}
std::expected<PollResult, Status> GrpcServerService::PollOnce(PollBudget budget,
                                                              MonotonicTimestamp now) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!budget.Valid()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);
  return Pump(budget, now);
}
std::expected<PollResult, Status> GrpcServerService::Pump(PollBudget budget, MonotonicTimestamp,
                                                          PollResult result) noexcept {
  if (!impl_ ||
      (impl_->lifecycle != Lifecycle::kRunning && impl_->lifecycle != Lifecycle::kDraining))
    return std::unexpected(RejectTransition());
  auto& i = *impl_;
  if (i.accept_done && result.socket_events < budget.socket_events) {
    ++result.socket_events;
    i.accept_done = false;
    i.accepting.reset();
    if (i.accept_result >= 0) {
      auto slot = std::ranges::find_if(
          i.connections, [](const auto& c) { return !c.used || (c.closed && !c.Pending()); });
      if (i.lifecycle == Lifecycle::kDraining || slot == i.connections.end())
        ::close(i.accept_result);
      else {
        auto socket = UringSocket::Adopt(i.accept_result);
        if (!socket) return std::unexpected(socket.error());
        auto reused = i.Reuse(*slot);
        if (!reused) return std::unexpected(reused.error());
        slot->socket = std::move(*socket);
        slot->used = true;
      }
    } else if (i.lifecycle != Lifecycle::kDraining)
      return std::unexpected(Status{StatusCode::kUnavailable});
  }
  const bool was_accepting = i.accepting.has_value();
  auto accepted = result.commands < budget.commands ? i.Accept() : std::expected<void, Status>{};
  if (!accepted) return std::unexpected(accepted.error());
  if (!was_accepting && i.accepting) ++result.commands;
  std::size_t remaining = budget.frames;
  for (std::size_t n = 0; n < i.connections.size() && remaining != 0; ++n) {
    auto& c = i.connections[i.next_connection];
    i.next_connection = (i.next_connection + 1) % i.connections.size();
    if (!c.used) continue;
    --remaining;
    auto pumped = i.Pump(c, result, budget);
    if (!pumped) return std::unexpected(pumped.error());
  }
  result.pending = i.accept_done || std::ranges::any_of(i.connections, [&i](const auto& c) {
                     if (!c.used) return false;
                     if (c.read_done || c.write_done) return true;
                     if (c.closed) {
                       return (c.reading && !c.cancelled) ||
                              (i.force && c.writing && !c.write_cancelled) ||
                              (!c.Pending() && c.socket.Fd() >= 0);
                     }
                     return c.received_offset < c.received_size ||
                            (!c.writing && (!c.output_empty || c.transmit_size != 0));
                   });
  return result;
}
bool GrpcServerService::IsCurrent(std::size_t connection, std::int32_t stream,
                                  std::uint64_t generation) const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return false;

  if (!impl_ || impl_->lifecycle == Lifecycle::kClosed || connection >= impl_->connections.size())
    return false;
  const auto& c = impl_->connections[connection];
  return c.used && !c.closed && generation > c.generation_offset &&
         c.session.IsCurrent(stream, generation - c.generation_offset);
}
std::expected<void, Status> GrpcServerService::Complete(std::size_t connection, std::int32_t stream,
                                                        std::uint64_t generation,
                                                        std::span<const std::byte> payload,
                                                        int grpc_status) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});

  if (!impl_ || connection >= impl_->connections.size())
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto& c = impl_->connections[connection];
  if (!c.used || c.closed || impl_->lifecycle == Lifecycle::kClosed ||
      generation <= c.generation_offset)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  c.output_empty = false;
  auto result = c.session.Complete(stream, generation - c.generation_offset, payload, grpc_status);
  if (!result) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return {};
}
std::expected<void, Status> GrpcServerService::BeginShutdown(MonotonicTimestamp deadline) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return {};
  auto& i = *impl_;
  if (i.lifecycle == Lifecycle::kClosed || i.lifecycle == Lifecycle::kQuiescent ||
      i.lifecycle == Lifecycle::kDraining)
    return {};
  if (!Transition(Lifecycle::kDraining))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  i.deadline = deadline;
  if (i.accepting && !i.accept_done && !i.accept_cancelled) {
    auto result = i.proactor.Cancel(*i.accepting);
    if (!result) return std::unexpected(result.error());
    i.accept_cancelled = true;
  }
  for (auto& c : i.connections) {
    if (!c.used || c.closed || c.session.GetState() != Nghttp2ServerSession::State::kOpen) continue;
    c.output_empty = false;
    auto result = c.session.GoAway();
    if (!result) return std::unexpected(Status{StatusCode::kInternal});
  }
  return {};
}
std::expected<PollResult, Status> GrpcServerService::PollShutdown(PollBudget budget,
                                                                  MonotonicTimestamp now) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!budget.Valid()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);
  if (!impl_ || impl_->lifecycle == Lifecycle::kClosed || impl_->lifecycle == Lifecycle::kQuiescent)
    return PollResult{};
  auto& i = *impl_;
  if (i.lifecycle != Lifecycle::kDraining) return std::unexpected(RejectTransition());
  if (now >= i.deadline) {
    i.force = true;
    for (auto& c : i.connections)
      if (c.used) c.closed = true;
  }
  PollResult progress{};
  /** A failed cancellation remains retryable after submission pressure clears. */
  if (i.accepting && !i.accept_done && !i.accept_cancelled && progress.commands < budget.commands) {
    auto cancelled = i.proactor.Cancel(*i.accepting);
    if (!cancelled) return std::unexpected(cancelled.error());
    i.accept_cancelled = true;
    ++progress.commands;
  }
  if (!i.force) {
    for (auto& c : i.connections) {
      if (!c.used || c.closed || c.session.GetState() != Nghttp2ServerSession::State::kOpen)
        continue;
      c.output_empty = false;
      auto goaway = c.session.GoAway();
      if (!goaway) return std::unexpected(goaway.error());
    }
  }
  auto result = Pump(budget, now, progress);
  if (!result) return std::unexpected(result.error());
  bool done = !i.accepting;
  for (auto& c : i.connections) {
    if (!c.used) continue;
    if (c.closed) {
      done = done && !c.Pending();
      continue;
    }
    if (c.session.ActiveStreams() != 0 || c.writing || c.transmit_size != 0) {
      done = false;
      continue;
    }
    if (!c.output_empty || c.received_offset != c.received_size) {
      done = false;
      continue;
    }

    c.closed = true;
    if (c.reading && !c.read_done && !c.cancelled && result->commands < budget.commands) {
      auto cancelled = i.proactor.Cancel(*c.reading);
      if (!cancelled) return std::unexpected(cancelled.error());
      c.cancelled = true;
      ++result->commands;
    }
    done = done && !c.Pending();
  }
  if (done && !Transition(Lifecycle::kQuiescent))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  result->pending = !done;
  result->deadline = i.deadline;
  return *result;
}
std::expected<void, Status> GrpcServerService::Shutdown() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return {};
  auto& i = *impl_;
  if (i.Pending()) return std::unexpected(RejectTransition());
  for (auto& c : i.connections) {
    if (c.used && !c.closed && (c.session.ActiveStreams() != 0 || c.transmit_size != 0))
      return std::unexpected(RejectTransition());
  }
  if (!Transition(Lifecycle::kClosed))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  for (auto& c : i.connections) c.socket.Close();
  i.acceptor.Close();

  return {};
}
bool GrpcServerService::Transition(Lifecycle next) noexcept {
  if (!impl_) {
    static_cast<void>(RejectTransition());
    return false;
  }
  bool accepted{};
  switch (impl_->lifecycle) {
    case Lifecycle::kCreated:
      accepted = next == Lifecycle::kRunning || next == Lifecycle::kClosed;
      break;
    case Lifecycle::kRunning:
      accepted = next == Lifecycle::kDraining;
      break;
    case Lifecycle::kDraining:
      accepted = next == Lifecycle::kQuiescent;
      break;
    case Lifecycle::kQuiescent:
      accepted = next == Lifecycle::kClosed;
      break;
    case Lifecycle::kClosed:
      accepted = next == Lifecycle::kClosed;
      break;
  }
  if (!accepted) {
    static_cast<void>(RejectTransition());
    return false;
  }
  impl_->lifecycle = next;
  return true;
}
ServiceState GrpcServerService::State() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ServiceState::kFailed;
  if (!impl_) return ServiceState::kStopped;
  switch (impl_->lifecycle) {
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
bool GrpcServerService::Stopped() const noexcept { return State() == ServiceState::kStopped; }

std::uint64_t GrpcServerService::CallerIdentity() const noexcept {
  return proactor_.CallerThreadId();
}

}  // namespace grpcxx
