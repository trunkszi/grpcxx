#include <grpcxx/client/client_channel_service.h>
#include <grpcxx/h2/nghttp2_client_session.h>
#include <grpcxx/uring/uring_socket.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
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
struct ClientChannelService::Impl {
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

  struct PendingCall {
    UnaryCompletion completion;
    bool used{};
    std::int32_t stream{};
    bool ready{};
    int status{};
    std::size_t response_size{};
    alignas(8) std::array<std::byte, Nghttp2ClientSession::kMaxMessageBytes> response{};
  };
  std::array<PendingCall, Nghttp2ClientSession::kMaxStreams> calls;
  ProactorService& proactor;
  Nghttp2ClientSession session;
  FrameCursor ingress;
  std::size_t received_size{}, received_offset{};
  bool output_empty{}, preface_sent{};
  UringSocket socket;
  ChannelState state{ChannelState::kIdle};

  MonotonicTimestamp deadline{};
  std::array<std::byte, sizeof(sockaddr_storage)> address{};
  std::size_t address_size{};
  std::array<std::byte, 16384> receive{};
  std::array<std::byte, 131072> transmit{};
  std::size_t transmit_size{}, sent{};
  std::optional<OperationId> connecting, reading, writing;
  bool connect_done{}, read_done{}, write_done{}, cancelled{}, force{}, connect_cancelled{},
      write_cancelled{}, abandon_connect{}, peer_closed{};
  int connect_result{}, read_result{}, write_result{};
  Impl(ProactorService& p, Options options) noexcept : proactor(p) {
    if (options.address.size() <= address.size()) {
      address_size = options.address.size();
      std::ranges::copy(options.address, address.begin());
    }
  }
  bool ReadyCalls() const noexcept {
    return std::ranges::any_of(calls, [](const auto& call) { return call.used && call.ready; });
  }
  bool Pending() const noexcept {
    return connecting || reading || writing ||
           std::ranges::any_of(calls, [](const auto& call) { return call.used; });
  }
  void Deliver(PollResult& progress, PollBudget budget) noexcept {
    for (auto& call : calls) {
      if (progress.completions == budget.completions) break;
      if (!call.used || !call.ready) continue;
      auto completion = std::move(call.completion);
      call.used = call.ready = false;
      const auto size = std::exchange(call.response_size, 0);
      ++progress.completions;
      completion(call.stream, call.status, std::span{call.response}.first(size));
    }
  }
  void FailPending(int status) noexcept {
    for (auto& call : calls) {
      if (!call.used || call.ready) continue;
      call.status = status;
      call.response_size = 0;
      call.ready = true;
    }
  }

  std::expected<void, Status> CancelReads(PollResult& progress, PollBudget budget) noexcept {
    if (reading && !read_done && !cancelled && progress.commands < budget.commands) {
      auto result = proactor.Cancel(*reading);
      if (!result) return std::unexpected(result.error());
      cancelled = true;
      ++progress.commands;
    }
    return {};
  }
};
ClientChannelService::ClientChannelService(ProactorService& p, Options options) noexcept
    : proactor_(p), impl_(new(std::nothrow) Impl(p, options)) {}
ClientChannelService::~ClientChannelService() {
  if (owner_ && owner_ != CallerIdentity()) std::abort();
  if (advancing_) std::abort();
  /** Pending operations retain callbacks into this storage until their CQEs arrive. */
  if (impl_ && impl_->Pending()) std::abort();
}
Status ClientChannelService::RejectTransition() noexcept {
  if (rejected_transitions_ != std::numeric_limits<std::uint64_t>::max()) ++rejected_transitions_;
  return Status{StatusCode::kFailedPrecondition};
}
std::uint64_t ClientChannelService::RejectedTransitions() const noexcept {
  return (!owner_ || owner_ == CallerIdentity()) ? rejected_transitions_ : 0;
}
std::expected<void, Status> ClientChannelService::Start() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  const auto identity = CallerIdentity();
  if (identity == 0) return std::unexpected(Status{StatusCode::kInvalidArgument});
  owner_ = identity;
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return std::unexpected(Status{StatusCode::kResourceExhausted});
  auto& i = *impl_;
  if (i.state != ChannelState::kIdle) return std::unexpected(RejectTransition());
  if (i.address_size < sizeof(sa_family_t))
    return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (!i.session.Ready()) return std::unexpected(Status{StatusCode::kUnavailable});
  sockaddr_storage address{};
  std::memcpy(&address, i.address.data(), i.address_size);
  auto socket = address.ss_family == AF_UNIX ? UringSocket::CreateUnix()
                                             : UringSocket::CreateTcp(address.ss_family);
  if (!socket) return std::unexpected(socket.error());
  i.socket = std::move(*socket);
  auto connected = i.proactor.SubmitConnect(
      i.socket.Fd(), std::span{i.address}.first(i.address_size), [&i](int result) noexcept {
        i.connect_result = result;
        i.connect_done = true;
      });
  if (!connected) return std::unexpected(connected.error());
  i.connecting = *connected;
  if (!Transition(ChannelState::kConnecting))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return {};
}
std::expected<PollResult, Status> ClientChannelService::PollOnce(PollBudget budget,
                                                                 MonotonicTimestamp now) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!budget.Valid()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);
  return Pump(budget, now);
}
std::expected<PollResult, Status> ClientChannelService::Pump(PollBudget budget, MonotonicTimestamp,
                                                             PollResult result) noexcept {
  if (!impl_) return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto& i = *impl_;
  if (i.state == ChannelState::kIdle || i.state == ChannelState::kClosed)
    return std::unexpected(RejectTransition());
  i.Deliver(result, budget);
  if (budget.frames == 0) return result;
  if (i.connect_done && result.socket_events < budget.socket_events) {
    ++result.socket_events;
    i.connect_done = false;
    i.connecting.reset();
    if (i.connect_result < 0 && !i.force && !i.abandon_connect) {
      if (!Transition(ChannelState::kTransientFailure))
        return std::unexpected(Status{StatusCode::kFailedPrecondition});
      i.FailPending(14);
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
    if (i.state != ChannelState::kShuttingDown)
      if (!Transition(ChannelState::kReady))
        return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  if (i.write_done && result.socket_events < budget.socket_events) {
    ++result.socket_events;
    i.write_done = false;
    i.writing.reset();
    if (i.write_result <= 0 && !i.force) {
      if (i.state == ChannelState::kShuttingDown && i.session.ActiveStreams() == 0) {
        i.peer_closed = true;
      } else {
        if (!Transition(ChannelState::kTransientFailure))
          return std::unexpected(Status{StatusCode::kFailedPrecondition});
        i.FailPending(14);
        return std::unexpected(Status{StatusCode::kUnavailable});
      }
    }
    if (i.write_result > 0) i.sent += static_cast<std::size_t>(i.write_result);
    if (i.force || i.peer_closed) i.sent = i.transmit_size;
    if (i.sent == i.transmit_size) i.sent = i.transmit_size = 0;
  }
  if (i.read_done && result.socket_events < budget.socket_events) {
    ++result.socket_events;
    i.read_done = false;
    i.reading.reset();
    if (i.read_result <= 0) {
      if (i.state == ChannelState::kShuttingDown && i.session.ActiveStreams() == 0) {
        i.peer_closed = true;
        i.cancelled = true;
      } else if (!(i.cancelled && i.state == ChannelState::kShuttingDown)) {
        if (!Transition(ChannelState::kTransientFailure))
          return std::unexpected(Status{StatusCode::kFailedPrecondition});
        i.FailPending(14);
        return std::unexpected(Status{StatusCode::kUnavailable});
      }
    } else {
      i.received_size = static_cast<std::size_t>(i.read_result);
      i.received_offset = 0;
    }
  }
  while (!i.force && i.received_offset < i.received_size && result.frames < budget.frames) {
    auto chunk =
        i.ingress.Chunk(std::span{i.receive}.first(i.received_size).subspan(i.received_offset));
    i.output_empty = false;
    auto received = i.session.Receive(chunk);
    if (!received) {
      i.received_offset = i.received_size;
      if (!Transition(ChannelState::kTransientFailure))
        return std::unexpected(Status{StatusCode::kFailedPrecondition});
      i.FailPending(14);
      return std::unexpected(received.error());
    }
    if (*received == 0 || *received > chunk.size()) {
      i.received_offset = i.received_size;
      if (!Transition(ChannelState::kTransientFailure))
        return std::unexpected(Status{StatusCode::kFailedPrecondition});
      i.FailPending(14);
      return std::unexpected(Status{StatusCode::kInternal});
    }
    i.received_offset += *received;
    if (i.ingress.Consume(chunk.first(*received))) ++result.frames;
  }
  if (i.force || i.abandon_connect || i.peer_closed) {
    i.Deliver(result, budget);
    result.pending = i.ReadyCalls();
    i.output_empty = true;
    i.transmit_size = i.sent = 0;
    return result;
  }
  if (budget.frames == 0 ||
      (i.state != ChannelState::kReady && i.state != ChannelState::kShuttingDown)) {
    result.pending = i.ReadyCalls();
    return result;
  }
  if (!i.writing && i.transmit_size == 0 && result.frames < budget.frames) {
    auto outbound = i.session.Send();
    if (!outbound) return std::unexpected(Status{StatusCode::kInternal});
    if (outbound->size() > i.transmit.size())
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    std::ranges::copy(*outbound, i.transmit.begin());
    i.transmit_size = outbound->size();
    i.output_empty = outbound->empty();
    if (!outbound->empty()) {
      constexpr std::string_view kPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
      const bool is_preface =
          !i.preface_sent && std::ranges::equal(*outbound, std::as_bytes(std::span{kPreface}));
      if (is_preface)
        i.preface_sent = true;
      else
        ++result.frames;
    }
  }
  if (!i.writing && i.transmit_size != 0 && result.commands < budget.commands) {
    auto sent = i.proactor.SubmitSend(i.socket.Fd(),
                                      std::span{i.transmit}.first(i.transmit_size).subspan(i.sent),
                                      [&i](int n) noexcept {
                                        i.write_result = n;
                                        i.write_done = true;
                                      });
    if (!sent) return std::unexpected(sent.error());
    i.writing = *sent;
    ++result.commands;
  }
  if (!i.reading && !i.cancelled && i.received_offset == i.received_size &&
      result.commands < budget.commands) {
    auto received = i.proactor.SubmitRecv(i.socket.Fd(), i.receive, [&i](int n) noexcept {
      i.read_result = n;
      i.read_done = true;
    });
    if (!received) return std::unexpected(received.error());
    i.reading = *received;
    ++result.commands;
  }
  i.Deliver(result, budget);
  result.pending = i.ReadyCalls() || i.read_done || i.write_done || i.connect_done ||
                   i.received_offset < i.received_size ||
                   (!i.writing && (!i.output_empty || i.transmit_size != 0));
  return result;
}
std::expected<std::int32_t, Status> ClientChannelService::CallUnary(
    std::string_view path, std::span<const std::byte> payload,
    UnaryCompletion completion) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});

  if (!impl_ || impl_->state != ChannelState::kReady || !completion)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  auto slot = std::ranges::find_if(impl_->calls, [](const auto& call) { return !call.used; });
  if (slot == impl_->calls.end()) return std::unexpected(Status{StatusCode::kResourceExhausted});
  slot->completion = std::move(completion);
  slot->used = true;
  slot->ready = false;
  slot->status = 0;
  slot->response_size = 0;
  impl_->output_empty = false;
  auto result = impl_->session.Submit(
      path, payload, &*slot,
      [](void* context, std::int32_t stream, int status,
         std::span<const std::byte> bytes) noexcept {
        auto& call = *static_cast<Impl::PendingCall*>(context);
        if (!call.used || call.ready) return;
        call.stream = stream;
        call.status = bytes.size() <= call.response.size() ? status : 8;
        call.response_size = bytes.size() <= call.response.size() ? bytes.size() : 0;
        std::ranges::copy(bytes.first(call.response_size), call.response.begin());
        call.ready = true;
      });
  if (!result) {
    slot->completion = {};
    slot->used = false;
    return std::unexpected(result.error());
  }
  slot->stream = *result;
  return *result;
}
std::expected<std::int32_t, Status> ClientChannelService::CallUnary(
    std::string_view path, std::span<const std::byte> payload, void* context,
    Completion completion) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});

  if (!completion) return std::unexpected(Status{StatusCode::kInvalidArgument});
  return CallUnary(path, payload,
                   [context, completion](std::int32_t stream, int status,
                                         std::span<const std::byte> bytes) noexcept {
                     completion(context, stream, status, bytes);
                   });
}
ClientChannelService::ChannelState ClientChannelService::GetState() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ChannelState::kTransientFailure;

  return impl_ ? impl_->state : ChannelState::kClosed;
}
std::expected<void, Status> ClientChannelService::BeginShutdown(
    MonotonicTimestamp deadline) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return {};
  auto& i = *impl_;
  if (i.state == ChannelState::kClosed || i.state == ChannelState::kShuttingDown) return {};
  if (i.state == ChannelState::kIdle) return std::unexpected(RejectTransition());
  if (i.state == ChannelState::kReady &&
      i.session.GetState() == Nghttp2ClientSession::State::kOpen) {
    i.output_empty = false;
    auto result = i.session.GoAway();
    if (!result) return std::unexpected(Status{StatusCode::kInternal});
  }
  i.abandon_connect = i.state == ChannelState::kConnecting;
  i.deadline = deadline;
  if (!Transition(ChannelState::kShuttingDown))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return {};
}
std::expected<PollResult, Status> ClientChannelService::PollShutdown(
    PollBudget budget, MonotonicTimestamp now) noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (!budget.Valid()) return std::unexpected(Status{StatusCode::kInvalidArgument});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);
  if (!impl_ || impl_->state == ChannelState::kClosed) return PollResult{};
  if (impl_->state != ChannelState::kShuttingDown) return std::unexpected(RejectTransition());

  PollResult progress{};
  auto& state = *impl_;
  if (now >= state.deadline && !state.force) {
    state.force = true;
    state.FailPending(4);
  }
  if (state.force || state.abandon_connect || state.peer_closed) {
    if (state.connecting && !state.connect_done && !state.connect_cancelled &&
        progress.commands < budget.commands) {
      auto cancelled = state.proactor.Cancel(*state.connecting);
      if (!cancelled) return std::unexpected(cancelled.error());
      state.connect_cancelled = true;
      ++progress.commands;
    }
    if (state.writing && !state.write_done && !state.write_cancelled &&
        progress.commands < budget.commands) {
      auto cancelled = state.proactor.Cancel(*state.writing);
      if (!cancelled) return std::unexpected(cancelled.error());
      state.write_cancelled = true;
      ++progress.commands;
    }
    auto cancelled = state.CancelReads(progress, budget);
    if (!cancelled) return std::unexpected(cancelled.error());
  }
  auto result = Pump(budget, now, progress);
  if (!result) return std::unexpected(result.error());
  result->pending = true;
  auto& i = *impl_;
  if ((!i.force && i.session.ActiveStreams() != 0) || i.connecting || i.writing ||
      i.transmit_size != 0)
    return *result;
  if (!i.force && (!i.output_empty || i.received_offset != i.received_size)) return *result;

  auto cancelled = i.CancelReads(*result, budget);
  if (!cancelled) return std::unexpected(cancelled.error());
  if (!i.Pending() && !Transition(ChannelState::kClosed))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  result->pending = i.Pending();
  result->deadline = i.deadline;
  return *result;
}
std::expected<void, Status> ClientChannelService::Shutdown() noexcept {
  if (owner_ && owner_ != CallerIdentity())
    return std::unexpected(Status{StatusCode::kPermissionDenied});
  if (advancing_) return std::unexpected(RejectTransition());
  AdvanceGuard advance(advancing_);

  if (!impl_) return {};
  auto& i = *impl_;
  if (i.state != ChannelState::kIdle && i.state != ChannelState::kClosed)
    return std::unexpected(RejectTransition());
  if (i.Pending() || (!i.force && i.session.ActiveStreams() != 0) || i.transmit_size != 0)
    return std::unexpected(RejectTransition());
  i.socket.Close();
  if (!Transition(ChannelState::kClosed))
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  return {};
}
bool ClientChannelService::Transition(ChannelState next) noexcept {
  if (!impl_) {
    static_cast<void>(RejectTransition());
    return false;
  }
  bool accepted{};
  switch (impl_->state) {
    case ChannelState::kIdle:
      accepted = next == ChannelState::kConnecting || next == ChannelState::kClosed;
      break;
    case ChannelState::kConnecting:
      accepted = next == ChannelState::kReady || next == ChannelState::kTransientFailure ||
                 next == ChannelState::kShuttingDown;
      break;
    case ChannelState::kReady:
      accepted = next == ChannelState::kTransientFailure || next == ChannelState::kShuttingDown;
      break;
    case ChannelState::kTransientFailure:
      accepted = next == ChannelState::kTransientFailure || next == ChannelState::kShuttingDown;
      break;
    case ChannelState::kShuttingDown:
      accepted = next == ChannelState::kClosed || next == ChannelState::kTransientFailure;
      break;
    case ChannelState::kClosed:
      accepted = next == ChannelState::kClosed;
      break;
  }
  if (!accepted) {
    static_cast<void>(RejectTransition());
    return false;
  }
  impl_->state = next;
  return true;
}
ServiceState ClientChannelService::State() const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return ServiceState::kFailed;
  if (!impl_) return ServiceState::kStopped;
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

std::uint64_t ClientChannelService::CallerIdentity() const noexcept {
  return proactor_.CallerThreadId();
}

}  // namespace grpcxx
