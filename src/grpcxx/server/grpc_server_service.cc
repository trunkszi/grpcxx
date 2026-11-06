#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/server/grpc_server_service.h>
#include <grpcxx/uring/uring_acceptor.h>
#include <grpcxx/uring/uring_socket.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <uarch/uarch.hpp>

namespace grpcxx {
namespace {
/** Rejects recursive lifecycle progress without affecting unary delivery from handlers. */
struct AdvanceGuard {
  bool& active;
  explicit AdvanceGuard(bool& flag) noexcept : active(flag) { active = true; }
  ~AdvanceGuard() { active = false; }
};

struct ServerConnMetaSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Flag{0}]] bool used{};
  [[= uarch::bits::Flag{1}]] bool closed{};
  [[= uarch::bits::Flag{2}]] bool cancelled{};
  [[= uarch::bits::Flag{3}]] bool write_cancelled{};
  [[= uarch::bits::Flag{4}]] bool read_done{};
  [[= uarch::bits::Flag{5}]] bool write_done{};
  [[= uarch::bits::Flag{6}]] bool output_empty{};
};

using ServerConnMetaWord = uarch::BitWord<ServerConnMetaSpec>;

constexpr auto kOpenConnPattern = uarch::BitPattern<ServerConnMetaSpec>{}
                                      .flag_set<^^ServerConnMetaSpec::used>()
                                      .flag_clear<^^ServerConnMetaSpec::closed>();

constexpr auto kReuseConnPatch = uarch::BitPatch<ServerConnMetaSpec>{}
                                     .set_flag<^^ServerConnMetaSpec::closed>(false)
                                     .set_flag<^^ServerConnMetaSpec::cancelled>(false)
                                     .set_flag<^^ServerConnMetaSpec::write_cancelled>(false)
                                     .set_flag<^^ServerConnMetaSpec::read_done>(false)
                                     .set_flag<^^ServerConnMetaSpec::write_done>(false)
                                     .set_flag<^^ServerConnMetaSpec::output_empty>(false);

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
  static constexpr std::uint64_t kAllConnectionBits = (1ULL << kConnections) - 1ULL;

  struct alignas(64) ServerConnHot final {
    ServerConnMetaWord meta_{};
    std::int32_t read_result_{0};
    std::int32_t write_result_{0};
    std::uint32_t index_{0U};
    std::uint64_t generation_offset_{0ULL};
    std::uint64_t last_generation_{0ULL};
    std::size_t received_size_{0U};
    std::size_t received_offset_{0U};
    std::size_t transmit_size_{0U};
    std::size_t sent_{0U};
  };
  static_assert(sizeof(ServerConnHot) == 64);
  static_assert(uarch::NoPadding<ServerConnHot>);

  struct Connection {
    ServerConnHot hot{};
    Nghttp2ServerSession session;
    FrameCursor ingress{.preface = 24};
    UringSocket socket;
    Impl* owner{};
    std::optional<OperationId> reading, writing;
    std::array<std::byte, 16384> receive{};
    std::array<std::byte, 131072> transmit{};

    [[nodiscard, gnu::always_inline]] bool Pending() const noexcept {
      return reading.has_value() || writing.has_value();
    }
  };

  ProactorService& proactor;
  GrpcServerService& service;
  Options options;
  PollResult* request_progress{};
  Lifecycle lifecycle{Lifecycle::kCreated};
  MonotonicTimestamp deadline{};
  std::array<std::byte, sizeof(sockaddr_storage)> address{};
  std::size_t address_size{}, next_connection{};
  uarch::Bitmap64 free_connections{uarch::Bitmap64::from_raw(kAllConnectionBits)};
  uarch::Bitmap64 used_connections{uarch::Bitmap64::empty()};
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
      c.hot.index_ = static_cast<std::uint32_t>(n);
      c.session.SetRequestHandler(&c, &OnRequest);
    }
  }

  static void OnRequest(void* context, std::int32_t stream, std::uint64_t generation,
                        std::string_view path, std::span<const std::byte> payload) noexcept {
    auto& connection = *static_cast<Connection*>(context);
    auto& owner = *connection.owner;
    if (uarch::unlikely(generation > std::numeric_limits<std::uint64_t>::max() -
                                         connection.hot.generation_offset_)) {
      connection.hot.meta_ = connection.hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
      return;
    }
    const auto external_generation = generation + connection.hot.generation_offset_;
    connection.hot.last_generation_ =
        std::max(connection.hot.last_generation_, external_generation);
    if (owner.options.handler) {
      if (owner.request_progress) ++owner.request_progress->completions;
      owner.options.handler(owner.options.context, owner.service, connection.hot.index_, stream,
                            external_generation, path, payload);
    } else {
      auto rejected = connection.session.Complete(stream, generation, {}, 12);
      if (!rejected) {
        connection.hot.meta_ = connection.hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
      }
    }
  }

  [[nodiscard]] Connection* FindReusableConnection() noexcept {
    if (const auto bit = free_connections.lowest_one()) {
      return &connections[*bit];
    }
    for (const std::uint32_t idx : used_connections | uarch::views::ones) {
      auto& c = connections[idx];
      if (c.hot.meta_.test<^^ServerConnMetaSpec::closed>() && !c.Pending()) {
        return &c;
      }
    }
    return nullptr;
  }

  std::expected<void, Status> Reuse(Connection& c) noexcept {
    if (!c.hot.meta_.test<^^ServerConnMetaSpec::used>()) return {};
    c.socket.Close();
    std::destroy_at(&c.session);
    std::construct_at(&c.session);
    c.hot.generation_offset_ = c.hot.last_generation_;
    c.session.SetRequestHandler(&c, &OnRequest);
    c.hot.meta_ = kReuseConnPatch.apply(c.hot.meta_);
    c.hot.sent_ = c.hot.transmit_size_ = 0U;
    c.ingress = FrameCursor{.preface = 24};
    c.hot.received_size_ = c.hot.received_offset_ = 0U;
    if (!c.session.Ready()) {
      c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
    return {};
  }

  bool Pending() const noexcept {
    if (accepting) return true;
    for (const std::uint32_t idx : used_connections | uarch::views::ones) {
      if (connections[idx].Pending()) return true;
    }
    return false;
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
    auto& hot = c.hot;
    if (hot.meta_.test<^^ServerConnMetaSpec::write_done>() &&
        progress.socket_events < budget.socket_events) {
      ++progress.socket_events;
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::write_done>(false);
      c.writing.reset();
      if (hot.write_result_ <= 0) {
        hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
      } else {
        hot.sent_ += static_cast<std::size_t>(hot.write_result_);
        if (hot.sent_ == hot.transmit_size_) hot.sent_ = hot.transmit_size_ = 0U;
      }
    }
    if (hot.meta_.test<^^ServerConnMetaSpec::read_done>() &&
        progress.socket_events < budget.socket_events) {
      ++progress.socket_events;
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::read_done>(false);
      c.reading.reset();
      if (hot.read_result_ <= 0) {
        hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
      } else {
        hot.received_size_ = static_cast<std::size_t>(hot.read_result_);
        hot.received_offset_ = 0U;
      }
    }
    while (!hot.meta_.test<^^ServerConnMetaSpec::closed>() &&
           hot.received_offset_ < hot.received_size_ && progress.frames < budget.frames &&
           progress.completions < budget.completions) {
      auto chunk = c.ingress.Chunk(
          std::span{c.receive}.first(hot.received_size_).subspan(hot.received_offset_));
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::output_empty>(false);
      request_progress = &progress;
      auto received = c.session.Receive(chunk);
      request_progress = nullptr;
      if (!received || *received == 0U || *received > chunk.size()) {
        hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
        break;
      }
      hot.received_offset_ += *received;
      if (c.ingress.Consume(chunk.first(*received))) ++progress.frames;
    }
    if (hot.meta_.test<^^ServerConnMetaSpec::closed>()) {
      if (force && c.writing && !hot.meta_.test<^^ServerConnMetaSpec::write_done>() &&
          !hot.meta_.test<^^ServerConnMetaSpec::write_cancelled>() &&
          progress.commands < budget.commands) {
        auto result = proactor.Cancel(*c.writing);
        if (!result) return std::unexpected(result.error());
        hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::write_cancelled>(true);
        ++progress.commands;
      }
      if (c.reading && !hot.meta_.test<^^ServerConnMetaSpec::read_done>() &&
          !hot.meta_.test<^^ServerConnMetaSpec::cancelled>() &&
          progress.commands < budget.commands) {
        auto result = proactor.Cancel(*c.reading);
        if (!result) return std::unexpected(result.error());
        hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::cancelled>(true);
        ++progress.commands;
      }
      if (!c.Pending()) {
        c.socket.Close();
        hot.sent_ = hot.transmit_size_ = 0U;
      }
      return {};
    }
    if (!c.writing && hot.transmit_size_ == 0U && progress.frames < budget.frames) {
      auto outbound = c.session.Send();
      if (!outbound) return std::unexpected(Status{StatusCode::kInternal});
      if (outbound->size() > c.transmit.size())
        return std::unexpected(Status{StatusCode::kResourceExhausted});
      if (!outbound->empty()) {
        std::memcpy(c.transmit.data(), outbound->data(), outbound->size());
      }
      hot.transmit_size_ = outbound->size();
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::output_empty>(outbound->empty());
      if (!outbound->empty()) {
        ++progress.frames;
      }
    }
    if (!c.writing && hot.transmit_size_ != 0U && progress.commands < budget.commands) {
      auto sent = proactor.SubmitSend(
          c.socket.Fd(), std::span{c.transmit}.first(hot.transmit_size_).subspan(hot.sent_),
          [&c](int n) noexcept {
            c.hot.write_result_ = n;
            c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::write_done>(true);
          });
      if (!sent) return std::unexpected(sent.error());
      c.writing = *sent;
      ++progress.commands;
    }
    if (!c.reading && !hot.meta_.test<^^ServerConnMetaSpec::read_done>() &&
        !hot.meta_.test<^^ServerConnMetaSpec::cancelled>() &&
        hot.received_offset_ == hot.received_size_ && progress.commands < budget.commands) {
      auto received = proactor.SubmitRecv(c.socket.Fd(), c.receive, [&c](int n) noexcept {
        c.hot.read_result_ = n;
        c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::read_done>(true);
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
      auto* slot = i.FindReusableConnection();
      if (i.lifecycle == Lifecycle::kDraining || slot == nullptr) {
        ::close(i.accept_result);
      } else {
        auto socket = UringSocket::Adopt(i.accept_result);
        if (!socket) return std::unexpected(socket.error());
        auto reused = i.Reuse(*slot);
        if (!reused) return std::unexpected(reused.error());
        slot->socket = std::move(*socket);
        slot->hot.meta_ = slot->hot.meta_.with<^^ServerConnMetaSpec::used>(true);
        const std::uint32_t bit = slot->hot.index_;
        i.free_connections = i.free_connections.clear(bit);
        i.used_connections = i.used_connections.set(bit);
      }
    } else if (i.lifecycle != Lifecycle::kDraining) {
      return std::unexpected(Status{StatusCode::kUnavailable});
    }
  }
  const bool was_accepting = i.accepting.has_value();
  auto accepted = result.commands < budget.commands ? i.Accept() : std::expected<void, Status>{};
  if (!accepted) return std::unexpected(accepted.error());
  if (!was_accepting && i.accepting) ++result.commands;
  std::size_t remaining = budget.frames;
  for (std::size_t n = 0; n < i.connections.size() && remaining != 0; ++n) {
    auto& c = i.connections[i.next_connection];
    i.next_connection = (i.next_connection + 1U) & (Impl::kConnections - 1U);
    if (!c.hot.meta_.test<^^ServerConnMetaSpec::used>()) continue;
    --remaining;
    auto pumped = i.Pump(c, result, budget);
    if (!pumped) return std::unexpected(pumped.error());
  }
  bool any_conn_pending = false;
  for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
    const auto& c = i.connections[idx];
    const auto& hot = c.hot;
    if (hot.meta_.test<^^ServerConnMetaSpec::read_done>() ||
        hot.meta_.test<^^ServerConnMetaSpec::write_done>()) {
      any_conn_pending = true;
      break;
    }
    if (hot.meta_.test<^^ServerConnMetaSpec::closed>()) {
      if ((c.reading && !hot.meta_.test<^^ServerConnMetaSpec::cancelled>()) ||
          (i.force && c.writing && !hot.meta_.test<^^ServerConnMetaSpec::write_cancelled>()) ||
          (!c.Pending() && c.socket.Fd() >= 0)) {
        any_conn_pending = true;
        break;
      }
      continue;
    }
    if (hot.received_offset_ < hot.received_size_ ||
        (!c.writing &&
         (!hot.meta_.test<^^ServerConnMetaSpec::output_empty>() || hot.transmit_size_ != 0U))) {
      any_conn_pending = true;
      break;
    }
  }
  result.pending = i.accept_done || any_conn_pending;
  return result;
}

bool GrpcServerService::IsCurrent(std::size_t connection, std::int32_t stream,
                                  std::uint64_t generation) const noexcept {
  if (owner_ && owner_ != CallerIdentity()) return false;

  if (!impl_ || impl_->lifecycle == Lifecycle::kClosed || connection >= impl_->connections.size())
    return false;
  const auto& c = impl_->connections[connection];
  return kOpenConnPattern.matches(c.hot.meta_) && generation > c.hot.generation_offset_ &&
         c.session.IsCurrent(stream, generation - c.hot.generation_offset_);
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
  if (!kOpenConnPattern.matches(c.hot.meta_) || impl_->lifecycle == Lifecycle::kClosed ||
      generation <= c.hot.generation_offset_)
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::output_empty>(false);
  auto result =
      c.session.Complete(stream, generation - c.hot.generation_offset_, payload, grpc_status);
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
  for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
    auto& c = i.connections[idx];
    if (!kOpenConnPattern.matches(c.hot.meta_) ||
        c.session.GetState() != Nghttp2ServerSession::State::kOpen)
      continue;
    c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::output_empty>(false);
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
    for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
      auto& hot = i.connections[idx].hot;
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
    }
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
    for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
      auto& c = i.connections[idx];
      if (!kOpenConnPattern.matches(c.hot.meta_) ||
          c.session.GetState() != Nghttp2ServerSession::State::kOpen)
        continue;
      c.hot.meta_ = c.hot.meta_.with<^^ServerConnMetaSpec::output_empty>(false);
      auto goaway = c.session.GoAway();
      if (!goaway) return std::unexpected(goaway.error());
    }
  }
  auto result = Pump(budget, now, progress);
  if (!result) return std::unexpected(result.error());
  bool done = !i.accepting;
  for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
    auto& c = i.connections[idx];
    auto& hot = c.hot;
    if (hot.meta_.test<^^ServerConnMetaSpec::closed>()) {
      done = done && !c.Pending();
      continue;
    }
    if (c.session.ActiveStreams() != 0 || c.writing || hot.transmit_size_ != 0U) {
      done = false;
      continue;
    }
    if (!hot.meta_.test<^^ServerConnMetaSpec::output_empty>() ||
        hot.received_offset_ != hot.received_size_) {
      done = false;
      continue;
    }

    hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::closed>(true);
    if (c.reading && !hot.meta_.test<^^ServerConnMetaSpec::read_done>() &&
        !hot.meta_.test<^^ServerConnMetaSpec::cancelled>() && result->commands < budget.commands) {
      auto cancelled = i.proactor.Cancel(*c.reading);
      if (!cancelled) return std::unexpected(cancelled.error());
      hot.meta_ = hot.meta_.with<^^ServerConnMetaSpec::cancelled>(true);
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
  for (const std::uint32_t idx : i.used_connections | uarch::views::ones) {
    const auto& c = i.connections[idx];
    if (kOpenConnPattern.matches(c.hot.meta_) &&
        (c.session.ActiveStreams() != 0 || c.hot.transmit_size_ != 0U))
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
