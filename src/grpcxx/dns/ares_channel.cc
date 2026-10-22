#include "grpcxx/dns/ares_channel.h"

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#endif
#ifndef FMT_USE_EXCEPTIONS
#define FMT_USE_EXCEPTIONS 0
#endif
#include <ares.h>
#include <arpa/inet.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

namespace grpcxx {
namespace {

/** Process-lifetime c-ares init. Cleanup runs after every channel is gone. */
struct LibraryGuard {
  LibraryGuard() noexcept { ok = ares_library_init(ARES_LIB_INIT_NONE) == ARES_SUCCESS; }
  ~LibraryGuard() {
    if (ok) {
      ares_library_cleanup();
    }
  }
  bool ok{false};
};

[[nodiscard]] LibraryGuard& Library() noexcept {
  static LibraryGuard guard;
  return guard;
}

[[nodiscard]] Status StatusFromAres(int code) noexcept {
  if (code == ARES_SUCCESS) {
    return Status::OkStatus();
  }
  StatusCode mapped = StatusCode::kUnavailable;
  switch (code) {
    case ARES_ENOTFOUND:
    case ARES_ENODATA:
    case ARES_ENONAME:
      mapped = StatusCode::kNotFound;
      break;
    case ARES_ECANCELLED:
    case ARES_EDESTRUCTION:
      mapped = StatusCode::kCancelled;
      break;
    case ARES_ETIMEOUT:
      mapped = StatusCode::kDeadlineExceeded;
      break;
    case ARES_EBADNAME:
    case ARES_EBADFAMILY:
    case ARES_EBADQUERY:
    case ARES_EBADSTR:
      mapped = StatusCode::kInvalidArgument;
      break;
    default:
      mapped = StatusCode::kUnavailable;
      break;
  }
  std::array<char, 96> buffer{};
  const auto written = fmt::format_to_n(buffer.data(), buffer.size() - 1, "c-ares status {}", code);
  return Status{mapped, std::string(buffer.data(), written.out)};
}

/** Caps a poll budget with the same rule at compile time and at run time. */
[[nodiscard]] constexpr std::size_t LimitByBudget(std::uint32_t budget,
                                                  std::size_t hard_cap) noexcept {
  const auto requested = static_cast<std::size_t>(budget);
  if consteval {
    return requested < hard_cap ? requested : hard_cap;
  } else {
    return std::min(requested, hard_cap);
  }
}

[[nodiscard]] constexpr MonotonicTimestamp SaturatingAdd(MonotonicTimestamp now,
                                                         std::int64_t delay) noexcept {
  if (delay <= 0) {
    return now;
  }
  constexpr auto kUintMax = std::numeric_limits<std::uint64_t>::max();
  const auto amount = static_cast<std::uint64_t>(delay);
  if (now > kUintMax - amount) {
    return kUintMax;
  }
  return now + amount;
}

void UpdateSocket(std::vector<SocketInterest>& sockets, int fd, bool readable, bool writable) {
  const auto same_fd = [fd](const SocketInterest& interest) { return interest.fd == fd; };
  if (!readable && !writable) {
    std::erase_if(sockets, same_fd);
    return;
  }
  if (auto it = std::ranges::find_if(sockets, same_fd); it != std::ranges::end(sockets)) {
    it->readable = readable;
    it->writable = writable;
    return;
  }
  sockets.push_back(SocketInterest{fd, readable, writable});
}

}  // namespace

struct AresChannel::Impl {
  struct Query {
    Impl* impl{nullptr};
    ResolveCallback callback{nullptr};
    void* user{nullptr};
    std::uint16_t port{0};
  };

  ares_channel_t* channel{nullptr};
  std::vector<SocketInterest> sockets{};
  std::vector<std::unique_ptr<Query>> pending{};
  std::optional<MonotonicTimestamp> timeout_at{};
  bool open{false};
  bool closing{false};

  static void OnSocketState(void* data, ares_socket_t fd, int readable, int writable);
  static void OnAddrInfo(void* arg, int status, int timeouts, ares_addrinfo* result);

  ~Impl() {
    if (channel != nullptr) {
      ares_destroy(channel);
      channel = nullptr;
    }
  }

  void ArmTimeout(MonotonicTimestamp now) noexcept {
    if (pending.empty() || channel == nullptr) {
      timeout_at.reset();
      return;
    }
    timeval max_wait{.tv_sec = 1, .tv_usec = 0};
    timeval chosen{};
    const timeval* result = ares_timeout(channel, &max_wait, &chosen);
    if (result == nullptr) {
      timeout_at.reset();
      return;
    }
    const auto wait_nanos = static_cast<std::int64_t>(result->tv_sec) * 1'000'000'000LL +
                            static_cast<std::int64_t>(result->tv_usec) * 1'000LL;
    timeout_at = SaturatingAdd(now, wait_nanos);
  }

  void Finish(Query* query, int status, ares_addrinfo* result) noexcept {
    std::array<ResolvedEndpoint, 16> endpoints{};
    std::size_t count = 0;
    int code = status;
    if (status == ARES_SUCCESS && result != nullptr) {
      for (const ares_addrinfo_node* node = result->nodes;
           node != nullptr && count < endpoints.size(); node = node->ai_next) {
        const void* address = nullptr;
        ResolvedEndpoint endpoint{};
        endpoint.family = node->ai_family;
        endpoint.port = query->port;
        if (node->ai_family == AF_INET && node->ai_addr != nullptr) {
          const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(node->ai_addr);
          address = &ipv4->sin_addr;
          if (ipv4->sin_port != 0) {
            endpoint.port = ntohs(ipv4->sin_port);
          }
        } else if (node->ai_family == AF_INET6 && node->ai_addr != nullptr) {
          const auto* ipv6 = reinterpret_cast<const sockaddr_in6*>(node->ai_addr);
          address = &ipv6->sin6_addr;
          if (ipv6->sin6_port != 0) {
            endpoint.port = ntohs(ipv6->sin6_port);
          }
        }
        if (address == nullptr) {
          continue;
        }
        if (inet_ntop(node->ai_family, address, endpoint.numeric.data(), endpoint.numeric.size()) ==
            nullptr) {
          continue;
        }
        endpoint.numeric_length = static_cast<std::uint8_t>(std::strlen(endpoint.numeric.data()));
        endpoints[count] = endpoint;
        ++count;
      }
    }
    if (status == ARES_SUCCESS && count == 0) {
      code = ARES_ENOTFOUND;
    }
    if (result != nullptr) {
      ares_freeaddrinfo(result);
    }
    const ResolveCallback callback = query->callback;
    void* user = query->user;
    std::erase_if(pending,
                  [query](const std::unique_ptr<Query>& item) { return item.get() == query; });
    if (callback != nullptr) {
      callback(user, StatusFromAres(code),
               std::span<const ResolvedEndpoint>(endpoints).first(count));
    }
  }
};

void AresChannel::Impl::OnSocketState(void* data, ares_socket_t fd, int readable, int writable) {
  auto* impl = static_cast<AresChannel::Impl*>(data);
  if (impl == nullptr || fd == ARES_SOCKET_BAD) {
    return;
  }
  UpdateSocket(impl->sockets, static_cast<int>(fd), readable != 0, writable != 0);
}

void AresChannel::Impl::OnAddrInfo(void* arg, int status, int /*timeouts*/, ares_addrinfo* result) {
  auto* query = static_cast<Query*>(arg);
  if (query == nullptr || query->impl == nullptr) {
    if (result != nullptr) {
      ares_freeaddrinfo(result);
    }
    return;
  }
  query->impl->Finish(query, status, result);
}

AresChannel::AresChannel() noexcept : impl_(std::make_unique<Impl>()) {}

AresChannel::~AresChannel() { Close(); }

AresChannel::AresChannel(AresChannel&&) noexcept = default;

AresChannel& AresChannel::operator=(AresChannel&& other) noexcept {
  if (this != &other) {
    Close();
    impl_ = std::move(other.impl_);
  }
  return *this;
}

std::expected<void, Status> AresChannel::Open() noexcept {
  if (impl_ == nullptr) {
    impl_ = std::make_unique<Impl>();
  }
  if (impl_->open) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "ares channel is already open"});
  }
  if (!Library().ok) {
    return std::unexpected(Status{StatusCode::kUnavailable, "ares_library_init failed"});
  }
  ares_options options{};
  options.flags = ARES_FLAG_NOSEARCH;
  options.sock_state_cb = &Impl::OnSocketState;
  options.sock_state_cb_data = impl_.get();
  ares_channel_t* channel = nullptr;
  const int created =
      ares_init_options(&channel, &options, ARES_OPT_FLAGS | ARES_OPT_SOCK_STATE_CB);
  if (created != ARES_SUCCESS || channel == nullptr) {
    return std::unexpected(StatusFromAres(created == ARES_SUCCESS ? ARES_ENOMEM : created));
  }
  impl_->channel = channel;
  impl_->open = true;
  return {};
}

void AresChannel::Close() noexcept {
  if (impl_ == nullptr || impl_->closing) {
    return;
  }
  impl_->closing = true;
  impl_->open = false;
  if (impl_->channel != nullptr) {
    ares_cancel(impl_->channel);
    ares_destroy(impl_->channel);
    impl_->channel = nullptr;
  }
  impl_->pending.clear();
  impl_->sockets.clear();
  impl_->timeout_at.reset();
  impl_->closing = false;
}

std::expected<void, Status> AresChannel::Resolve(std::string_view host, std::uint16_t port,
                                                 ResolveCallback callback, void* user) noexcept {
  if (impl_ == nullptr || !impl_->open || impl_->channel == nullptr) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "ares channel is closed"});
  }
  if (callback == nullptr || host.empty()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "host and callback are required"});
  }
  auto query = std::make_unique<Impl::Query>();
  query->impl = impl_.get();
  query->callback = callback;
  query->user = user;
  query->port = port;
  Impl::Query* raw = query.get();
  impl_->pending.push_back(std::move(query));

  std::array<char, 8> port_text{};
  const auto written = fmt::format_to_n(port_text.data(), port_text.size() - 1, "{}", port);
  *written.out = '\0';
  const std::string host_text{host};

  ares_addrinfo_hints hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  ares_getaddrinfo(impl_->channel, host_text.c_str(), port_text.data(), &hints, &Impl::OnAddrInfo,
                   raw);
  return {};
}

std::expected<PollResult, Status> AresChannel::Process(PollBudget budget,
                                                       MonotonicTimestamp now) noexcept {
  if (impl_ == nullptr || !impl_->open || impl_->channel == nullptr) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition, "ares channel is closed"});
  }
  PollResult result{};
  const auto fire_timeout = [&] {
    ares_process_fd(impl_->channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
    ++result.timers;
  };
  if (budget.timers > 0 && impl_->timeout_at.has_value() && now >= *impl_->timeout_at) {
    fire_timeout();
    if (impl_->channel == nullptr) {
      result.pending = !impl_->pending.empty();
      result.deadline = impl_->timeout_at;
      return result;
    }
  }

  const std::size_t pending_before = impl_->pending.size();
  std::array<pollfd, 64> polled{};
  const auto limit = LimitByBudget(budget.socket_events, polled.size());
  std::size_t count = 0;
  for (const SocketInterest& interest : impl_->sockets | std::views::take(limit)) {
    short events = 0;
    if (interest.readable) {
      events = static_cast<short>(events | POLLIN);
    }
    if (interest.writable) {
      events = static_cast<short>(events | POLLOUT);
    }
    if (events == 0) {
      continue;
    }
    polled[count].fd = interest.fd;
    polled[count].events = events;
    polled[count].revents = 0;
    ++count;
  }
  if (impl_->channel != nullptr && count > 0) {
    const int ready = ::poll(polled.data(), static_cast<nfds_t>(count), 0);
    if (ready < 0) {
      if (errno != EINTR) {
        return std::unexpected(Status{StatusCode::kUnavailable, "DNS poll failed"});
      }
    } else if (ready > 0) {
      for (const pollfd& item : std::span<const pollfd>(polled).first(count)) {
        if (result.completions >= budget.completions) {
          break;
        }
        if (item.revents == 0) {
          continue;
        }
        const bool can_read = (item.revents & (POLLIN | POLLERR | POLLHUP)) != 0;
        const bool can_write = (item.revents & POLLOUT) != 0;
        if (impl_->channel == nullptr) {
          break;
        }
        ares_process_fd(impl_->channel, can_read ? item.fd : ARES_SOCKET_BAD,
                        can_write ? item.fd : ARES_SOCKET_BAD);
        ++result.socket_events;
        if (impl_->channel == nullptr) {
          break;
        }
      }
    }
  }

  const std::size_t pending_after = impl_->pending.size();
  const auto completed = pending_before > pending_after ? pending_before - pending_after : 0U;
  result.completions = static_cast<std::uint32_t>(
      std::min<std::size_t>(completed, std::numeric_limits<std::uint32_t>::max()));

  impl_->ArmTimeout(now);
  if (impl_->channel != nullptr && budget.timers > result.timers && impl_->timeout_at.has_value() &&
      now >= *impl_->timeout_at) {
    fire_timeout();
    impl_->ArmTimeout(now);
  }
  result.pending = !impl_->pending.empty();
  result.deadline = impl_->timeout_at;
  return result;
}

bool AresChannel::HasPending() const noexcept { return PendingCount() != 0; }

std::size_t AresChannel::PendingCount() const noexcept {
  return impl_ == nullptr ? 0 : impl_->pending.size();
}

std::size_t AresChannel::SocketCount() const noexcept {
  return impl_ == nullptr ? 0 : impl_->sockets.size();
}

std::span<const SocketInterest> AresChannel::Sockets() const noexcept {
  if (impl_ == nullptr) {
    return {};
  }
  return impl_->sockets;
}

}  // namespace grpcxx
