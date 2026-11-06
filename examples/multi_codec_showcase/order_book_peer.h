#pragma once

#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/reflection/client_stub.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace examples::order_book {

using Result = std::expected<void, grpcxx::Status>;

/** A failed scenario returns a status to main instead of relying on assertions. */
inline auto Failure(std::string_view message) noexcept -> std::unexpected<grpcxx::Status> {
  return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInternal, std::string(message)});
}

/** Quote completes inside dispatch; PlaceOrder retains only owned response data. */
template <typename Traits>
struct OrderBookService {
  using QuoteResponse = typename Traits::QuoteResponse;
  using OrderResponse = typename Traits::OrderResponse;
  std::int64_t id_;
  int quotes_{};
  int orders_{};
  bool failed_{};
  std::optional<grpcxx::UnaryResponder<OrderResponse>> pending_;
  std::optional<OrderResponse> response_;

  explicit OrderBookService(std::int64_t id) noexcept : id_(id) {}

  auto Quote(grpcxx::ServerContext&, const typename Traits::QuoteRequest& request) noexcept
      -> std::expected<grpcxx::Response<QuoteResponse>, grpcxx::Status> {
    ++quotes_;
    if (!Traits::ValidQuote(request)) {
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
    }
    return grpcxx::Response<QuoteResponse>{
        Traits::QuoteReply(Traits::QuoteId(request), id_), {}, {}};
  }

  void PlaceOrder(grpcxx::ServerContext&, const typename Traits::OrderRequest& request,
                  grpcxx::UnaryResponder<OrderResponse> responder) noexcept {
    ++orders_;
    if (!Traits::ValidOrder(request) || pending_) {
      auto failed = responder.Fail(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
      failed_ |= !failed;
      return;
    }
    response_.emplace(Traits::OrderReply(Traits::OrderId(request), Traits::Quantity(request), id_));
    pending_.emplace(std::move(responder));
  }

  /** The caller explicitly completes deferred work after both connections become idle. */
  auto CompletePending() noexcept -> Result {
    if (!pending_ || !response_ || !pending_->Current()) return Failure("deferred order missing");
    auto completed =
        pending_->Complete(grpcxx::Response<OrderResponse>{std::move(*response_), {}, {}});
    if (!completed) return std::unexpected(std::move(completed.error()));
    if (pending_->Current()) return Failure("deferred responder was not consumed");
    pending_.reset();
    response_.reset();
    return {};
  }
};

/** Each peer owns a client connection and a separately routed server connection. */
template <typename Traits>
struct Peer {
  using Codec = typename Traits::Codec;
  using Service = OrderBookService<Traits>;
  using Router = grpcxx::ServiceRouter<typename Traits::Contract, Service, Codec>;
  Codec client_codec_;
  Codec server_codec_;
  grpcxx::Nghttp2ClientSession client_;
  grpcxx::Nghttp2ServerSession server_{Router::kContentType};
  Service service_;
  Router router_;
  grpcxx::ServerContext context_;
  int dispatch_failures_{};

  explicit Peer(std::int64_t id) noexcept : service_(id), router_(service_, server_codec_) {
    Traits::Configure(client_codec_);
    Traits::Configure(server_codec_);
    server_.SetRequestHandler(
        this, [](void* opaque, std::int32_t stream, std::uint64_t generation, std::string_view path,
                 std::span<const std::byte> payload) noexcept {
          auto& self = *static_cast<Peer*>(opaque);
          auto dispatched = self.router_.Dispatch(path, self.context_, payload, self.Hooks(),
                                                  static_cast<std::uint64_t>(self.service_.id_),
                                                  stream, generation);
          if (!dispatched) ++self.dispatch_failures_;
        });
  }

  auto Hooks() noexcept -> grpcxx::UnaryResponderSink {
    return {.context = this,
            .is_current =
                [](void* opaque, std::uint64_t session, std::int32_t stream,
                   std::uint64_t generation) noexcept {
                  auto& self = *static_cast<Peer*>(opaque);
                  return session == static_cast<std::uint64_t>(self.service_.id_) &&
                         self.server_.IsCurrent(stream, generation);
                },
            .complete = [](void* opaque, std::uint64_t, std::int32_t stream,
                           std::uint64_t generation, std::span<const std::byte> payload,
                           grpcxx::MetadataMap&&, grpcxx::MetadataMap&&) noexcept -> Result {
              return static_cast<Peer*>(opaque)->server_.Complete(
                  stream, generation, payload, static_cast<int>(grpcxx::StatusCode::kOk));
            },
            .fail = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                       grpcxx::Status status) noexcept -> Result {
              return static_cast<Peer*>(opaque)->server_.Complete(stream, generation, {},
                                                                  static_cast<int>(status.Code()));
            }};
  }

  bool Idle() const noexcept {
    return client_.ActiveStreams() == 0 && server_.ActiveStreams() == 0 &&
           client_.RejectedTransitions() == 0 && server_.RejectedTransitions() == 0 &&
           dispatch_failures_ == 0 && !service_.failed_ && !service_.pending_;
  }
};

/** Consume borrowed output before the next operation on its source session; fragment every frame.
 */
template <typename From, typename To>
auto Transfer(From& from, To& to) noexcept -> std::expected<bool, grpcxx::Status> {
  auto bytes = from.Send();
  if (!bytes) return std::unexpected(std::move(bytes.error()));
  if (bytes->empty()) return false;
  auto remaining = *bytes;
  while (!remaining.empty()) {
    const auto fragment = remaining.first(std::min(remaining.size(), std::size_t{7}));
    auto received = to.Receive(fragment);
    if (!received) return std::unexpected(std::move(received.error()));
    if (*received == 0 || *received > fragment.size())
      return Failure("invalid HTTP/2 receive progress");
    remaining = remaining.subspan(*received);
  }
  return true;
}

/** Connect one peer's client to the other's server, including the reverse response path. */
template <typename Traits>
auto PumpConnection(Peer<Traits>& caller,
                    Peer<Traits>& callee) noexcept -> std::expected<bool, grpcxx::Status> {
  auto outgoing = Transfer(caller.client_, callee.server_);
  if (!outgoing) return std::unexpected(std::move(outgoing.error()));
  auto incoming = Transfer(callee.server_, caller.client_);
  if (!incoming) return std::unexpected(std::move(incoming.error()));
  return *outgoing || *incoming;
}

/** Poll both crossed connections with a deterministic bound and no threads or sleeps. */
template <typename Traits>
auto Drive(Peer<Traits>& left, Peer<Traits>& right) noexcept -> Result {
  for (auto tick : std::views::iota(0, 512)) {
    static_cast<void>(tick);
    auto forward = PumpConnection(left, right);
    if (!forward) return std::unexpected(std::move(forward.error()));
    auto reverse = PumpConnection(right, left);
    if (!reverse) return std::unexpected(std::move(reverse.error()));
    if (!*forward && !*reverse) return {};
  }
  return std::unexpected(
      grpcxx::Status{grpcxx::StatusCode::kDeadlineExceeded, "HTTP/2 pump exhausted"});
}

/** Own decoded responses beyond the callback, including Cap'n Proto arenas. */
template <typename Message>
struct Completion {
  int count_{};
  grpcxx::StatusCode status_{grpcxx::StatusCode::kUnknown};
  std::optional<Message> response_;

  auto Callback() noexcept {
    return [this](grpcxx::ClientContext&,
                  std::expected<grpcxx::Response<Message>, grpcxx::Status> result) noexcept {
      ++count_;
      status_ = result ? grpcxx::StatusCode::kOk : result.error().Code();
      if (result) response_.emplace(std::move(result->message));
    };
  }
  bool Ok() const noexcept {
    return count_ == 1 && status_ == grpcxx::StatusCode::kOk && response_.has_value();
  }
};

}  // namespace examples::order_book
