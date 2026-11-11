#pragma once
#include <grpcxx/h2/h2_server_session.h>
#include <grpcxx/h2/session_detail.h>
#include <grpcxx/server/grpc_server_service.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <limits>
#include <nexora/socket/tcp_transport.hpp>
#include <nexora/tls/tls_session.hpp>

namespace grpcxx {
struct GrpcServerService::Impl {
  static_assert(std::has_single_bit(kMaxConnections));
  static constexpr std::uint64_t kStreamGenMask = std::numeric_limits<std::uint32_t>::max();

  struct IoScratch {
    std::array<std::byte, h2_detail::kIoBufferBytes> output;
    std::array<std::uint8_t, 65536> network;
  };

  struct Connection {
    Impl* owner{};
    std::size_t index{};
    std::uint32_t generation{};
    std::unique_ptr<H2ServerSession> session;
    std::optional<nexora::IoUringSocket> socket;
    std::optional<nexora::TlsSession> tls;
    std::unique_ptr<IoScratch> scratch;
    std::size_t output_size{}, output_offset{}, tx_size{};
    std::uint64_t accepted_at{}, last_active_at{}, bytes_received{}, peer_eof_at{};
    h2_detail::FlagState flags;

    [[nodiscard]] bool NeedsWork() const noexcept {
      if (!socket) return false;
      if (flags.Test<^^h2_detail::RuntimeFlags::closing>() ||
          flags.Test<^^h2_detail::RuntimeFlags::rx_ready>() ||
          flags.Test<^^h2_detail::RuntimeFlags::tx_ready>())
        return true;
      if (!flags.Test<^^h2_detail::RuntimeFlags::peer_eof>() && !socket->RxToken()) return true;
      if (!socket->TxToken()) {
        if (output_offset < output_size) return true;
        if (tls && (tls->HasPendingCiphertextOut() || !tls->IsHandshakeComplete())) return true;
        if (flags.Test<^^h2_detail::RuntimeFlags::established>() && session &&
            session->HasPendingOutput())
          return true;
      }
      return false;
    }

    [[nodiscard]] bool Drained() const noexcept {
      return (!session || (session->ActiveStreams() == 0 && !session->HasPendingOutput())) &&
             output_offset == output_size && (!socket || !socket->TxToken()) &&
             (!tls || !tls->HasPendingCiphertextOut());
    }

    [[nodiscard]] std::expected<void, Status> Ingress(std::span<const std::uint8_t>,
                                                      std::uint64_t now_ns) noexcept;
    [[nodiscard]] std::expected<void, Status> Egress() noexcept;
    [[nodiscard]] std::expected<void, Status> Advance(std::uint64_t now_ns) noexcept;

    static void StreamMessage(void* context, std::int32_t id, std::uint64_t generation,
                              std::string_view path, std::span<const std::byte> payload,
                              bool end) noexcept {
      auto& self = *static_cast<Connection*>(context);
      if (generation > kStreamGenMask) {
        self.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
        return;
      }
      self.owner->options.stream_handler(
          self.owner->options.context, self.owner->service, self.index, id,
          (std::uint64_t{self.generation} << 32U) | generation, path, payload, end);
    }

    static void Request(void* context, std::int32_t stream, std::uint64_t generation,
                        std::string_view path, std::span<const std::byte> payload) noexcept {
      auto& self = *static_cast<Connection*>(context);
      if (generation > kStreamGenMask) {
        self.flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
        return;
      }
      const auto token = (std::uint64_t{self.generation} << 32U) | generation;
      if (self.owner->options.handler)
        self.owner->options.handler(self.owner->options.context, self.owner->service, self.index,
                                    stream, token, path, payload);
      else
        static_cast<void>(self.session->Complete(stream, generation, {}, 12));
    }
  };

  GrpcServerService& service;
  Options options;
  Lifecycle state{Lifecycle::kCreated};
  std::optional<nexora::TcpListener> listener;
  std::optional<uring::UserDataToken> accepting;
  std::optional<int> accepted;
  std::array<Connection, kMaxConnections> connections;
  uarch::Bitmap64 active{};
  uarch::Bitmap64 exhausted{};
  std::uint32_t poll_cursor{};
  MonotonicTimestamp deadline{std::numeric_limits<MonotonicTimestamp>::max()};
  h2_detail::FlagState flags;

  Impl(GrpcServerService& owner, Options opts) noexcept : service(owner), options(opts) {
    for (std::size_t index = 0; index < connections.size(); ++index) {
      connections[index].owner = this;
      connections[index].index = index;
    }
  }

  [[nodiscard]] bool AdoptAccepted(int fd, uring::Proactor& proactor,
                                   nexora::CompletionRoutes<>& routes,
                                   std::uint64_t now_ns) noexcept;
  [[nodiscard]] bool EvictOneForAccept() noexcept;

  [[nodiscard]] bool Pending() const noexcept {
    if (accepting || accepted) return true;
    return !active.is_empty();
  }
};
}  // namespace grpcxx
