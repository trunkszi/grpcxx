#include <grpcxx/server/grpc_server_service_detail.h>

namespace grpcxx {
std::expected<void, Status> GrpcServerService::Impl::Connection::Ingress(
    std::span<const std::uint8_t> bytes, std::uint64_t now_ns) noexcept {
  bytes_received += bytes.size();
  last_active_at = now_ns;
  if (!tls) {
    auto fed = session->Receive(h2_detail::Bytes(bytes));
    if (!fed) return std::unexpected(fed.error());
    if (bytes_received >= 24) flags.Set<^^h2_detail::RuntimeFlags::handshake_done>(true);
    return {};
  }
  if (auto fed = tls->FeedCiphertext(bytes); !fed)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  if (auto driven = tls->DriveHandshake(); !driven)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  if (!tls->IsHandshakeComplete()) return {};
  if (tls->NegotiatedAlpn() != nexora::AlpnProtocol::kHttp2)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  flags.Set<^^h2_detail::RuntimeFlags::established>(true);
  flags.Set<^^h2_detail::RuntimeFlags::handshake_done>(true);
  std::array<std::uint8_t, 16384> plaintext;
  for (unsigned step = 0; step < 32; ++step) {
    auto read = tls->ReadPlaintext(plaintext);
    if (!read) return std::unexpected(Status{StatusCode::kDataLoss});
    if (*read == 0) break;
    if (auto fed = session->Receive(std::as_bytes(std::span(plaintext).first(*read))); !fed)
      return std::unexpected(fed.error());
  }
  return {};
}

std::expected<void, Status> GrpcServerService::Impl::Connection::Egress() noexcept {
  if (socket->TxToken()) return {};
  auto& io = *scratch;
  if (output_offset == output_size && flags.Test<^^h2_detail::RuntimeFlags::established>()) {
    auto bytes = session->Send();
    if (!bytes) return std::unexpected(bytes.error());
    output_size = bytes->size();
    output_offset = 0;
    if (!bytes->empty()) std::memcpy(io.output.data(), bytes->data(), bytes->size());
  }
  if (tls) {
    if (auto driven = tls->DriveHandshake(); !driven)
      return std::unexpected(Status{StatusCode::kUnauthenticated});
    if (tls->IsHandshakeComplete() && output_offset < output_size) {
      auto wrote = tls->WritePlaintext(h2_detail::Wire(
          std::span<const std::byte>(io.output).first(output_size).subspan(output_offset)));
      if (!wrote) return std::unexpected(Status{StatusCode::kDataLoss});
      output_offset += *wrote;
    }
    if (!tls->HasPendingCiphertextOut()) return {};
    auto taken = tls->TakeCiphertext(io.network);
    if (!taken) return std::unexpected(Status{StatusCode::kDataLoss});
    tx_size = *taken;
  } else {
    tx_size = std::min(output_size - output_offset, socket->TxBuffer().size());
    if (tx_size) std::memcpy(io.network.data(), io.output.data() + output_offset, tx_size);
  }
  if (tx_size && !socket->SubmitSend(std::span<const std::uint8_t>(io.network).first(tx_size)))
    return std::unexpected(Status{StatusCode::kUnavailable});
  return {};
}

std::expected<void, Status> GrpcServerService::Impl::Connection::Advance(
    std::uint64_t now_ns) noexcept {
  if (flags.Test<^^h2_detail::RuntimeFlags::closing>()) {
    static_cast<void>(socket->SubmitCancel());
    if (!socket->InFlightOps()) {
      socket.reset();
      owner->active = owner->active.clear(static_cast<std::uint32_t>(index));
      tls.reset();
      session.reset();
    }
    return {};
  }
  auto& io = *scratch;
  if (flags.Test<^^h2_detail::RuntimeFlags::tx_ready>()) {
    flags.Set<^^h2_detail::RuntimeFlags::tx_ready>(false);
    const auto result = socket->LastTxResult();
    if (result <= 0) return std::unexpected(Status{StatusCode::kUnavailable});
    last_active_at = now_ns;
    const auto sent = static_cast<std::size_t>(result);
    if (sent > tx_size) return std::unexpected(Status{StatusCode::kInternal});
    if (tls && sent < tx_size) {
      std::memmove(io.network.data(), io.network.data() + sent, tx_size - sent);
      tx_size -= sent;
      if (!socket->SubmitSend(std::span<const std::uint8_t>(io.network).first(tx_size)))
        return std::unexpected(Status{StatusCode::kUnavailable});
    } else if (!tls) {
      output_offset += sent;
    }
  }
  if (flags.Test<^^h2_detail::RuntimeFlags::rx_ready>()) {
    flags.Set<^^h2_detail::RuntimeFlags::rx_ready>(false);
    const auto read = socket->LastRxResult();
    if (read < 0) return std::unexpected(Status{StatusCode::kUnavailable});
    if (read == 0) {
      flags.Set<^^h2_detail::RuntimeFlags::peer_eof>(true);
      peer_eof_at = now_ns;
      if (flags.Test<^^h2_detail::RuntimeFlags::handshake_done>()) {
        if (auto ended = session->OnPeerEof(); !ended) return ended;
      }
    } else if (auto fed = Ingress(socket->RxBuffer().first(static_cast<std::size_t>(read)), now_ns);
               !fed) {
      return fed;
    }
  }
  if (auto sent = Egress(); !sent) return sent;
  if (flags.Test<^^h2_detail::RuntimeFlags::peer_eof>()) {
    if (!flags.Test<^^h2_detail::RuntimeFlags::handshake_done>() || Drained()) {
      flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
      static_cast<void>(socket->SubmitCancel());
      if (!socket->InFlightOps()) {
        socket.reset();
        owner->active = owner->active.clear(static_cast<std::uint32_t>(index));
        tls.reset();
        session.reset();
      }
    }
    return {};
  }
  if (!socket->RxToken() && !socket->SubmitRecv())
    return std::unexpected(Status{StatusCode::kUnavailable});
  return {};
}
}  // namespace grpcxx
