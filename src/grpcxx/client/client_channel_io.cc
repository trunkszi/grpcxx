#include <grpcxx/client/client_channel_service_detail.h>

namespace grpcxx {
bool ClientChannelService::Impl::SeedEndpoints(std::span<const cares::Endpoint> list) noexcept {
  return endpoints.ReconcileEndpoints(list).has_value();
}

std::optional<cares::Endpoint> ClientChannelService::Impl::PickEndpoint(
    std::uint64_t now_ns) noexcept {
  auto idx = endpoints.SelectNext<nexora::EndpointSelectionPolicy::kRoundRobin>(now_ns);
  if (!idx) return std::nullopt;
  return endpoints.EndpointAt(*idx);
}

void ClientChannelService::Impl::MarkConnected(std::uint64_t now_ns) noexcept {
  const auto rtt = now_ns >= connect_started_at ? now_ns - connect_started_at : 0ULL;
  if (auto idx = endpoints.FindEndpoint(active_endpoint)) {
    endpoints.SetReady(*idx, true);
    endpoints.RecordSuccess(*idx, rtt);
  }
}

void ClientChannelService::Impl::ConfigureSessionTarget() noexcept {
  const std::string_view scheme = options.tls != nullptr ? "https" : "http";
  const std::uint16_t default_port = options.tls != nullptr ? 443U : 80U;
  std::array<char, 320> auth_buf{};
  std::size_t len = 0;
  auto push = [&](std::string_view part) noexcept {
    const auto copy = std::min(part.size(), auth_buf.size() - len);
    if (copy != 0) std::memcpy(auth_buf.data() + len, part.data(), copy);
    len += copy;
  };
  if (!server_name.empty()) {
    push(server_name);
  } else if (!target.empty()) {
    const bool bracket = target.find(':') != std::string_view::npos && !target.starts_with('[');
    if (bracket) push("[");
    push(target);
    if (bracket) push("]");
    const std::uint16_t port = active_endpoint.port != 0 ? active_endpoint.port : options.port;
    if (port != 0 && port != default_port) {
      std::array<char, 8> port_buf{};
      push(":");
      push(h2_detail::FormatPortU16(port, port_buf));
    }
  } else {
    std::array<char, 64> ip_buf{};
    const auto ip_len = active_endpoint.ip.FormatTo(ip_buf);
    const std::string_view ip_view(ip_buf.data(), ip_len);
    if (active_endpoint.ip.IsV6()) push("[");
    push(ip_view);
    if (active_endpoint.ip.IsV6()) push("]");
    if (active_endpoint.port != 0 && active_endpoint.port != default_port) {
      std::array<char, 8> port_buf{};
      push(":");
      push(h2_detail::FormatPortU16(active_endpoint.port, port_buf));
    }
  }
  const std::string_view authority(auth_buf.data(), len);
  static_cast<void>(session->SetTarget(scheme, authority.empty() ? "localhost" : authority));
  session->SetDefaultTimeoutNs(options.default_timeout_ns);
}

std::string_view ClientChannelService::Impl::FormatTlsHost(
    std::span<char, 256> scratch) const noexcept {
  std::string_view raw{};
  std::array<char, 64> ip_buf{};
  if (!server_name.empty()) {
    raw = server_name;
  } else if (!target.empty()) {
    raw = target;
  } else {
    const auto ip_len = active_endpoint.ip.FormatTo(ip_buf);
    raw = std::string_view(ip_buf.data(), ip_len);
  }
  if (raw.starts_with('[')) {
    if (const auto close = raw.find(']'); close != std::string_view::npos && close > 1)
      raw = raw.substr(1, close - 1);
  } else {
    const auto first_colon = raw.find(':');
    if (first_colon != std::string_view::npos &&
        raw.find(':', first_colon + 1) == std::string_view::npos)
      raw = raw.substr(0, first_colon);
  }
  const auto copy = std::min(raw.size(), scratch.size());
  if (copy != 0) std::memcpy(scratch.data(), raw.data(), copy);
  return {scratch.data(), copy};
}

std::expected<void, Status> ClientChannelService::Impl::ConnectNext(
    uring::Proactor& ring, nexora::CompletionRoutes<>& routes, std::uint64_t now_ns) noexcept {
  FailActiveCalls(static_cast<int>(StatusCode::kUnavailable));
  if (socket && socket->InFlightOps()) return RotateEndpoint(ring, routes, now_ns);
  while (auto selected = PickEndpoint(now_ns)) {
    active_endpoint = *selected;
    connect_started_at = now_ns;
    tls.reset();
    socket.reset();
    session = std::make_unique<H2ClientSession>();
    ConfigureSessionTarget();
    output_size = output_offset = tx_size = 0;
    flags.Set<^^h2_detail::RuntimeFlags::rx_ready>(false);
    flags.Set<^^h2_detail::RuntimeFlags::tx_ready>(false);
    flags.Set<^^h2_detail::RuntimeFlags::connect_ready>(false);
    flags.Set<^^h2_detail::RuntimeFlags::closing>(false);

    auto opened = nexora::IoUringSocket::CreateForEndpoint(active_endpoint);
    if (!opened) {
      endpoints.MarkFailure(active_endpoint, now_ns);
      continue;
    }
    socket.emplace(std::move(*opened));
    socket->BindProactor(ring, routes);
    if (!socket->SubmitConnect(active_endpoint)) {
      socket.reset();
      endpoints.MarkFailure(active_endpoint, now_ns);
      continue;
    }
    if (state != ChannelState::kShuttingDown) state = ChannelState::kConnecting;
    return {};
  }
  if (state != ChannelState::kShuttingDown) state = ChannelState::kTransientFailure;
  if (endpoints.Size() == 1 && active_endpoint.port == 0)
    return std::unexpected(Status{StatusCode::kUnavailable});
  return {};
}

std::expected<void, Status> ClientChannelService::Impl::RotateEndpoint(
    uring::Proactor& ring, nexora::CompletionRoutes<>& routes, std::uint64_t now_ns) noexcept {
  FailActiveCalls(static_cast<int>(StatusCode::kUnavailable));
  flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
  if (socket) {
    static_cast<void>(socket->SubmitCancel());
    if (socket->InFlightOps()) {
      state = ChannelState::kTransientFailure;
      return {};
    }
  }
  socket.reset();
  tls.reset();
  return ConnectNext(ring, routes, now_ns);
}

void ClientChannelService::Impl::TearDownAndFailover(uring::Proactor& ring,
                                                     nexora::CompletionRoutes<>& routes,
                                                     std::uint64_t now_ns,
                                                     int call_error) noexcept {
  FailActiveCalls(call_error);
  if (!flags.Test<^^h2_detail::RuntimeFlags::closing>()) {
    endpoints.MarkFailure(active_endpoint, now_ns);
    flags.Set<^^h2_detail::RuntimeFlags::closing>(true);
  }
  if (socket) {
    static_cast<void>(socket->SubmitCancel());
    if (socket->InFlightOps()) {
      if (state != ChannelState::kShuttingDown) state = ChannelState::kTransientFailure;
      return;
    }
    socket.reset();
  }
  tls.reset();
  flags.Set<^^h2_detail::RuntimeFlags::closing>(false);
  if (state != ChannelState::kShuttingDown && state != ChannelState::kClosed)
    static_cast<void>(ConnectNext(ring, routes, now_ns));
}

std::expected<void, Status> ClientChannelService::Impl::Ingress(std::span<const std::uint8_t> bytes,
                                                                std::uint64_t now_ns) noexcept {
  if (!tls) {
    auto fed = session->Receive(h2_detail::Bytes(bytes));
    return fed ? std::expected<void, Status>{} : std::unexpected(fed.error());
  }
  if (auto fed = tls->FeedCiphertext(bytes); !fed)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  if (auto driven = tls->DriveHandshake(); !driven)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  if (!tls->IsHandshakeComplete()) return {};
  if (tls->NegotiatedAlpn() != nexora::AlpnProtocol::kHttp2)
    return std::unexpected(Status{StatusCode::kUnauthenticated});
  if (state != ChannelState::kReady && state != ChannelState::kShuttingDown) {
    state = ChannelState::kReady;
    MarkConnected(now_ns);
  }
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

std::expected<void, Status> ClientChannelService::Impl::Egress() noexcept {
  if (socket->TxToken()) return {};
  if (output_offset == output_size &&
      (state == ChannelState::kReady || state == ChannelState::kShuttingDown)) {
    auto bytes = session->Send();
    if (!bytes) return std::unexpected(bytes.error());
    output_size = bytes->size();
    output_offset = 0;
    if (!bytes->empty()) std::memcpy(output.data(), bytes->data(), bytes->size());
  }
  if (tls) {
    if (auto driven = tls->DriveHandshake(); !driven)
      return std::unexpected(Status{StatusCode::kUnauthenticated});
    if (tls->IsHandshakeComplete() && output_offset < output_size) {
      auto wrote = tls->WritePlaintext(h2_detail::Wire(
          std::span<const std::byte>(output).first(output_size).subspan(output_offset)));
      if (!wrote) return std::unexpected(Status{StatusCode::kDataLoss});
      output_offset += *wrote;
    }
    if (!tls->HasPendingCiphertextOut()) return {};
    auto taken = tls->TakeCiphertext(network);
    if (!taken) return std::unexpected(Status{StatusCode::kDataLoss});
    tx_size = *taken;
  } else {
    tx_size = std::min(output_size - output_offset, socket->TxBuffer().size());
    if (tx_size) std::memcpy(network.data(), output.data() + output_offset, tx_size);
  }
  if (tx_size && !socket->SubmitSend(std::span<const std::uint8_t>(network).first(tx_size)))
    return std::unexpected(Status{StatusCode::kUnavailable});
  return {};
}

std::expected<void, Status> ClientChannelService::Impl::Advance(uring::Proactor& ring,
                                                                nexora::CompletionRoutes<>& routes,
                                                                std::uint64_t now_ns) noexcept {
  if (flags.Test<^^h2_detail::RuntimeFlags::closing>()) {
    if (socket) {
      static_cast<void>(socket->SubmitCancel());
      if (socket->InFlightOps()) return {};
      socket.reset();
    }
    tls.reset();
    flags.Set<^^h2_detail::RuntimeFlags::closing>(false);
    return ConnectNext(ring, routes, now_ns);
  }
  if (resolution) {
    auto resolved = std::move(*resolution);
    resolution.reset();
    if (!resolved || resolved->empty() ||
        !SeedEndpoints(std::span<const cares::Endpoint>(resolved->data(), resolved->size()))) {
      if (state != ChannelState::kReady) state = ChannelState::kTransientFailure;
      return {};
    }
    if (!socket || !endpoints.FindEndpoint(active_endpoint).has_value())
      return RotateEndpoint(ring, routes, now_ns);
    return {};
  }
  if (!socket) {
    if (state == ChannelState::kTransientFailure && endpoints.Size() != 0 &&
        PickEndpoint(now_ns).has_value())
      return ConnectNext(ring, routes, now_ns);
    return {};
  }
  if (flags.Test<^^h2_detail::RuntimeFlags::connect_ready>()) {
    flags.Set<^^h2_detail::RuntimeFlags::connect_ready>(false);
    if (socket->LastConnectResult() < 0) {
      socket.reset();
      endpoints.MarkFailure(active_endpoint, now_ns);
      return ConnectNext(ring, routes, now_ns);
    }
    if (options.tls) {
      std::array<char, 256> sni_buf{};
      auto made = nexora::TlsSession::CreateClient(*options.tls, FormatTlsHost(sni_buf));
      if (!made) return std::unexpected(Status{StatusCode::kUnauthenticated});
      tls.emplace(std::move(*made));
    } else if (state != ChannelState::kShuttingDown) {
      state = ChannelState::kReady;
      MarkConnected(now_ns);
    }
  }
  if (socket->ConnectToken()) {
    if (options.connect_timeout_ns != 0 && now_ns >= connect_started_at &&
        now_ns - connect_started_at >= options.connect_timeout_ns)
      return std::unexpected(Status{StatusCode::kDeadlineExceeded});
    return {};
  }
  if (state == ChannelState::kConnecting && options.tls && options.connect_timeout_ns != 0 &&
      now_ns >= connect_started_at && now_ns - connect_started_at >= options.connect_timeout_ns)
    return std::unexpected(Status{StatusCode::kDeadlineExceeded});
  if (flags.Test<^^h2_detail::RuntimeFlags::tx_ready>()) {
    flags.Set<^^h2_detail::RuntimeFlags::tx_ready>(false);
    const auto result = socket->LastTxResult();
    if (result <= 0) return std::unexpected(Status{StatusCode::kUnavailable});
    const auto sent = static_cast<std::size_t>(result);
    if (sent > tx_size) return std::unexpected(Status{StatusCode::kInternal});
    if (tls && sent < tx_size) {
      std::memmove(network.data(), network.data() + sent, tx_size - sent);
      tx_size -= sent;
      if (!socket->SubmitSend(std::span<const std::uint8_t>(network).first(tx_size)))
        return std::unexpected(Status{StatusCode::kUnavailable});
    } else if (!tls) {
      output_offset += sent;
    }
  }
  if (flags.Test<^^h2_detail::RuntimeFlags::rx_ready>()) {
    flags.Set<^^h2_detail::RuntimeFlags::rx_ready>(false);
    const auto read = socket->LastRxResult();
    if (read <= 0) return std::unexpected(Status{StatusCode::kUnavailable});
    if (auto fed = Ingress(socket->RxBuffer().first(static_cast<std::size_t>(read)), now_ns); !fed)
      return fed;
  }
  if (auto sent = Egress(); !sent) return sent;
  if (!socket->RxToken() && !socket->SubmitRecv())
    return std::unexpected(Status{StatusCode::kUnavailable});
  return {};
}
}  // namespace grpcxx
