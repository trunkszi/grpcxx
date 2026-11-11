#include <fmt/format.h>
#include <grpcxx/h2/session_detail.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <nexora/tls/tls_session.hpp>
#include <ranges>
#include <span>
#include <string>

#include "grpcxx/compression/compression_registry.h"

namespace {

auto Transfer(nexora::TlsSession& from, nexora::TlsSession& to) -> bool {
  std::array<std::uint8_t, 16384> buffer;
  while (from.HasPendingCiphertextOut()) {
    auto taken = from.TakeCiphertext(buffer);
    if (!taken || *taken == 0) return false;
    if (!to.FeedCiphertext(std::span(buffer).first(*taken))) return false;
  }
  return true;
}

}  // namespace

/** Runs TLS 1.3 and gRPC compression without sockets or background threads. */
auto main() -> int {
  auto server_context = nexora::TlsServerConfig::SelfSignedForLocalhost();
  auto client_context = nexora::TlsClientConfig::CreateInsecureForTest();
  if (!server_context || !client_context) return 1;
  if (!server_context->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2) ||
      !client_context->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2))
    return 1;
  auto server = nexora::TlsSession::CreateServer(*server_context);
  auto client = nexora::TlsSession::CreateClient(*client_context, "localhost");
  if (!server || !client) return 1;
  bool established = false;
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    if (!client->DriveHandshake() || !Transfer(*client, *server) || !server->DriveHandshake() ||
        !Transfer(*server, *client)) {
      return 1;
    }
    if (client->IsHandshakeComplete() && server->IsHandshakeComplete()) {
      established = true;
      break;
    }
  }
  if (!established || client->NegotiatedAlpn() != nexora::AlpnProtocol::kHttp2 ||
      server->NegotiatedAlpn() != nexora::AlpnProtocol::kHttp2) {
    return 1;
  }
  grpcxx::CompressionRegistry compression;
  const std::string message(4096, 'x');
  for (const auto algorithm :
       {grpcxx::CompressionAlgorithm::kZstd, grpcxx::CompressionAlgorithm::kLz4,
        grpcxx::CompressionAlgorithm::kGzip, grpcxx::CompressionAlgorithm::kDeflate,
        grpcxx::CompressionAlgorithm::kSnappy}) {
    auto encoded = compression.Compress(algorithm, std::as_bytes(std::span(message)));
    if (!encoded) {
      return 1;
    }
    auto written = client->WritePlaintext(grpcxx::h2_detail::Wire(encoded->Payload()));
    if (!written || *written != encoded->Payload().size() || !Transfer(*client, *server)) {
      return 1;
    }
    std::array<std::uint8_t, 8192> buffer;
    auto received = server->ReadPlaintext(buffer);
    if (!received) {
      return 1;
    }
    auto decoded = compression.Decompress(
        algorithm, std::as_bytes(std::span(buffer).first(*received)), message.size());
    if (!decoded || !std::ranges::equal(decoded->Payload(), std::as_bytes(std::span(message)))) {
      return 1;
    }
    fmt::print("TLS 1.3 / ALPN h2: {} bytes compressed to {} bytes, restored successfully\n",
               message.size(), *received);
  }
  return 0;
}
