#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <ranges>
#include <span>
#include <string>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/tls/tls_session.h"

namespace {

auto Transfer(grpcxx::TlsSession& from, grpcxx::TlsSession& to) -> bool {
  std::array<std::byte, 16'384> buffer;
  while (from.PendingCiphertext() != 0) {
    auto drained = from.DrainCiphertext(buffer);
    if (!drained || *drained == 0) {
      return false;
    }
    auto pending = std::span(buffer).first(*drained);
    while (!pending.empty()) {
      auto fed = to.FeedCiphertext(pending);
      if (!fed || *fed == 0) {
        return false;
      }
      pending = pending.subspan(*fed);
    }
  }
  return true;
}

}  // namespace

/** Runs TLS 1.3 and gRPC compression without sockets or background threads. */
auto main() -> int {
  grpcxx::TlsContextOptions server_options;
  server_options.mode = grpcxx::TlsMode::kServer;
  server_options.verify_peer = false;
  server_options.certificate_file = fmt::format("{}/server-cert.pem", GRPCXX_TLS_FIXTURE_DIR);
  server_options.private_key_file = fmt::format("{}/server-key.pem", GRPCXX_TLS_FIXTURE_DIR);
  grpcxx::TlsContextOptions client_options;
  client_options.ca_file = server_options.certificate_file;
  auto server_context = grpcxx::TlsContext::Create(server_options);
  auto client_context = grpcxx::TlsContext::Create(client_options);
  if (!server_context || !client_context) {
    fmt::print(stderr, "TLS configuration failed\n");
    return 1;
  }
  auto server = grpcxx::TlsSession::Create(*server_context);
  auto client = grpcxx::TlsSession::Create(*client_context, "localhost");
  if (!server || !client) {
    fmt::print(stderr, "TLS session creation failed\n");
    return 1;
  }
  bool established = false;
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    if (!client->PollOnce() || !Transfer(*client, *server) || !server->PollOnce() ||
        !Transfer(*server, *client)) {
      return 1;
    }
    if (client->State() == grpcxx::TlsHandshakeState::kEstablished &&
        server->State() == grpcxx::TlsHandshakeState::kEstablished) {
      established = true;
      break;
    }
  }
  if (!established || !client->VerifyAlpnH2() || !server->VerifyAlpnH2()) {
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
    auto written = client->WritePlaintext(encoded->Payload());
    if (!written || *written != encoded->Payload().size() || !Transfer(*client, *server)) {
      return 1;
    }
    std::array<std::byte, 8192> buffer;
    auto received = server->ReadPlaintext(buffer);
    if (!received) {
      return 1;
    }
    auto decoded =
        compression.Decompress(algorithm, std::span(buffer).first(*received), message.size());
    if (!decoded || !std::ranges::equal(decoded->Payload(), std::as_bytes(std::span(message)))) {
      return 1;
    }
    fmt::print("TLS 1.3 / ALPN h2: {} bytes compressed to {} bytes, restored successfully\n",
               message.size(), *received);
  }
  return 0;
}
