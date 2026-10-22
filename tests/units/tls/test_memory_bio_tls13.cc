#include <fmt/format.h>
#include <gtest/gtest.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <fstream>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "grpcxx/tls/tls_session.h"

namespace grpcxx {
namespace {

auto ServerOptions(bool mutual = false) -> TlsContextOptions {
  TlsContextOptions options;
  options.mode = TlsMode::kServer;
  options.verify_peer = mutual;
  options.require_client_certificate = mutual;
  options.certificate_file = fmt::format("{}/server-cert.pem", GRPCXX_TLS_FIXTURE_DIR);
  options.private_key_file = fmt::format("{}/server-key.pem", GRPCXX_TLS_FIXTURE_DIR);
  options.ca_file = options.certificate_file;
  return options;
}

auto ClientOptions(bool trusted = true) -> TlsContextOptions {
  TlsContextOptions options;
  if (trusted) {
    options.ca_file = fmt::format("{}/server-cert.pem", GRPCXX_TLS_FIXTURE_DIR);
  }
  return options;
}

auto Transfer(TlsSession& from, TlsSession& to, std::size_t fragment_size = 16'384) -> bool {
  std::array<std::byte, 16'384> ciphertext;
  while (from.PendingCiphertext() != 0) {
    auto drained = from.DrainCiphertext(std::span(ciphertext).first(fragment_size));
    if (!drained || *drained == 0) {
      return false;
    }
    auto remainder = std::span(ciphertext).first(*drained);
    while (!remainder.empty()) {
      auto fed = to.FeedCiphertext(remainder);
      if (!fed || *fed == 0) {
        return false;
      }
      remainder = remainder.subspan(*fed);
    }
  }
  return true;
}

auto Handshake(TlsSession& client, TlsSession& server, std::size_t fragment_size = 16'384) -> bool {
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    if (!client.PollOnce() || !Transfer(client, server, fragment_size)) {
      return false;
    }
    if (!server.PollOnce() || !Transfer(server, client, fragment_size)) {
      return false;
    }
    if (client.State() == TlsHandshakeState::kEstablished &&
        server.State() == TlsHandshakeState::kEstablished) {
      return true;
    }
  }
  return false;
}

TEST(MemoryBioTls13, HandshakeAlpnAndBidirectionalEncryptedExchange) {
  auto client_context = TlsContext::Create(ClientOptions());
  auto server_context = TlsContext::Create(ServerOptions());
  ASSERT_TRUE(client_context);
  ASSERT_TRUE(server_context);
  auto client = TlsSession::Create(*client_context, "localhost");
  auto server = TlsSession::Create(*server_context);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server));
  ASSERT_TRUE(client->VerifyAlpnH2());
  ASSERT_TRUE(server->VerifyAlpnH2());
  EXPECT_EQ(client->ProtocolVersion(), "TLSv1.3");
  EXPECT_EQ(server->ProtocolVersion(), "TLSv1.3");
  const std::string message = "TLS 1.3 carries a caller-driven gRPC record";
  const auto input = std::as_bytes(std::span(message));
  std::array<std::byte, 256> output;
  for (const auto reverse : {false, true}) {
    auto& writer = reverse ? *server : *client;
    auto& reader = reverse ? *client : *server;
    auto written = writer.WritePlaintext(input);
    ASSERT_TRUE(written);
    ASSERT_EQ(*written, input.size());
    EXPECT_GT(writer.PendingCiphertext(), input.size());
    ASSERT_TRUE(Transfer(writer, reader));
    auto received = reader.ReadPlaintext(output);
    ASSERT_TRUE(received);
    ASSERT_EQ(*received, input.size());
    EXPECT_TRUE(std::ranges::equal(std::span(output).first(*received), input));
  }
  ASSERT_TRUE(client->BeginShutdown());
  ASSERT_TRUE(client->PollOnce());
  ASSERT_TRUE(Transfer(*client, *server));
  auto received = server->ReadPlaintext(output);
  ASSERT_TRUE(received);
  EXPECT_EQ(*received, 0);
  ASSERT_TRUE(server->BeginShutdown());
  ASSERT_TRUE(server->PollOnce());
  ASSERT_TRUE(Transfer(*server, *client));
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    ASSERT_TRUE(client->PollOnce());
    ASSERT_TRUE(server->PollOnce());
    ASSERT_TRUE(Transfer(*client, *server));
    ASSERT_TRUE(Transfer(*server, *client));
    if (client->State() == TlsHandshakeState::kClosed &&
        server->State() == TlsHandshakeState::kClosed) {
      break;
    }
  }
  EXPECT_EQ(client->State(), TlsHandshakeState::kClosed);
  EXPECT_EQ(server->State(), TlsHandshakeState::kClosed);
}

TEST(MemoryBioTls13, RejectsUntrustedPeerAndHostnameMismatch) {
  for (const auto trusted : {false, true}) {
    auto client_context = TlsContext::Create(ClientOptions(trusted));
    auto server_context = TlsContext::Create(ServerOptions());
    ASSERT_TRUE(client_context);
    ASSERT_TRUE(server_context);
    auto client = TlsSession::Create(*client_context, trusted ? "wrong.example" : "localhost");
    auto server = TlsSession::Create(*server_context);
    ASSERT_TRUE(client);
    ASSERT_TRUE(server);
    EXPECT_FALSE(Handshake(*client, *server));
    EXPECT_NE(client->State(), TlsHandshakeState::kEstablished);
  }
}

TEST(MemoryBioTls13, MutualTlsRequiresClientCertificate) {
  auto client_context = TlsContext::Create(ClientOptions());
  auto server_context = TlsContext::Create(ServerOptions(true));
  ASSERT_TRUE(client_context);
  ASSERT_TRUE(server_context);
  auto client = TlsSession::Create(*client_context, "localhost");
  auto server = TlsSession::Create(*server_context);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  EXPECT_FALSE(Handshake(*client, *server));
  EXPECT_NE(server->State(), TlsHandshakeState::kEstablished);
}

TEST(MemoryBioTls13, MutualTlsAcceptsTrustedClientCertificate) {
  auto options = ClientOptions();
  options.certificate_file = fmt::format("{}/server-cert.pem", GRPCXX_TLS_FIXTURE_DIR);
  options.private_key_file = fmt::format("{}/server-key.pem", GRPCXX_TLS_FIXTURE_DIR);
  auto client_context = TlsContext::Create(options);
  auto server_context = TlsContext::Create(ServerOptions(true));
  ASSERT_TRUE(client_context);
  ASSERT_TRUE(server_context);
  auto client = TlsSession::Create(*client_context, "localhost");
  auto server = TlsSession::Create(*server_context);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server));
}

TEST(MemoryBioTls13, RejectsMismatchedAlpn) {
  auto server_context = TlsContext::Create(ServerOptions());
  ASSERT_TRUE(server_context);
  auto server = TlsSession::Create(*server_context);
  ASSERT_TRUE(server);
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> context(SSL_CTX_new(TLS_client_method()),
                                                            SSL_CTX_free);
  ASSERT_TRUE(context);
  ASSERT_EQ(SSL_CTX_set_min_proto_version(context.get(), TLS1_3_VERSION), 1);
  ASSERT_EQ(SSL_CTX_set_max_proto_version(context.get(), TLS1_3_VERSION), 1);
  SSL_CTX_set_verify(context.get(), SSL_VERIFY_NONE, nullptr);
  std::unique_ptr<SSL, decltype(&SSL_free)> client(SSL_new(context.get()), SSL_free);
  ASSERT_TRUE(client);
  BIO* input = BIO_new(BIO_s_mem());
  BIO* output = BIO_new(BIO_s_mem());
  ASSERT_NE(input, nullptr);
  ASSERT_NE(output, nullptr);
  SSL_set_bio(client.get(), input, output);
  SSL_set_connect_state(client.get());
  constexpr std::array<unsigned char, 9> kProtocols{8, 'h', 't', 't', 'p', '/', '1', '.', '1'};
  ASSERT_EQ(SSL_set_alpn_protos(client.get(), kProtocols.data(), kProtocols.size()), 0);
  static_cast<void>(SSL_do_handshake(client.get()));
  std::array<std::byte, 16'384> ciphertext;
  const auto size = BIO_read(output, ciphertext.data(), static_cast<int>(ciphertext.size()));
  ASSERT_GT(size, 0);
  auto fed = server->FeedCiphertext(std::span(ciphertext).first(static_cast<std::size_t>(size)));
  ASSERT_TRUE(fed);
  auto state = server->PollOnce();
  EXPECT_FALSE(state);
  EXPECT_EQ(server->State(), TlsHandshakeState::kFailed);
}

auto ReadFixture(std::string_view name) -> std::string {
  std::ifstream file(fmt::format("{}/{}", GRPCXX_TLS_FIXTURE_DIR, name));
  std::ostringstream text;
  text << file.rdbuf();
  return text.str();
}

auto DetachedSession(const TlsContextOptions& options,
                     std::string_view name = {}) -> std::expected<TlsSession, Status> {
  auto context = TlsContext::Create(options);
  if (!context) {
    return std::unexpected(context.error());
  }
  auto moved_context = std::move(*context);
  return TlsSession::Create(moved_context, name);
}

TEST(MemoryBioTls13, MemoryPemAndSessionOutlivesMovedContext) {
  auto server_options = ServerOptions();
  server_options.certificate_file.clear();
  server_options.private_key_file.clear();
  server_options.ca_file.clear();
  server_options.certificate_pem = ReadFixture("server-cert.pem");
  server_options.private_key_pem = ReadFixture("server-key.pem");
  auto client_options = ClientOptions();
  client_options.ca_file.clear();
  client_options.ca_pem = server_options.certificate_pem;
  auto client = DetachedSession(client_options, "localhost");
  auto server = DetachedSession(server_options);
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server));
  EXPECT_EQ(client->ProtocolVersion(), "TLSv1.3");
  ASSERT_TRUE(client->VerifyAlpnH2());
}

TEST(MemoryBioTls13, ClosingSessionStillReadsFinalPeerPlaintext) {
  auto client = DetachedSession(ClientOptions(), "localhost");
  auto server = DetachedSession(ServerOptions());
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server));
  const std::string message = "final peer response";
  const auto bytes = std::as_bytes(std::span(message));
  auto written = server->WritePlaintext(bytes);
  ASSERT_TRUE(written);
  ASSERT_EQ(*written, bytes.size());
  ASSERT_TRUE(client->BeginShutdown());
  ASSERT_TRUE(client->PollOnce());
  ASSERT_TRUE(Transfer(*server, *client));
  ASSERT_TRUE(client->PollOnce());
  std::array<std::byte, 128> output;
  auto read = client->ReadPlaintext(output);
  ASSERT_TRUE(read);
  EXPECT_EQ(*read, bytes.size());
  EXPECT_TRUE(std::ranges::equal(std::span(output).first(*read), bytes));
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    ASSERT_TRUE(client->PollOnce());
    ASSERT_TRUE(Transfer(*client, *server));
    if (server->State() == TlsHandshakeState::kEstablished) {
      ASSERT_TRUE(server->ReadPlaintext(output));
    }
    ASSERT_TRUE(server->PollOnce());
    ASSERT_TRUE(Transfer(*server, *client));
    if (client->State() == TlsHandshakeState::kClosed &&
        server->State() == TlsHandshakeState::kClosed) {
      break;
    }
  }
  EXPECT_EQ(client->State(), TlsHandshakeState::kClosed);
  EXPECT_EQ(server->State(), TlsHandshakeState::kClosed);
}

TEST(MemoryBioTls13, AbruptTransportEofFailsInsteadOfCleanClose) {
  auto client = DetachedSession(ClientOptions(), "localhost");
  auto server = DetachedSession(ServerOptions());
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server));
  server->MarkTransportEof();
  std::array<std::byte, 32> plaintext;
  EXPECT_FALSE(server->ReadPlaintext(plaintext));
  EXPECT_EQ(server->State(), TlsHandshakeState::kFailed);
}

TEST(MemoryBioTls13, InvalidConfigurationFailsWithoutHandshake) {
  auto options = ServerOptions();
  options.private_key_pem = "invalid private key";
  options.private_key_file.clear();
  EXPECT_FALSE(TlsContext::Create(options));
  options = ClientOptions();
  options.ca_pem = "invalid CA";
  options.ca_file.clear();
  EXPECT_FALSE(TlsContext::Create(options));
}

TEST(MemoryBioTls13, FragmentedHandshakeAndBackpressuredLargeRecordExchange) {
  auto client = DetachedSession(ClientOptions(), "localhost");
  auto server = DetachedSession(ServerOptions());
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  ASSERT_TRUE(Handshake(*client, *server, 1));
  std::vector<std::byte> input(600 * 1024);
  for (const auto index : std::views::iota(std::size_t{0}, input.size())) {
    input[index] = static_cast<std::byte>(index % 251);
  }
  std::size_t sent = 0;
  bool blocked = false;
  for (const auto tick : std::views::iota(0, 128)) {
    static_cast<void>(tick);
    auto written = client->WritePlaintext(std::span(input).subspan(sent));
    ASSERT_TRUE(written);
    if (*written == 0) {
      blocked = true;
      break;
    }
    sent += *written;
  }
  ASSERT_TRUE(blocked);
  EXPECT_LT(sent, input.size());
  std::vector<std::byte> received;
  std::array<std::byte, 257> output;
  std::array<std::byte, 256> ciphertext;
  for (const auto tick : std::views::iota(0, 8192)) {
    static_cast<void>(tick);
    auto drained = client->DrainCiphertext(ciphertext);
    ASSERT_TRUE(drained);
    auto fed = server->FeedCiphertext(std::span(ciphertext).first(*drained));
    ASSERT_TRUE(fed);
    ASSERT_EQ(*fed, *drained);
    auto read = server->ReadPlaintext(output);
    ASSERT_TRUE(read);
    for (const auto byte : std::span(output).first(*read)) received.push_back(byte);
    if (sent != input.size()) {
      auto written = client->WritePlaintext(std::span(input).subspan(sent));
      ASSERT_TRUE(written);
      sent += *written;
    }
    if (received.size() == input.size()) break;
  }
  EXPECT_EQ(sent, input.size());
  EXPECT_TRUE(std::ranges::equal(received, input));
  EXPECT_EQ(client->State(), TlsHandshakeState::kEstablished);
  EXPECT_EQ(server->State(), TlsHandshakeState::kEstablished);
}

TEST(MemoryBioTls13, InputBufferRejectsOverflowWithoutGrowing) {
  auto server = DetachedSession(ServerOptions());
  ASSERT_TRUE(server);
  std::vector<std::byte> bytes(300 * 1024, std::byte{0});
  auto accepted = server->FeedCiphertext(bytes);
  ASSERT_TRUE(accepted);
  EXPECT_EQ(*accepted, 256 * 1024);
  auto blocked = server->FeedCiphertext(bytes);
  ASSERT_TRUE(blocked);
  EXPECT_EQ(*blocked, 0);
}

TEST(MemoryBioTls13, RejectedOperationsPreserveHandshakeAndMovedSessionOwnership) {
  auto client = DetachedSession(ClientOptions(), "localhost");
  auto server = DetachedSession(ServerOptions());
  ASSERT_TRUE(client);
  ASSERT_TRUE(server);
  std::array<std::byte, 16> output;
  EXPECT_FALSE(client->WritePlaintext(output));
  EXPECT_FALSE(client->ReadPlaintext(output));
  EXPECT_FALSE(client->VerifyAlpnH2());
  EXPECT_FALSE(client->BeginShutdown());
  EXPECT_EQ(client->RejectedTransitions(), 4);
  EXPECT_EQ(client->State(), TlsHandshakeState::kHandshaking);
  auto moved = std::move(*client);
  EXPECT_FALSE(client->PollOnce());
  EXPECT_FALSE(client->DrainCiphertext(output));
  EXPECT_FALSE(client->BeginShutdown());
  EXPECT_EQ(moved.RejectedTransitions(), 4);
  ASSERT_TRUE(Handshake(moved, *server));
  const std::string message = "owner moved without losing the pending TLS connection";
  const auto bytes = std::as_bytes(std::span(message));
  auto written = moved.WritePlaintext(bytes);
  ASSERT_TRUE(written);
  ASSERT_EQ(*written, bytes.size());
  ASSERT_TRUE(Transfer(moved, *server));
  std::array<std::byte, 128> plaintext;
  auto read = server->ReadPlaintext(plaintext);
  ASSERT_TRUE(read);
  EXPECT_TRUE(std::ranges::equal(std::span(plaintext).first(*read), bytes));
}

}  // namespace
}  // namespace grpcxx
