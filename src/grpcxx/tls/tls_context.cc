#include "grpcxx/tls/tls_context.h"

#include <fmt/format.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <utility>

namespace grpcxx {
namespace {

auto Failure(std::string_view operation) -> Status {
  std::array<char, 256> description{};
  ERR_error_string_n(ERR_get_error(), description.data(), description.size());
  ERR_clear_error();
  return Status{StatusCode::kInternal, fmt::format("TLS {}: {}", operation, description.data())};
}

auto ValidPemSize(std::string_view pem) -> bool {
  return pem.size() <= static_cast<std::size_t>(std::numeric_limits<int>::max());
}

auto RejectPasswordPrompt(char*, int, int, void*) -> int { return 0; }

auto SelectH2(SSL*, const unsigned char** selected, unsigned char* selected_size,
              const unsigned char* offered, unsigned int offered_size, void*) -> int {
  auto protocols = std::span(offered, offered_size);
  while (!protocols.empty()) {
    const auto length = protocols.front();
    protocols = protocols.subspan(1);
    if (length == 0 || length > protocols.size()) {
      return SSL_TLSEXT_ERR_ALERT_FATAL;
    }
    if (length == 2 && protocols[0] == 'h' && protocols[1] == '2') {
      *selected = protocols.data();
      *selected_size = 2;
      return SSL_TLSEXT_ERR_OK;
    }
    protocols = protocols.subspan(length);
  }
  return SSL_TLSEXT_ERR_ALERT_FATAL;
}

auto LoadCertificates(SSL_CTX* context,
                      const TlsContextOptions& options) -> std::expected<void, Status> {
  if (!options.certificate_pem.empty()) {
    std::unique_ptr<BIO, decltype(&BIO_free)> input(
        BIO_new_mem_buf(options.certificate_pem.data(),
                        static_cast<int>(options.certificate_pem.size())),
        BIO_free);
    if (!input) return std::unexpected(Failure("certificate BIO allocation"));
    std::unique_ptr<X509, decltype(&X509_free)> certificate(
        PEM_read_bio_X509_AUX(input.get(), nullptr, nullptr, nullptr), X509_free);
    if (!certificate || SSL_CTX_use_certificate(context, certificate.get()) != 1) {
      return std::unexpected(Failure("certificate PEM loading"));
    }
    while (BIO_ctrl_pending(input.get()) != 0) {
      std::unique_ptr<X509, decltype(&X509_free)> chain(
          PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
      if (!chain) {
        if (ERR_GET_REASON(ERR_peek_last_error()) == PEM_R_NO_START_LINE) {
          ERR_clear_error();
          break;
        }
        return std::unexpected(Failure("certificate chain PEM loading"));
      }
      if (SSL_CTX_add1_chain_cert(context, chain.get()) != 1) {
        return std::unexpected(Failure("certificate chain installation"));
      }
    }
  } else if (!options.certificate_file.empty() &&
             SSL_CTX_use_certificate_chain_file(context, options.certificate_file.c_str()) != 1) {
    return std::unexpected(Failure("certificate file loading"));
  }
  if (!options.private_key_pem.empty()) {
    std::unique_ptr<BIO, decltype(&BIO_free)> input(
        BIO_new_mem_buf(options.private_key_pem.data(),
                        static_cast<int>(options.private_key_pem.size())),
        BIO_free);
    if (!input) return std::unexpected(Failure("private-key BIO allocation"));
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
        PEM_read_bio_PrivateKey(input.get(), nullptr, RejectPasswordPrompt, nullptr),
        EVP_PKEY_free);
    if (!key || SSL_CTX_use_PrivateKey(context, key.get()) != 1) {
      return std::unexpected(Failure("private-key PEM loading"));
    }
  } else if (!options.private_key_file.empty() &&
             SSL_CTX_use_PrivateKey_file(context, options.private_key_file.c_str(),
                                         SSL_FILETYPE_PEM) != 1) {
    return std::unexpected(Failure("private-key file loading"));
  }
  if ((!options.certificate_pem.empty() || !options.certificate_file.empty()) &&
      SSL_CTX_check_private_key(context) != 1) {
    return std::unexpected(Failure("certificate/private-key matching"));
  }
  return {};
}

auto LoadTrust(SSL_CTX* context, const TlsContextOptions& options) -> std::expected<void, Status> {
  if (!options.ca_file.empty() &&
      SSL_CTX_load_verify_locations(context, options.ca_file.c_str(), nullptr) != 1) {
    return std::unexpected(Failure("CA file loading"));
  }
  if (!options.ca_pem.empty()) {
    std::unique_ptr<BIO, decltype(&BIO_free)> input(
        BIO_new_mem_buf(options.ca_pem.data(), static_cast<int>(options.ca_pem.size())), BIO_free);
    if (!input) return std::unexpected(Failure("CA BIO allocation"));
    bool loaded = false;
    while (BIO_ctrl_pending(input.get()) != 0) {
      std::unique_ptr<X509, decltype(&X509_free)> certificate(
          PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
      if (!certificate) {
        if (loaded && ERR_GET_REASON(ERR_peek_last_error()) == PEM_R_NO_START_LINE) {
          ERR_clear_error();
          break;
        }
        return std::unexpected(Failure("CA PEM loading"));
      }
      if (X509_STORE_add_cert(SSL_CTX_get_cert_store(context), certificate.get()) != 1) {
        return std::unexpected(Failure("CA installation"));
      }
      loaded = true;
    }
    if (!loaded) return std::unexpected(Failure("empty CA PEM"));
  } else if (options.ca_file.empty() &&
             (options.verify_peer || options.require_client_certificate) &&
             SSL_CTX_set_default_verify_paths(context) != 1) {
    return std::unexpected(Failure("default CA paths loading"));
  }
  return {};
}

}  // namespace

TlsContext::TlsContext(void* handle, TlsMode mode, bool verify_peer) noexcept
    : handle_(handle), mode_(mode), verify_peer_(verify_peer) {}

TlsContext::~TlsContext() { SSL_CTX_free(static_cast<SSL_CTX*>(handle_)); }

TlsContext::TlsContext(TlsContext&& other) noexcept
    : handle_(std::exchange(other.handle_, nullptr)),
      mode_(other.mode_),
      verify_peer_(other.verify_peer_) {}

auto TlsContext::operator=(TlsContext&& other) noexcept -> TlsContext& {
  if (this != &other) {
    SSL_CTX_free(static_cast<SSL_CTX*>(handle_));
    handle_ = std::exchange(other.handle_, nullptr);
    mode_ = other.mode_;
    verify_peer_ = other.verify_peer_;
  }
  return *this;
}

auto TlsContext::Create(const TlsContextOptions& options) -> std::expected<TlsContext, Status> {
  ERR_clear_error();
  const auto pem_inputs = std::array<std::string_view, 3>{options.certificate_pem,
                                                          options.private_key_pem, options.ca_pem};
  if (!std::ranges::all_of(pem_inputs, ValidPemSize)) {
    return std::unexpected(Status{StatusCode::kResourceExhausted, "TLS PEM input is too large"});
  }
  const auto paths = std::array<std::string_view, 3>{options.certificate_file,
                                                     options.private_key_file, options.ca_file};
  if (std::ranges::any_of(
          paths, [](std::string_view path) { return path.find('\0') != std::string_view::npos; })) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument, "TLS file paths must not contain NUL bytes"});
  }
  const bool has_certificate =
      !options.certificate_pem.empty() || !options.certificate_file.empty();
  const bool has_key = !options.private_key_pem.empty() || !options.private_key_file.empty();
  if (has_certificate != has_key || (options.mode == TlsMode::kServer && !has_certificate) ||
      (options.require_client_certificate && options.mode != TlsMode::kServer)) {
    return std::unexpected(
        Status{StatusCode::kInvalidArgument,
               "TLS certificate and key must be provided together; servers require both"});
  }
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> context(SSL_CTX_new(TLS_method()),
                                                            SSL_CTX_free);
  if (!context) return std::unexpected(Failure("context allocation"));
  SSL_CTX_set_default_passwd_cb(context.get(), RejectPasswordPrompt);
  if (SSL_CTX_set_min_proto_version(context.get(), TLS1_3_VERSION) != 1 ||
      SSL_CTX_set_max_proto_version(context.get(), TLS1_3_VERSION) != 1 ||
      SSL_CTX_set_max_early_data(context.get(), 0) != 1 ||
      SSL_CTX_set_recv_max_early_data(context.get(), 0) != 1) {
    return std::unexpected(Failure("TLS 1.3 configuration"));
  }
  SSL_CTX_set_options(context.get(), SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set_session_cache_mode(context.get(), SSL_SESS_CACHE_OFF);
  int verify_mode = options.verify_peer ? SSL_VERIFY_PEER : SSL_VERIFY_NONE;
  if (options.require_client_certificate)
    verify_mode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  SSL_CTX_set_verify(context.get(), verify_mode, nullptr);
  if (auto result = LoadCertificates(context.get(), options); !result)
    return std::unexpected(result.error());
  if (auto result = LoadTrust(context.get(), options); !result)
    return std::unexpected(result.error());
  SSL_CTX_set_alpn_select_cb(context.get(), SelectH2, nullptr);
  return TlsContext(context.release(), options.mode, options.verify_peer);
}

}  // namespace grpcxx
