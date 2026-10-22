#pragma once

#include <expected>
#include <string>

#include "grpcxx/core/status.h"

namespace grpcxx {

enum class TlsMode { kClient, kServer };

/** Configuration loaded before the caller-driven connection path starts. */
struct TlsContextOptions {
  TlsMode mode{TlsMode::kClient};
  bool verify_peer{true};
  bool require_client_certificate{false};
  std::string certificate_pem;
  std::string private_key_pem;
  std::string ca_pem;
  std::string certificate_file;
  std::string private_key_file;
  std::string ca_file;
};

/** Move-only TLS 1.3 configuration with mandatory HTTP/2 ALPN. */
class TlsContext final {
 public:
  static auto Create(const TlsContextOptions& options) -> std::expected<TlsContext, Status>;
  ~TlsContext();
  TlsContext(TlsContext&& other) noexcept;
  auto operator=(TlsContext&& other) noexcept -> TlsContext&;
  TlsContext(const TlsContext&) = delete;
  auto operator=(const TlsContext&) -> TlsContext& = delete;

 private:
  friend class TlsSession;
  TlsContext(void* handle, TlsMode mode, bool verify_peer) noexcept;
  void* handle_{nullptr};
  TlsMode mode_{TlsMode::kClient};
  bool verify_peer_{true};
};

}  // namespace grpcxx
