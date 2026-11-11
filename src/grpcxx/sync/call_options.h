#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <nexora/tls/tls_config.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"

namespace grpcxx {

enum class EndpointScheme : std::uint8_t { kGrpc = 0, kGrpcs = 1, kUnix = 2 };

class SyncEndpoint final {
 public:
  [[nodiscard]] static std::expected<SyncEndpoint, Status> Parse(std::string_view uri) noexcept;

  [[nodiscard]] constexpr EndpointScheme Scheme() const noexcept { return scheme_; }
  [[nodiscard]] constexpr std::string_view Host() const noexcept { return host_; }
  [[nodiscard]] constexpr std::uint16_t Port() const noexcept { return port_; }
  [[nodiscard]] constexpr std::string_view UnixPath() const noexcept { return unix_path_; }
  [[nodiscard]] constexpr std::string_view Authority() const noexcept { return authority_; }
  [[nodiscard]] constexpr std::string_view SocketAddress() const noexcept {
    return scheme_ == EndpointScheme::kUnix ? std::string_view{unix_path_}
                                            : std::string_view{authority_};
  }

 private:
  EndpointScheme scheme_{EndpointScheme::kGrpc};
  std::string host_{};
  std::uint16_t port_{0};
  std::string unix_path_{};
  std::string authority_{};
};

struct CallOptions final {
  MetadataMap metadata{};
  std::chrono::nanoseconds timeout{std::chrono::seconds{30}};
  CompressionAlgorithm compression{CompressionAlgorithm::kIdentity};
  std::string_view content_type{"application/grpc"};
};

struct RpcResponse final {
  std::vector<std::byte> payload{};
  MetadataMap initial_metadata{};
  MetadataMap trailing_metadata{};
  Status status{Status::OkStatus()};
};

struct SyncTlsClientOptions final {
  const nexora::TlsClientConfig* config{nullptr};
  std::optional<std::string> ca_cert_path{};
  std::optional<std::string> client_cert_path{};
  std::optional<std::string> client_key_path{};
  std::optional<std::string> server_name_override{};
  bool insecure_skip_verify{false};

  [[nodiscard]] std::expected<nexora::TlsClientConfig, Status> BuildConfig() const noexcept;
};

struct SynchronousChannelOptions final {
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds resolve_timeout{5000};
  std::chrono::milliseconds read_timeout{0};
  std::chrono::milliseconds write_timeout{5000};
  std::size_t max_message_bytes{16uz * 1024uz * 1024uz};
  std::size_t max_queued_bytes{32uz * 1024uz * 1024uz};
  std::size_t max_metadata_bytes{65536uz};
  std::uint32_t max_concurrent_streams{32U};
  std::uint32_t flow_control_window_bytes{16U * 1024U * 1024U};
  bool tcp_nodelay{true};
  bool tcp_keepalive{false};
  std::string authority_override{};

  [[nodiscard]] std::expected<void, Status> Validate() const noexcept;
};

}  // namespace grpcxx
