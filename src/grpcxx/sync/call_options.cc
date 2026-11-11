#include "grpcxx/sync/call_options.h"

#include <array>
#include <charconv>
#include <limits>
#include <string>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {
namespace {

[[nodiscard, gnu::cold, gnu::noinline]] Status MakeOptionStatus(StatusCode code,
                                                                std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard, gnu::cold, gnu::noinline]] Status MapTlsOptionError(const nexora::Error& err,
                                                                 std::string_view prefix) noexcept {
  uarch::cold_path();
  std::string msg{prefix};
  msg.append(": ");
  msg.append(err.Message());
  return Status{StatusCode::kInvalidArgument, std::move(msg)};
}

}  // namespace

std::expected<SyncEndpoint, Status> SyncEndpoint::Parse(std::string_view uri) noexcept {
  if (uri.empty()) {
    return std::unexpected(MakeOptionStatus(StatusCode::kInvalidArgument, "endpoint URI is empty"));
  }
  SyncEndpoint ep{};
  std::string_view rest = uri;
  if (rest.starts_with("grpc://")) {
    ep.scheme_ = EndpointScheme::kGrpc;
    rest.remove_prefix(7uz);
  } else if (rest.starts_with("grpcs://")) {
    ep.scheme_ = EndpointScheme::kGrpcs;
    rest.remove_prefix(8uz);
  } else if (rest.starts_with("unix://")) {
    ep.scheme_ = EndpointScheme::kUnix;
    rest.remove_prefix(7uz);
  } else if (rest.starts_with("unix:")) {
    ep.scheme_ = EndpointScheme::kUnix;
    rest.remove_prefix(5uz);
  } else if (rest.contains("://")) {
    return std::unexpected(
        MakeOptionStatus(StatusCode::kInvalidArgument, "unsupported endpoint scheme"));
  }

  if (ep.scheme_ == EndpointScheme::kUnix) {
    if (rest.empty()) {
      return std::unexpected(MakeOptionStatus(StatusCode::kInvalidArgument, "empty unix path"));
    }
    ep.unix_path_.assign(rest);
    ep.authority_ = "localhost";
    return ep;
  }

  if (rest.contains('/') || rest.contains('?') || rest.contains('#')) {
    return std::unexpected(
        MakeOptionStatus(StatusCode::kInvalidArgument, "TCP endpoint must not contain path/query"));
  }

  std::string_view host_part{};
  std::string_view port_part{};
  if (rest.starts_with('[')) {
    const auto close_bracket = rest.find(']');
    if (close_bracket == std::string_view::npos || close_bracket == 1uz ||
        close_bracket + 2uz > rest.size() || rest[close_bracket + 1uz] != ':') {
      return std::unexpected(
          MakeOptionStatus(StatusCode::kInvalidArgument, "invalid IPv6 endpoint host:port"));
    }
    host_part = rest.substr(1uz, close_bracket - 1uz);
    port_part = rest.substr(close_bracket + 2uz);
  } else {
    const auto colon = rest.rfind(':');
    if (colon == std::string_view::npos || colon == 0uz || colon + 1uz >= rest.size() ||
        rest.find(':') != colon) {
      return std::unexpected(
          MakeOptionStatus(StatusCode::kInvalidArgument, "endpoint requires host:port"));
    }
    host_part = rest.substr(0uz, colon);
    port_part = rest.substr(colon + 1uz);
  }

  std::uint32_t parsed_port = 0U;
  const auto [ptr, ec] =
      std::from_chars(port_part.data(), port_part.data() + port_part.size(), parsed_port);
  if (ec != std::errc{} || ptr != port_part.data() + port_part.size() || parsed_port == 0U ||
      parsed_port > 65535U) {
    return std::unexpected(MakeOptionStatus(StatusCode::kInvalidArgument, "invalid endpoint port"));
  }
  ep.host_.assign(host_part);
  ep.port_ = static_cast<std::uint16_t>(parsed_port);
  ep.authority_.assign(rest);
  return ep;
}

std::expected<nexora::TlsClientConfig, Status> SyncTlsClientOptions::BuildConfig() const noexcept {
  if (client_cert_path.has_value() != client_key_path.has_value()) {
    return std::unexpected(MakeOptionStatus(StatusCode::kInvalidArgument,
                                            "TLS client cert and key paths must both be set"));
  }
  auto base = insecure_skip_verify ? nexora::TlsClientConfig::CreateInsecureForTest()
                                   : nexora::TlsClientConfig::CreateDefault();
  if (!base) return std::unexpected(MapTlsOptionError(base.error(), "TLS client config"));
  if (ca_cert_path.has_value() && !ca_cert_path->empty()) {
    if (auto ok = base->SetCaCertFile(*ca_cert_path); !ok) {
      return std::unexpected(MapTlsOptionError(ok.error(), "TLS CA bundle"));
    }
  }
  if (client_cert_path.has_value()) {
    if (auto ok = base->SetClientIdentityFiles(*client_cert_path, *client_key_path); !ok) {
      return std::unexpected(MapTlsOptionError(ok.error(), "TLS client identity"));
    }
  }
  static constexpr std::array<nexora::AlpnProtocol, 1> kH2Only{nexora::AlpnProtocol::kHttp2};
  if (auto ok = base->SetAlpn(kH2Only); !ok) {
    return std::unexpected(MapTlsOptionError(ok.error(), "TLS ALPN"));
  }
  if (auto ok = base->SetRequiredAlpn(nexora::AlpnProtocol::kHttp2); !ok) {
    return std::unexpected(MapTlsOptionError(ok.error(), "TLS required ALPN"));
  }
  return std::move(*base);
}

std::expected<void, Status> SynchronousChannelOptions::Validate() const noexcept {
  if (connect_timeout.count() <= 0 || resolve_timeout.count() <= 0 || read_timeout.count() < 0 ||
      write_timeout.count() <= 0) {
    return std::unexpected(MakeOptionStatus(
        StatusCode::kInvalidArgument, "connect, resolve, and write timeouts must be positive"));
  }
  if (max_message_bytes == 0uz || max_message_bytes > std::numeric_limits<std::uint32_t>::max() ||
      max_queued_bytes < max_message_bytes + 5uz || max_metadata_bytes == 0uz ||
      max_concurrent_streams == 0U || max_concurrent_streams > 64U ||
      flow_control_window_bytes < 65535U || flow_control_window_bytes > 0x7fffffffU) {
    return std::unexpected(MakeOptionStatus(StatusCode::kInvalidArgument,
                                            "invalid channel capacity or window limits"));
  }
  return {};
}

}  // namespace grpcxx
