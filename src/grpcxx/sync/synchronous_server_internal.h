#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <nexora/http2/connection.hpp>
#include <nexora/http2/connection_headers.hpp>
#include <nexora/http2/connection_rx.hpp>
#include <nexora/http2/connection_submit.hpp>
#include <nexora/http2/connection_tx.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <uarch/uarch.hpp>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"
#include "grpcxx/sync/sync_wire_transport.h"
#include "grpcxx/sync/synchronous_server.h"

namespace grpcxx {

[[nodiscard, gnu::cold, gnu::noinline]] inline Status MakeSrvError(
    StatusCode code, std::string_view message) noexcept {
  uarch::cold_path();
  return Status{code, std::string{message}};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status MapSrvH2Error(
    const nexora::Error& err) noexcept {
  uarch::cold_path();
  std::string msg = "HTTP/2 server protocol error: ";
  msg.append(err.Message());
  return Status{StatusCode::kInternal, std::move(msg)};
}

struct ServerStreamMetaSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Flag{0}]] bool active{};
  [[= uarch::bits::Flag{1}]] bool started{};
  [[= uarch::bits::Flag{2}]] bool finished{};
  [[= uarch::bits::Flag{3}]] bool trailers_submitted{};
  [[= uarch::bits::Flag{4}]] bool half_closed{};
  [[= uarch::bits::Flag{5}]] bool rejected{};
  [[= uarch::bits::Flag{6}]] bool in_dispatch{};
};

struct SynchronousServer::Impl final {
  static constexpr std::size_t kMaxSlots = 64uz;
  static constexpr std::size_t kIoBufferBytes = 262144uz;
  static constexpr std::uint32_t kWindowReplenishThreshold = 32768U;

  struct StreamSlot final {
    std::uint32_t slot_index{0U};
    std::int32_t id{0};
    std::string path{};
    MetadataMap request_metadata{};
    MetadataMap initial{};
    MetadataMap trailers{};
    std::optional<std::uint64_t> timeout_ns{};
    CompressionAlgorithm request_encoding{CompressionAlgorithm::kIdentity};
    CompressionAlgorithm response_encoding{CompressionAlgorithm::kIdentity};
    LengthPrefixedFrame parser{0uz};
    std::deque<AlignedMessageBuf> output{};
    std::size_t output_offset{0uz};
    std::size_t queued_bytes{0uz};
    std::uint32_t unreplenished_recv_bytes{0U};
    Status status{Status::OkStatus()};
    uarch::BitWord<ServerStreamMetaSpec> meta{};

    [[nodiscard, gnu::always_inline]] constexpr bool IsActive() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::active>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsStarted() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::started>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsFinished() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::finished>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsTrailersSubmitted() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::trailers_submitted>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsHalfClosed() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::half_closed>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsRejected() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::rejected>();
    }
    [[nodiscard, gnu::always_inline]] constexpr bool IsInDispatch() const noexcept {
      return meta.test<^^ServerStreamMetaSpec::in_dispatch>();
    }
  };

  [[nodiscard]] std::size_t OutputFreeBytes() const noexcept {
    const auto used = h2 ? h2->PendingOutput().size() : kIoBufferBytes;
    return used < kIoBufferBytes ? (kIoBufferBytes - used) : 0uz;
  }

  [[nodiscard]] std::expected<void, Status> Initialize() noexcept;
  [[nodiscard]] StreamSlot* Find(std::int32_t id) noexcept;
  void ReleaseSlot(StreamSlot& slot) noexcept;
  void MaybeReclaimCompleted(StreamSlot& slot) noexcept;
  void RejectStream(StreamSlot& slot, nexora::Http2ErrorCode code) noexcept;
  void CloseOnPeerEof() noexcept;
  [[nodiscard]] static bool IsPeerTransportClosed(const Status& status) noexcept;
  [[nodiscard]] std::expected<void, Status> DrainInbound() noexcept;
  [[nodiscard]] std::expected<void, Status> FeedInboundBytes(
      std::span<const std::uint8_t> wire) noexcept;
  [[nodiscard]] std::expected<void, Status> FinishPeerClose() noexcept;
  [[nodiscard]] std::expected<void, Status> EnsureStarted(StreamSlot& slot) noexcept;
  [[nodiscard]] std::expected<void, Status> PumpOutgoing() noexcept;
  [[nodiscard]] std::expected<void, Status> FlushWire() noexcept;
  void Dispatch(StreamSlot& slot, ServerRequest::Event event,
                std::span<const std::byte> payload = {}) noexcept;
  void HandleRequestHeaders(const nexora::Http2EventView& ev) noexcept;
  void HandleRequestData(const nexora::Http2EventView& ev) noexcept;
  void DrainEvents() noexcept;

  sync_detail::SyncWireTransport transport{};
  SynchronousServerOptions options{};
  CompressionRegistry compression{256uz};
  std::unique_ptr<nexora::Http2Connection> h2{};
  std::array<StreamSlot, kMaxSlots> streams{};
  std::array<std::byte, 16384> rx_scratch{};
  uarch::Bitmap64 active_streams_mask{};
  HandlerDelegate handler{};
  HeadersDelegate headers_handler{};
  SynchronousServer* owner_server{nullptr};
  Status fatal_error{Status::OkStatus()};
  std::uint32_t conn_unreplenished_recv_bytes{0U};
  std::uint32_t last_peer_stream_id{0U};
  std::size_t cancelled_streams{0uz};
  std::size_t window_updates_received{0uz};
  bool polling{false};
  bool closed{false};
  bool peer_eof{false};
};

}  // namespace grpcxx
