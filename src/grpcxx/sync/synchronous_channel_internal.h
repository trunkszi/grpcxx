#pragma once

#include <array>
#include <chrono>
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
#include <string>
#include <thread>
#include <uarch/uarch.hpp>
#include <vector>

#include "grpcxx/compression/compression_registry.h"
#include "grpcxx/framing/aligned_message_buf.h"
#include "grpcxx/framing/length_prefixed_frame.h"
#include "grpcxx/sync/sync_wire_transport.h"
#include "grpcxx/sync/synchronous_channel.h"

namespace grpcxx {

enum class SyncStreamPhase : std::uint8_t {
  kOpen = 0,
  kHalfClosed = 1,
  kClosed = 2,
  kCancelled = 3,
};

struct SyncStreamMetaSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Field{0, 2}]] SyncStreamPhase phase{};
  [[= uarch::bits::Flag{2}]] bool received_headers{};
  [[= uarch::bits::Flag{3}]] bool received_status{};
  [[= uarch::bits::Flag{4}]] bool request_ended{};
  [[= uarch::bits::Flag{5}]] bool end_stream_submitted{};
  [[= uarch::bits::Flag{6}]] bool peer_closed{};
  [[= uarch::bits::Flag{7}]] bool terminal{};
  [[= uarch::bits::Flag{8}]] bool local_failure{};
  [[= uarch::bits::Flag{9}]] bool rst_sent{};
};

inline constexpr auto kSyncStreamWritablePattern =
    uarch::BitPattern<SyncStreamMetaSpec>{}
        .field_eq<^^SyncStreamMetaSpec::phase>(SyncStreamPhase::kOpen)
        .flag_clear<^^SyncStreamMetaSpec::terminal>();

inline constexpr auto kSyncStreamTerminalClosedPatch =
    uarch::BitPatch<SyncStreamMetaSpec>{}
        .set_field<^^SyncStreamMetaSpec::phase>(SyncStreamPhase::kClosed)
        .set_flags<^^SyncStreamMetaSpec::peer_closed, ^^SyncStreamMetaSpec::terminal>();

inline constexpr auto kSyncStreamPeerResetPatch =
    uarch::BitPatch<SyncStreamMetaSpec>{}
        .set_field<^^SyncStreamMetaSpec::phase>(SyncStreamPhase::kClosed)
        .set_flags<^^SyncStreamMetaSpec::peer_closed, ^^SyncStreamMetaSpec::terminal,
                   ^^SyncStreamMetaSpec::local_failure>();

inline constexpr auto kSyncStreamLocalAbortPatch =
    uarch::BitPatch<SyncStreamMetaSpec>{}
        .set_field<^^SyncStreamMetaSpec::phase>(SyncStreamPhase::kCancelled)
        .set_flags<^^SyncStreamMetaSpec::local_failure, ^^SyncStreamMetaSpec::terminal>();

struct SyncChannelRuntimeSpec {
  using Word = std::uint32_t;
  [[= uarch::bits::Flag{0}]] bool polling{};
  [[= uarch::bits::Flag{1}]] bool secure{};
  [[= uarch::bits::Flag{2}]] bool draining{};
  [[= uarch::bits::Flag{3}]] bool closed{};
};

struct RpcStream::State final {
  explicit State(std::size_t max_message_bytes) noexcept : parser(max_message_bytes) {}

  std::uint32_t slot_index{0U};
  std::uint32_t id{0U};
  RpcKind kind{RpcKind::kUnary};
  std::string path{};
  CallOptions options{};
  std::chrono::steady_clock::time_point deadline{};
  uarch::BitWord<SyncStreamMetaSpec> meta{};
  MetadataMap initial{};
  MetadataMap trailers{};
  Status status{Status::OkStatus()};
  CompressionAlgorithm incoming_encoding{CompressionAlgorithm::kIdentity};
  LengthPrefixedFrame parser;
  std::deque<AlignedMessageBuf> outgoing{};
  std::deque<std::vector<std::byte>> incoming{};
  std::vector<std::byte> last_payload{};
  std::size_t outgoing_offset{0uz};
  std::size_t outgoing_bytes{0uz};
  std::size_t incoming_bytes{0uz};
  std::size_t sent_messages{0uz};
  std::size_t received_messages{0uz};
  std::uint32_t unreplenished_recv_bytes{0U};

  [[nodiscard, gnu::always_inline]] constexpr bool IsWritable() const noexcept {
    return kSyncStreamWritablePattern.matches(meta);
  }
  [[nodiscard, gnu::always_inline]] constexpr bool ReceivedHeaders() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::received_headers>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool ReceivedStatus() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::received_status>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool RequestEnded() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::request_ended>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool EndStreamSubmitted() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::end_stream_submitted>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsPeerClosed() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::peer_closed>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsTerminal() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::terminal>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsLocalFailure() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::local_failure>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool RstSent() const noexcept {
    return meta.test<^^SyncStreamMetaSpec::rst_sent>();
  }
};

struct SynchronousChannel::Impl final {
  static constexpr std::size_t kMaxSlots = 64uz;
  static constexpr std::size_t kIoBufferBytes = 262144uz;
  static constexpr std::uint32_t kWindowReplenishThreshold = 32768U;

  Impl(sync_detail::SyncWireTransport wire, std::string_view authority_text,
       SynchronousChannelOptions opts) noexcept;
  ~Impl() noexcept;

  [[nodiscard]] std::size_t OutputFreeBytes() const noexcept {
    const auto used = h2 ? h2->PendingOutput().size() : kIoBufferBytes;
    return used < kIoBufferBytes ? (kIoBufferBytes - used) : 0uz;
  }

  [[nodiscard]] std::expected<void, Status> Initialize() noexcept;
  [[nodiscard]] std::expected<void, Status> CheckOwner() const noexcept;
  [[nodiscard]] RpcStream::State* Find(std::uint32_t stream_id) noexcept;
  [[nodiscard]] std::expected<bool, Status> PumpOutgoing() noexcept;
  [[nodiscard]] std::expected<bool, Status> FlushWire() noexcept;
  [[nodiscard]] std::expected<bool, Status> DrainEvents() noexcept;
  void HandleHeadersEvent(const nexora::Http2EventView& ev) noexcept;
  void HandleTrailersEvent(const nexora::Http2EventView& ev) noexcept;
  void HandleDataEvent(const nexora::Http2EventView& ev) noexcept;
  void FinalizeRemoteEndStream(RpcStream::State& stream) noexcept;
  void AbortStream(RpcStream::State& stream, Status reason, bool send_rst) noexcept;
  void FailAll(const Status& reason) noexcept;
  [[nodiscard]] bool OutboundCanProgress(bool caller_wants_write) const noexcept;
  [[nodiscard]] bool InboundFrameOpen() const noexcept;
  [[nodiscard]] std::expected<bool, Status> PollStep() noexcept;
  [[nodiscard]] std::expected<void, Status> PollUntilProgress(
      bool want_write, std::chrono::steady_clock::time_point deadline) noexcept;

  [[nodiscard, gnu::always_inline]] constexpr bool IsPolling() const noexcept {
    return runtime_meta.test<^^SyncChannelRuntimeSpec::polling>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsSecure() const noexcept {
    return runtime_meta.test<^^SyncChannelRuntimeSpec::secure>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsDraining() const noexcept {
    return runtime_meta.test<^^SyncChannelRuntimeSpec::draining>();
  }
  [[nodiscard, gnu::always_inline]] constexpr bool IsClosed() const noexcept {
    return runtime_meta.test<^^SyncChannelRuntimeSpec::closed>();
  }

  sync_detail::SyncWireTransport transport;
  std::string authority;
  SynchronousChannelOptions options;
  CompressionRegistry compression{256uz};
  std::unique_ptr<nexora::Http2Connection> h2{};
  std::array<RpcStream::State*, kMaxSlots> streams{};
  std::array<RpcStream*, kMaxSlots> handles{};
  std::array<std::byte, 16384> rx_scratch{};
  uarch::Bitmap64 active_streams_mask{};
  uarch::BitWord<SyncChannelRuntimeSpec> runtime_meta{};
  ProgressDelegate progress_hook{};
  Status failure{Status::OkStatus()};
  std::uint32_t conn_unreplenished_recv_bytes{0U};
  std::size_t window_updates_received{0uz};
  const std::thread::id owner{std::this_thread::get_id()};
};

}  // namespace grpcxx
