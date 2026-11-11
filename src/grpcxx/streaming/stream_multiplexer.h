#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <uarch/uarch.hpp>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/core/metadata.h"
#include "grpcxx/core/service.h"
#include "grpcxx/core/status.h"
#include "grpcxx/reflection/annotations.h"
#include "grpcxx/streaming/write_status.h"

namespace grpcxx {

/** A slot generation prevents a retained writer from addressing a replacement RPC. */
struct StreamHandle {
  std::uint32_t index{};
  std::uint32_t generation{};
  friend constexpr bool operator==(StreamHandle, StreamHandle) noexcept = default;
};
static_assert(uarch::NoPadding<StreamHandle>);
static_assert(uarch::RegisterPassable<StreamHandle>);

/** One GPR: slot index in the low half, generation in the high half. */
struct[[= uarch::layout::register_leaf]] StreamId;
struct StreamId {
  std::uint64_t value{};

  [[nodiscard]] static constexpr auto Pack(std::uint32_t index,
                                           std::uint32_t generation) noexcept -> StreamId {
    return StreamId{(static_cast<std::uint64_t>(generation) << 32U) | index};
  }
  [[nodiscard]] constexpr auto Index() const noexcept -> std::uint32_t {
    return static_cast<std::uint32_t>(value);
  }
  [[nodiscard]] constexpr auto Generation() const noexcept -> std::uint32_t {
    return static_cast<std::uint32_t>(value >> 32U);
  }
  [[nodiscard]] constexpr auto Handle() const noexcept -> StreamHandle {
    return StreamHandle{Index(), Generation()};
  }
};
static_assert(uarch::NoPadding<StreamId> && uarch::RegisterPassable<StreamId> &&
              uarch::RegisterLeaf<StreamId>);

/** Scheduling counters copied into two GPRs on the open path. */
struct StreamMultiplexerOptions {
  std::uint32_t max_streams{};
  std::uint32_t queue_depth{};
  std::size_t initial_window{};
};
static_assert(uarch::NoPadding<StreamMultiplexerOptions> &&
              uarch::RegisterPassable<StreamMultiplexerOptions> &&
              uarch::GprPair<StreamMultiplexerOptions>);

enum class StreamFrameKind : std::uint8_t { kHeaders, kMessage, kHalfClose, kTrailers, kReset };

/** Payload and metadata are borrowed only for the synchronous transport callback. */
struct StreamFrame {
  StreamHandle stream{};
  StreamFrameKind kind{};
  std::span<const std::byte> payload{};
  const MetadataMap* metadata{};
  Status status{};
  bool end_stream{};
  bool message_end{true};
};

/** Bounded endpoint-local state with preallocated payload queues.
 * Metadata copies follow MetadataMap's storage policy; the transport adapter
 * supplies validated HTTP/2 events and owns connection-level flow control.
 */
class StreamMultiplexer final {
 public:
  struct Options {
    std::uint32_t max_streams{64};
    std::uint32_t queue_depth{16};
    std::size_t max_message_bytes{65536};
    std::size_t high_water_bytes{131072};
    std::size_t low_water_bytes{65536};
    std::size_t initial_window{65535};
  };
  /** Success accepts the complete event; failure must leave the transport unchanged.
   * Message events may be fragments. The HTTP/2 adapter owns gRPC framing and
   * connection-window accounting in addition to this payload-window budget.
   */
  using WireHandler = InplaceDelegate<std::expected<void, Status>(const StreamFrame&) noexcept>;
  using ReadyHandler = InplaceDelegate<void() noexcept>;
  using HeadersHandler = InplaceDelegate<void(const MetadataMap&) noexcept>;
  using MessageHandler = InplaceDelegate<void(std::span<const std::byte>) noexcept>;
  using CloseHandler = InplaceDelegate<void(const Status&, const MetadataMap&) noexcept>;

  StreamMultiplexer() noexcept;
  explicit StreamMultiplexer(Options options) noexcept;
  ~StreamMultiplexer();
  StreamMultiplexer(const StreamMultiplexer&) =
      delete("StreamMultiplexer owns bounded stream arenas; copying is disabled");
  StreamMultiplexer& operator=(const StreamMultiplexer&) =
      delete("StreamMultiplexer owns bounded stream arenas; copying is disabled");
  StreamMultiplexer(StreamMultiplexer&&) =
      delete("StreamMultiplexer is referenced by stream handles; moving is disabled");
  StreamMultiplexer& operator=(StreamMultiplexer&&) =
      delete("StreamMultiplexer is referenced by stream handles; moving is disabled");

  [[nodiscard]] bool Ready() const noexcept;
  [[nodiscard]] std::expected<StreamHandle, Status> Open(RpcKind kind,
                                                         bool is_server = false) noexcept;
  [[nodiscard]] std::expected<void, Status> Release(StreamHandle stream) noexcept;
  [[nodiscard]] std::expected<StreamState, Status> State(StreamHandle stream) const noexcept;
  [[nodiscard]] std::expected<std::size_t, Status> QueuedBytes(StreamHandle stream) const noexcept;
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept;
  void SetWireHandler(WireHandler handler) noexcept;

  [[nodiscard]] std::expected<void, Status> SetOnWriteReady(StreamHandle, ReadyHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SetOnCancel(StreamHandle, ReadyHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SetOnHeaders(StreamHandle, HeadersHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SetOnMessage(StreamHandle, MessageHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SetOnHalfClose(StreamHandle, ReadyHandler) noexcept;
  [[nodiscard]] std::expected<void, Status> SetOnClose(StreamHandle, CloseHandler) noexcept;

  [[nodiscard]] std::expected<void, Status> SendHeaders(StreamHandle, const MetadataMap&) noexcept;
  /** HighWaterMarkReached means accepted; ResourceExhausted means no message was queued. */
  [[nodiscard]] std::expected<WriteStatus, Status> Send(StreamHandle,
                                                        std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> HalfClose(StreamHandle) noexcept;
  [[nodiscard]] std::expected<void, Status> Finish(StreamHandle, const Status&,
                                                   const MetadataMap& trailers = {}) noexcept;
  [[nodiscard]] std::expected<void, Status> Cancel(StreamHandle) noexcept;
  [[nodiscard]] std::expected<void, Status> UpdateWindow(StreamHandle,
                                                         std::size_t increment) noexcept;

  /** Feed consumes one already validated transport event without retaining borrowed spans. */
  [[nodiscard]] std::expected<void, Status> Feed(StreamHandle, const StreamFrame&) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveHeaders(StreamHandle,
                                                           const MetadataMap&) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveMessage(StreamHandle,
                                                           std::span<const std::byte>) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveHalfClose(StreamHandle) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveReset(StreamHandle) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveTrailers(StreamHandle, const Status&,
                                                            const MetadataMap&) noexcept;
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget,
                                                           MonotonicTimestamp now) noexcept;

 private:
  enum class Event : std::uint8_t { kOpen, kLocalEnd, kRemoteEnd, kReset };
  struct Entry;
  struct StreamSlotHot;
  struct StreamSlotCold;
  [[nodiscard]] std::optional<std::uint32_t> FindIndex(StreamHandle) noexcept;
  [[nodiscard]] std::optional<std::uint32_t> FindIndex(StreamHandle) const noexcept;
  [[nodiscard]] std::expected<void, Status> Transition(StreamSlotHot&, Event, bool peer) noexcept;
  [[nodiscard]] std::expected<void, Status> Queue(StreamSlotHot&, StreamSlotCold&, StreamFrameKind,
                                                  std::span<const std::byte>, const MetadataMap*,
                                                  const Status&) noexcept;
  [[nodiscard]] bool Limited(const StreamSlotHot&, bool outbound) const noexcept;
  [[nodiscard]] bool CanSend(const StreamSlotHot&) const noexcept;
  [[nodiscard]] bool CanReceive(const StreamSlotHot&) const noexcept;
  void NotifyReset(StreamSlotHot&, StreamSlotCold&) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveChunk(StreamHandle, std::span<const std::byte>,
                                                         bool message_end) noexcept;
  [[nodiscard]] std::optional<std::uint32_t> NextActiveFrom(
      std::uint32_t start_slot) const noexcept;
  Options options_;
  uarch::FastDivisorU32 queue_depth_div_{uarch::arithmetic::divisor_u32<1U>()};
  std::size_t bitmap_words_{0U};
  uarch::Bitmap64 free_l0_{};
  uarch::Bitmap64 active_l0_{};
  std::unique_ptr<uarch::Bitmap64[]> free_words_;
  std::unique_ptr<uarch::Bitmap64[]> active_words_;
  std::unique_ptr<StreamSlotHot[]> hot_;
  std::unique_ptr<StreamSlotCold[]> cold_;
  WireHandler wire_handler_;
  std::uint32_t next_slot_{};
  std::uint64_t rejected_transitions_{};
  bool polling_{};
  bool wire_active_{};
};

}  // namespace grpcxx
