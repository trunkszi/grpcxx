#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

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
  StreamMultiplexer(const StreamMultiplexer&) = delete;
  StreamMultiplexer& operator=(const StreamMultiplexer&) = delete;
  StreamMultiplexer(StreamMultiplexer&&) = delete;
  StreamMultiplexer& operator=(StreamMultiplexer&&) = delete;

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
  struct Slot;
  [[nodiscard]] Slot* Find(StreamHandle) noexcept;
  [[nodiscard]] const Slot* Find(StreamHandle) const noexcept;
  [[nodiscard]] std::expected<void, Status> Transition(Slot&, Event, bool peer) noexcept;
  [[nodiscard]] std::expected<void, Status> Queue(Slot&, StreamFrameKind,
                                                  std::span<const std::byte>, const MetadataMap*,
                                                  const Status&) noexcept;
  [[nodiscard]] bool Limited(const Slot&, bool outbound) const noexcept;
  [[nodiscard]] bool CanSend(const Slot&) const noexcept;
  [[nodiscard]] bool CanReceive(const Slot&) const noexcept;
  void NotifyReset(Slot&) noexcept;
  [[nodiscard]] std::expected<void, Status> ReceiveChunk(StreamHandle, std::span<const std::byte>,
                                                         bool message_end) noexcept;
  Options options_;
  std::unique_ptr<Slot[]> slots_;
  WireHandler wire_handler_;
  std::uint32_t next_slot_{};
  std::uint64_t rejected_transitions_{};
  bool polling_{};
  bool wire_active_{};
};

}  // namespace grpcxx
