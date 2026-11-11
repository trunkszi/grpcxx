#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "grpcxx/core/metadata.h"
#include "grpcxx/core/status.h"
#include "grpcxx/sync/call_options.h"

namespace grpcxx {

class SynchronousChannel;

/**
 * Caller-thread handle for one active client HTTP/2 gRPC stream.
 *
 * Destroying an unfinished `RpcStream` automatically submits `RST_STREAM(kCancel)` so stream
 * slots and flow-control credit are reclaimed immediately without waiting for server timeouts.
 */
class RpcStream final {
 public:
  struct State;

  RpcStream(const RpcStream&) =
      delete("RpcStream owns an active HTTP/2 stream; copying is disabled");
  RpcStream& operator=(const RpcStream&) =
      delete("RpcStream owns an active HTTP/2 stream; copying is disabled");
  RpcStream(RpcStream&& other) noexcept;
  RpcStream& operator=(RpcStream&& other) noexcept;
  ~RpcStream() noexcept;

  [[nodiscard]] std::expected<void, Status> Send(std::span<const std::byte> payload) noexcept;
  [[nodiscard]] std::expected<void, Status> HalfClose() noexcept;

  [[nodiscard]] std::expected<std::optional<std::vector<std::byte>>, Status> TryRecvNext() noexcept;
  [[nodiscard]] std::expected<std::optional<std::vector<std::byte>>, Status> RecvNext() noexcept;

  [[nodiscard]] std::expected<RpcResponse, Status> Finish() noexcept;
  [[nodiscard]] std::expected<void, Status> Cancel() noexcept;

  [[nodiscard]] std::uint32_t StreamId() const noexcept;
  [[nodiscard]] bool IsDone() const noexcept;

  [[nodiscard]] const MetadataMap& InitialMetadata() const noexcept;
  [[nodiscard]] const MetadataMap& TrailingMetadata() const noexcept;
  [[nodiscard]] const Status& FinalStatus() const noexcept;

  RpcStream(SynchronousChannel& channel, std::unique_ptr<State> state) noexcept;

 private:
  friend class SynchronousChannel;

  void DetachChannel() noexcept;
  void RebindHandle() noexcept;
  [[nodiscard]] std::expected<void, Status> RequireAttached() noexcept;
  void ReleaseOnTeardown() noexcept;

  SynchronousChannel* channel_{nullptr};
  std::unique_ptr<State> state_{};
};

}  // namespace grpcxx
