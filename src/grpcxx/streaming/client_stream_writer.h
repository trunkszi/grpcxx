#pragma once

#include <cstddef>
#include <expected>
#include <span>
#include <utility>

#include "grpcxx/streaming/stream_multiplexer.h"

namespace grpcxx {

/** Generation-checked client request handle with explicit upload half-close. */
class RawClientStreamWriter {
 public:
  RawClientStreamWriter(StreamMultiplexer& streams, StreamHandle handle) noexcept
      : streams_(&streams), handle_(handle) {}

  [[nodiscard]] auto Send(std::span<const std::byte> payload) noexcept
      -> std::expected<WriteStatus, Status>;
  [[nodiscard]] auto HalfClose() noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto Cancel() noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status>;
  [[nodiscard]] auto OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status>;

 private:
  StreamMultiplexer* streams_;
  StreamHandle handle_;
};

/** Typed request adapter; its inline codec captures only caller-owned storage. */
template <typename Req>
class ClientStreamWriter {
 public:
  using Encode =
      InplaceDelegate<std::expected<std::span<const std::byte>, Status>(const Req&) noexcept>;

  /** Encoded spans are borrowed only during Send and copied into the bounded queue. */
  ClientStreamWriter(RawClientStreamWriter writer, Encode encode) noexcept
      : writer_(writer), encode_(std::move(encode)) {}

  [[nodiscard]] auto Send(const Req& message) noexcept -> std::expected<WriteStatus, Status> {
    if (!encode_) {
      return std::unexpected(
          Status{StatusCode::kFailedPrecondition, "stream encoder not configured"});
    }
    auto encoded = encode_(message);
    if (!encoded) {
      return std::unexpected(encoded.error());
    }
    return writer_.Send(*encoded);
  }

  [[nodiscard]] auto HalfClose() noexcept -> std::expected<void, Status> {
    return writer_.HalfClose();
  }

  [[nodiscard]] auto Cancel() noexcept -> std::expected<void, Status> { return writer_.Cancel(); }

  [[nodiscard]] auto OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status> {
    return writer_.OnWriteReady(std::move(callback));
  }

  [[nodiscard]] auto OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status> {
    return writer_.OnCancel(std::move(callback));
  }

 private:
  RawClientStreamWriter writer_;
  Encode encode_;
};

}  // namespace grpcxx
