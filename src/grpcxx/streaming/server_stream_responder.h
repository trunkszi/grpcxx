#pragma once

#include <cstddef>
#include <expected>
#include <span>
#include <utility>

#include "grpcxx/streaming/stream_multiplexer.h"

namespace grpcxx {

/** Generation-checked server response handle; all operations only enqueue work. */
class RawServerStreamResponder {
 public:
  RawServerStreamResponder(StreamMultiplexer& streams, StreamHandle handle) noexcept
      : streams_(&streams), handle_(handle) {}

  [[nodiscard]] auto SendHeaders(const MetadataMap& metadata) noexcept
      -> std::expected<void, Status>;
  /** Copies the payload into bounded stream storage before returning. */
  [[nodiscard]] auto Send(std::span<const std::byte> payload) noexcept
      -> std::expected<WriteStatus, Status>;
  [[nodiscard]] auto Finish(const Status& status, const MetadataMap& trailers = {}) noexcept
      -> std::expected<void, Status>;
  [[nodiscard]] auto Cancel() noexcept -> std::expected<void, Status>;
  [[nodiscard]] auto OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status>;
  [[nodiscard]] auto OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status>;

 private:
  StreamMultiplexer* streams_;
  StreamHandle handle_;
};

/** Typed response adapter over caller-owned, reusable codec storage. */
template <typename Resp>
class ServerStreamResponder {
 public:
  using Encode =
      InplaceDelegate<std::expected<std::span<const std::byte>, Status>(const Resp&) noexcept>;

  /** The codec output must remain valid until Send returns, including on failure. */
  ServerStreamResponder(RawServerStreamResponder responder, Encode encode) noexcept
      : responder_(responder), encode_(std::move(encode)) {}

  [[nodiscard]] auto SendHeaders(const MetadataMap& metadata) noexcept
      -> std::expected<void, Status> {
    return responder_.SendHeaders(metadata);
  }

  [[nodiscard]] auto Send(const Resp& message) noexcept -> std::expected<WriteStatus, Status> {
    if (!encode_) {
      return std::unexpected(
          Status{StatusCode::kFailedPrecondition, "stream encoder not configured"});
    }
    auto encoded = encode_(message);
    if (!encoded) {
      return std::unexpected(encoded.error());
    }
    return responder_.Send(*encoded);
  }

  [[nodiscard]] auto Finish(const Status& status, const MetadataMap& trailers = {}) noexcept
      -> std::expected<void, Status> {
    return responder_.Finish(status, trailers);
  }

  [[nodiscard]] auto Cancel() noexcept -> std::expected<void, Status> {
    return responder_.Cancel();
  }

  [[nodiscard]] auto OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status> {
    return responder_.OnWriteReady(std::move(callback));
  }

  [[nodiscard]] auto OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
      -> std::expected<void, Status> {
    return responder_.OnCancel(std::move(callback));
  }

 private:
  RawServerStreamResponder responder_;
  Encode encode_;
};

}  // namespace grpcxx
