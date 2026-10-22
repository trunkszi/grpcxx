#pragma once

#include "grpcxx/streaming/stream_multiplexer.h"

namespace grpcxx {

/** Inline callbacks run on the calling thread while its service is being polled. */
template <typename Resp>
struct ClientStreamObserver {
  InplaceDelegate<void(const MetadataMap&) noexcept> OnHeaders;
  /** The decoded message is borrowed for the duration of this callback only. */
  InplaceDelegate<void(const Resp&) noexcept> OnMessage;
  InplaceDelegate<void() noexcept> OnWriteReady;
  InplaceDelegate<void(Status, MetadataMap) noexcept> OnClose;
};

}  // namespace grpcxx
