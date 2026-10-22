#include "grpcxx/streaming/server_stream_responder.h"

namespace grpcxx {

auto RawServerStreamResponder::SendHeaders(const MetadataMap& metadata) noexcept
    -> std::expected<void, Status> {
  return streams_->SendHeaders(handle_, metadata);
}

auto RawServerStreamResponder::Send(std::span<const std::byte> payload) noexcept
    -> std::expected<WriteStatus, Status> {
  return streams_->Send(handle_, payload);
}

auto RawServerStreamResponder::Finish(const Status& status, const MetadataMap& trailers) noexcept
    -> std::expected<void, Status> {
  return streams_->Finish(handle_, status, trailers);
}

auto RawServerStreamResponder::Cancel() noexcept -> std::expected<void, Status> {
  return streams_->Cancel(handle_);
}

auto RawServerStreamResponder::OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  return streams_->SetOnWriteReady(handle_, std::move(callback));
}

auto RawServerStreamResponder::OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  return streams_->SetOnCancel(handle_, std::move(callback));
}

}  // namespace grpcxx
