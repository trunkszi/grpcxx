#include "grpcxx/streaming/client_stream_writer.h"

namespace grpcxx {

auto RawClientStreamWriter::Send(std::span<const std::byte> payload) noexcept
    -> std::expected<WriteStatus, Status> {
  return streams_->Send(handle_, payload);
}

auto RawClientStreamWriter::HalfClose() noexcept -> std::expected<void, Status> {
  return streams_->HalfClose(handle_);
}

auto RawClientStreamWriter::Cancel() noexcept -> std::expected<void, Status> {
  return streams_->Cancel(handle_);
}

auto RawClientStreamWriter::OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  return streams_->SetOnWriteReady(handle_, std::move(callback));
}

auto RawClientStreamWriter::OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  return streams_->SetOnCancel(handle_, std::move(callback));
}

}  // namespace grpcxx
