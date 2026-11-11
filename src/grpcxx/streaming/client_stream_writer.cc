#include "grpcxx/streaming/client_stream_writer.h"

#include <uarch/uarch.hpp>

namespace grpcxx {

auto RawClientStreamWriter::Send(std::span<const std::byte> payload) noexcept
    -> std::expected<WriteStatus, Status> {
  if (uarch::unlikely(handle_.generation == 0U)) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  return streams_->Send(handle_, payload);
}

auto RawClientStreamWriter::HalfClose() noexcept -> std::expected<void, Status> {
  if (uarch::unlikely(handle_.generation == 0U)) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  return streams_->HalfClose(handle_);
}

auto RawClientStreamWriter::Cancel() noexcept -> std::expected<void, Status> {
  if (uarch::unlikely(handle_.generation == 0U)) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  return streams_->Cancel(handle_);
}

auto RawClientStreamWriter::OnWriteReady(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  if (uarch::unlikely(handle_.generation == 0U)) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  return streams_->SetOnWriteReady(handle_, std::move(callback));
}

auto RawClientStreamWriter::OnCancel(InplaceDelegate<void() noexcept> callback) noexcept
    -> std::expected<void, Status> {
  if (uarch::unlikely(handle_.generation == 0U)) {
    return std::unexpected(Status{StatusCode::kFailedPrecondition});
  }
  return streams_->SetOnCancel(handle_, std::move(callback));
}

}  // namespace grpcxx
