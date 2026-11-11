#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <uarch/uarch.hpp>

#include "grpcxx/streaming/stream_multiplexer.h"

namespace grpcxx {
namespace streaming_detail {

[[nodiscard, gnu::cold, gnu::noinline]] inline Status Invalid() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kFailedPrecondition};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status Full() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kResourceExhausted};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status Cancelled() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kCancelled};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status OutOfRangeStatus() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kOutOfRange};
}

struct StreamSlotMetaSpec {
  using Word = std::uint64_t;
  [[= uarch::bits::Flag{0}]] bool used{};
  [[= uarch::bits::Flag{1}]] bool server{};
  [[= uarch::bits::Flag{2}]] bool sent_headers{};
  [[= uarch::bits::Flag{3}]] bool received_headers{};
  [[= uarch::bits::Flag{4}]] bool pressured{};
  [[= uarch::bits::Flag{5}]] bool reset_pending{};
  [[= uarch::bits::Flag{6}]] bool reset_notified{};
  [[= uarch::bits::Flag{7}]] bool close_notified{};
  [[= uarch::bits::Flag{8}]] bool inbound_in_progress{};
  [[= uarch::bits::Flag{9}]] bool end_stream_sent{};
  [[= uarch::bits::Flag{10}]] bool half_closed_remote{};
  [[= uarch::bits::Flag{11}]] bool trailers_queued{};
  [[= uarch::bits::Flag{12}]] bool callback_active{};
  [[= uarch::bits::Flag{13}]] bool deferred_recycle{};
  [[= uarch::bits::Field{16, 4}]] StreamState state{};
  [[= uarch::bits::Field{20, 4}]] RpcKind kind{};
  [[= uarch::bits::Field{24, 8}]] std::uint8_t callbacks_active{};
  [[= uarch::bits::Field{32, 32}]] std::uint32_t generation{};
};

using StreamSlotMetaWord = uarch::BitWord<StreamSlotMetaSpec>;

inline constexpr auto kReceiveHeadersReadyPattern =
    uarch::BitPattern<StreamSlotMetaSpec>{}
        .flag_set<^^StreamSlotMetaSpec::used>()
        .flag_clear<^^StreamSlotMetaSpec::received_headers>()
        .flag_clear<^^StreamSlotMetaSpec::inbound_in_progress>()
        .flag_clear<^^StreamSlotMetaSpec::callback_active>();

inline constexpr auto kCancelPatch = uarch::BitPatch<StreamSlotMetaSpec>{}
                                         .set_flag<^^StreamSlotMetaSpec::pressured>(false)
                                         .set_flag<^^StreamSlotMetaSpec::inbound_in_progress>(false)
                                         .set_flag<^^StreamSlotMetaSpec::reset_pending>(true);

}  // namespace streaming_detail

struct StreamMultiplexer::Entry {
  StreamFrameKind kind_{};
  MetadataMap metadata_;
  Status status_;
  std::size_t size_{};
  std::size_t offset_{};
};

struct alignas(64) StreamMultiplexer::StreamSlotHot final {
  streaming_detail::StreamSlotMetaWord meta_{};
  std::uint32_t head_{0U};
  std::uint32_t count_{0U};
  std::size_t queued_bytes_{0U};
  std::size_t window_{0U};
  std::size_t inbound_size_{0U};
  std::size_t sent_messages_{0U};
  std::size_t received_messages_{0U};
  std::uint64_t reserved_{0U};

  [[nodiscard, gnu::always_inline]] StreamState StateValue() const noexcept {
    return meta_.get<^^streaming_detail::StreamSlotMetaSpec::state>();
  }
  [[nodiscard, gnu::always_inline]] RpcKind KindValue() const noexcept {
    return meta_.get<^^streaming_detail::StreamSlotMetaSpec::kind>();
  }
  [[nodiscard, gnu::always_inline]] std::uint32_t GenerationValue() const noexcept {
    return meta_.get<^^streaming_detail::StreamSlotMetaSpec::generation>();
  }
  [[gnu::always_inline]] void EnterCallback() noexcept {
    const auto d = static_cast<std::uint8_t>(
        meta_.get<^^streaming_detail::StreamSlotMetaSpec::callbacks_active>() + 1U);
    meta_ = meta_.with<^^streaming_detail::StreamSlotMetaSpec::callbacks_active>(d)
                .with<^^streaming_detail::StreamSlotMetaSpec::callback_active>(true);
  }
  [[gnu::always_inline]] void LeaveCallback() noexcept {
    const auto d = static_cast<std::uint8_t>(
        meta_.get<^^streaming_detail::StreamSlotMetaSpec::callbacks_active>() - 1U);
    meta_ = meta_.with<^^streaming_detail::StreamSlotMetaSpec::callbacks_active>(d)
                .with<^^streaming_detail::StreamSlotMetaSpec::callback_active>(d != 0U);
  }
  static void VerifyLayout() noexcept {
    static_assert(uarch::CacheLineDense<StreamSlotHot> &&
                  uarch::assert_cache_line<StreamSlotHot>());
  }
};

struct StreamMultiplexer::StreamSlotCold final {
  std::unique_ptr<Entry[]> entries_;
  std::unique_ptr<std::byte[]> bytes_;
  std::unique_ptr<std::byte[]> inbound_;
  ReadyHandler on_ready_;
  ReadyHandler on_cancel_;
  HeadersHandler on_headers_;
  MessageHandler on_message_;
  ReadyHandler on_half_close_;
  CloseHandler on_close_;
};

}  // namespace grpcxx
