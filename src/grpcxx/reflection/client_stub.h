#pragma once

#include <grpcxx/core/inplace_delegate.h>
#include <grpcxx/h2/nghttp2_client_session.h>
#include <grpcxx/reflection/service_router.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <tuple>
#include <uarch/uarch.hpp>
#include <utility>

namespace grpcxx {

/** Typed, bounded unary calls over a caller-driven HTTP/2 session. The session outlives the stub.
 */
template <ServiceContract Contract, typename Codec>
class ClientStub final {
  using Methods = decltype(reflection_detail::Methods<Contract>());
  template <std::size_t Index>
  using Method = std::tuple_element_t<Index, Methods>;
  template <typename Resp>
  using Completion =
      InplaceDelegate<void(ClientContext&, std::expected<Response<Resp>, Status>) noexcept, 64>;

  template <typename Descriptor>
  struct Calls {
    static constexpr std::size_t kSlotLimit = Nghttp2ClientSession::kMaxStreams;
    static_assert(kSlotLimit <= 16);
    /** Set bits are free slots. `lowest_one` allocates one in a single scan. */
    static constexpr auto kFreeSlots =
        uarch::Bitmap64::from_raw((std::uint64_t{1} << kSlotLimit) - 1U);

    struct Slot {
      ClientContext* context_{};
      Codec* codec_{};
      Completion<typename Descriptor::ResponseType> completion_;
      uarch::Bitmap64* free_mask_{};
      std::uint32_t index_{};
      std::int32_t stream_id_{};
    };
    std::array<Slot, kSlotLimit> slots_{};
    uarch::Bitmap64 free_mask_{kFreeSlots};
  };

  static constexpr auto kMethodIndices = [] {
    constexpr auto kCount = std::tuple_size_v<Methods>;
    std::array<std::size_t, kCount> indices{};
    for (std::size_t index = 0; index < kCount; ++index) {
      indices[index] = index;
    }
    return indices;
  }();

  template <std::size_t... Index>
  static auto CallStorage(std::index_sequence<Index...>) -> std::tuple<Calls<Method<Index>>...>;

 public:
  explicit ClientStub(Nghttp2ClientSession& session) noexcept : session_(session) {}
  /** The supplied codec outlives the stub and every outstanding completion. */
  ClientStub(Nghttp2ClientSession& session, Codec& codec) noexcept
      : session_(session), codec_(&codec) {}
  ClientStub(const ClientStub&) = delete;
  ClientStub& operator=(const ClientStub&) = delete;
  ClientStub(ClientStub&&) = delete;
  ClientStub& operator=(ClientStub&&) = delete;

  ~ClientStub() noexcept {
    template for (constexpr auto Index : kMethodIndices) {
      using Descriptor = Method<Index>;
      auto& entry = std::get<Index>(calls_);
      const auto occupied =
          uarch::Bitmap64::from_raw(Calls<Descriptor>::kFreeSlots.raw() & ~entry.free_mask_.raw());
      for (const auto index : occupied | uarch::views::ones) {
        /** Cancel detaches the completion before attempting the native reset. */
        const auto cancelled = session_.Cancel(entry.slots_[index].stream_id_);
        static_cast<void>(cancelled);
      }
    }
  }

  template <std::size_t Index>
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(
      ClientContext& context, const typename Method<Index>::RequestType& request,
      Completion<typename Method<Index>::ResponseType> completion) noexcept {
    using Descriptor = Method<Index>;
    static_assert(Descriptor::kKind == RpcKind::kUnary, "CallUnary requires a unary method");
    if (!completion) return std::unexpected(Status{StatusCode::kInvalidArgument});
    auto& bucket = std::get<Index>(calls_);
    const auto free = bucket.free_mask_.lowest_one();
    if (!free.has_value() || *free >= Calls<Descriptor>::kSlotLimit) {
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    }
    return reflection_detail::EncodePayload<Codec>(
        codec_, request,
        [&](std::span<const std::byte> payload) noexcept -> std::expected<std::int32_t, Status> {
          const auto index = *free;
          auto& slot = bucket.slots_[index];
          slot.free_mask_ = &bucket.free_mask_;
          slot.index_ = index;
          bucket.free_mask_ = bucket.free_mask_.clear(index);
          slot.context_ = &context;
          slot.codec_ = codec_;
          slot.completion_ = std::move(completion);
          constexpr auto& path = reflection_detail::kMethodPath<Contract, Descriptor>;
          auto submitted =
              session_.Submit(std::string_view{path.data(), path.size()}, payload, &slot,
                              &OnCompletion<Index>, reflection_detail::CodecContentType<Codec>());
          if (!submitted) {
            slot.completion_ = {};
            bucket.free_mask_ = bucket.free_mask_.set(index);
            slot.free_mask_ = nullptr;
            return std::unexpected(std::move(submitted.error()));
          }
          slot.stream_id_ = *submitted;
          return *submitted;
        });
  }

 private:
  template <std::size_t Index>
  static void OnCompletion(void* opaque, std::int32_t, int code,
                           std::span<const std::byte> payload) noexcept {
    using Descriptor = Method<Index>;
    using Resp = typename Descriptor::ResponseType;
    auto& slot = *static_cast<typename Calls<Descriptor>::Slot*>(opaque);
    auto completion = std::move(slot.completion_);
    auto* context = slot.context_;
    auto* codec = slot.codec_;
    if (slot.free_mask_ != nullptr) {
      *slot.free_mask_ = slot.free_mask_->set(slot.index_);
      slot.free_mask_ = nullptr;
    }
    if (code != 0) {
      const auto status =
          code >= 1 && code <= 16 ? static_cast<StatusCode>(code) : StatusCode::kUnknown;
      completion(*context, std::unexpected(Status{status}));
      return;
    }
    auto response = reflection_detail::DecodePayload<Codec, Resp>(codec, payload);
    if (!response) {
      completion(*context, std::unexpected(std::move(response.error())));
      return;
    }
    completion(*context, Response<Resp>{std::move(*response), {}, {}});
  }

  Nghttp2ClientSession& session_;
  Codec* codec_{};
  decltype(CallStorage(std::make_index_sequence<std::tuple_size_v<Methods>>{})) calls_;
};

}  // namespace grpcxx
