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
    struct Slot {
      ClientContext* context_{};
      Codec* codec_{};
      Completion<typename Descriptor::ResponseType> completion_;
      std::int32_t stream_id_{};
      bool used_{false};
    };
    std::array<Slot, Nghttp2ClientSession::kMaxStreams> slots_;
  };

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
    std::apply(
        [this](auto&... calls) {
          (
              [this](auto& entry) {
                for (auto& slot : entry.slots_) {
                  if (slot.used_) {
                    /** Cancel detaches the completion before attempting the native reset. */
                    const auto cancelled = session_.Cancel(slot.stream_id_);
                    static_cast<void>(cancelled);
                  }
                }
              }(calls),
              ...);
        },
        calls_);
  }

  template <std::size_t Index>
  [[nodiscard]] std::expected<std::int32_t, Status> CallUnary(
      ClientContext& context, const typename Method<Index>::RequestType& request,
      Completion<typename Method<Index>::ResponseType> completion) noexcept {
    using Descriptor = Method<Index>;
    static_assert(Descriptor::kKind == RpcKind::kUnary, "CallUnary requires a unary method");
    if (!completion) return std::unexpected(Status{StatusCode::kInvalidArgument});
    auto& slots = std::get<Index>(calls_).slots_;
    auto free = std::ranges::find_if(slots, [](const auto& slot) { return !slot.used_; });
    if (free == slots.end()) {
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    }
    return reflection_detail::EncodePayload<Codec>(
        codec_, request,
        [&](std::span<const std::byte> payload) noexcept -> std::expected<std::int32_t, Status> {
          free->used_ = true;
          free->context_ = &context;
          free->codec_ = codec_;
          free->completion_ = std::move(completion);
          constexpr auto& path = reflection_detail::kMethodPath<Contract, Descriptor>;
          auto submitted =
              session_.Submit(std::string_view{path.data(), path.size()}, payload, &*free,
                              &OnCompletion<Index>, reflection_detail::CodecContentType<Codec>());
          if (!submitted) {
            free->completion_ = {};
            free->used_ = false;
            return std::unexpected(std::move(submitted.error()));
          }
          free->stream_id_ = *submitted;
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
    slot.used_ = false;
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
