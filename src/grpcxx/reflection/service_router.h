#pragma once

#include <fmt/format.h>
#include <grpcxx/reflection/unary_responder.h>

#include <array>
#include <expected>
#include <ranges>
#include <span>
#include <string_view>
#include <tuple>
#include <utility>

namespace grpcxx {
namespace reflection_detail {

template <typename Contract, typename Method>
consteval auto MethodPath() {
  constexpr auto service_name = ServiceName<Contract>();
  constexpr auto method_name = Method::kName;
  static_assert(!service_name.empty() && !method_name.empty(), "RPC names must not be empty");
  std::array<char, service_name.size() + method_name.size() + 2> path{};
  path[0] = '/';
  for (auto i : std::views::iota(std::size_t{0}, service_name.size()))
    path[1 + i] = service_name[i];
  path[1 + service_name.size()] = '/';
  for (auto i : std::views::iota(std::size_t{0}, method_name.size())) {
    path[2 + service_name.size() + i] = method_name[i];
  }
  return path;
}

template <typename Contract, typename Method>
inline constexpr auto kMethodPath = MethodPath<Contract, Method>();

}  // namespace reflection_detail

/** Compile-time routes dispatch directly to concrete unary handler implementations. */
template <ServiceContract Contract, typename Impl, typename Codec>
class ServiceRouter final {
 public:
  /** Configure the associated H2 server session with this decoder's media type. */
  static constexpr auto kContentType = reflection_detail::CodecContentType<Codec>();
  explicit ServiceRouter(Impl& implementation) noexcept : implementation_(implementation) {}
  /** The supplied codec outlives this router and all responders retained by its handlers. */
  ServiceRouter(Impl& implementation, Codec& codec) noexcept
      : implementation_(implementation), codec_(&codec) {}

  [[nodiscard]] std::expected<void, Status> Dispatch(std::string_view path, ServerContext& context,
                                                     std::span<const std::byte> payload,
                                                     UnaryResponderSink sink,
                                                     std::uint64_t session_id,
                                                     std::int32_t stream_id,
                                                     std::uint64_t generation) noexcept {
    for (const auto& route : kRoutes) {
      if (route.path_ == path) {
        return route.invoke_(implementation_, codec_, context, payload, sink, session_id, stream_id,
                             generation);
      }
    }
    if (sink.is_current == nullptr || sink.fail == nullptr ||
        !sink.is_current(sink.context, session_id, stream_id, generation)) {
      return std::unexpected(Status{StatusCode::kFailedPrecondition});
    }
    return sink.fail(sink.context, session_id, stream_id, generation,
                     Status{StatusCode::kUnimplemented});
  }

  static constexpr std::size_t RouteCount() noexcept { return kRoutes.size(); }

 private:
  struct Route {
    std::string_view path_;
    std::expected<void, Status> (*invoke_)(Impl&, Codec*, ServerContext&,
                                           std::span<const std::byte>, UnaryResponderSink,
                                           std::uint64_t, std::int32_t, std::uint64_t) noexcept;
  };

  template <typename Method>
  static std::expected<void, Status> Invoke(Impl& impl, Codec* codec, ServerContext& context,
                                            std::span<const std::byte> payload,
                                            UnaryResponderSink sink, std::uint64_t session_id,
                                            std::int32_t stream_id,
                                            std::uint64_t generation) noexcept {
    using Req = typename Method::RequestType;
    using Resp = typename Method::ResponseType;
    auto responder =
        UnaryResponder<Resp>::template Bind<Codec>(sink, session_id, stream_id, generation, codec);
    if (!responder.Current()) {
      return std::unexpected(Status{StatusCode::kFailedPrecondition});
    }
    if constexpr (Method::kKind != RpcKind::kUnary) {
      return responder.Fail(Status{StatusCode::kUnimplemented});
    } else {
      auto request = reflection_detail::DecodePayload<Codec, Req>(codec, payload);
      if (!request) return responder.Fail(std::move(request.error()));
      if constexpr (Method::template Deferred<Impl>()) {
        static_assert(noexcept(Method::Invoke(impl, context, *request, std::move(responder))),
                      "RPC handlers must be noexcept");
        Method::Invoke(impl, context, *request, std::move(responder));
        return {};
      } else {
        static_assert(noexcept(Method::Invoke(impl, context, *request)),
                      "RPC handlers must be noexcept");
        auto response = Method::Invoke(impl, context, *request);
        if (!response) return responder.Fail(std::move(response.error()));
        auto completed = responder.Complete(std::move(*response));
        if (!completed && responder.Current()) {
          return responder.Fail(std::move(completed.error()));
        }
        return completed;
      }
    }
  }

  static consteval auto BuildRoutes() {
    return std::apply(
        [](auto... descriptors) {
          return std::array<Route, sizeof...(descriptors)>{
              Route{std::string_view{
                        reflection_detail::kMethodPath<Contract, decltype(descriptors)>.data(),
                        reflection_detail::kMethodPath<Contract, decltype(descriptors)>.size()},
                    &Invoke<decltype(descriptors)>}...};
        },
        reflection_detail::Methods<Contract>());
  }

  inline static constexpr auto kRoutes = BuildRoutes();
  static consteval bool UniqueRoutes() {
    for (auto left : std::views::iota(std::size_t{0}, kRoutes.size())) {
      for (auto right : std::views::iota(left + 1, kRoutes.size())) {
        if (kRoutes[left].path_ == kRoutes[right].path_) return false;
      }
    }
    return true;
  }
  static_assert(UniqueRoutes(), "RPC paths must be unique");
  Impl& implementation_;
  Codec* codec_{};
};

}  // namespace grpcxx
