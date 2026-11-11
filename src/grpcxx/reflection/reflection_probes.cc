#include <array>
#include <cstdint>
#include <string_view>
#include <tuple>

#include "grpcxx/core/inplace_delegate.h"
#include "grpcxx/reflection/service_router.h"

namespace {

struct ProbeRequest {
  std::uint64_t value;
};

struct ProbeResponse {
  std::uint64_t value;
};

struct[[= grpcxx::service{.name = "probe.v1.Route"}]] ProbeContract;
struct ProbeContract {
  [[= grpcxx::rpc{.name = "Ping"}]] ProbeResponse Ping(ProbeRequest);
};

struct RouteKey {
  std::uint64_t suffix;
  std::uint64_t length;
};

[[nodiscard]] constexpr std::uint64_t SuffixWord(std::string_view path) noexcept {
  std::uint64_t word = 0;
  const auto width = path.size() < 8U ? path.size() : 8U;
  const auto start = path.size() - width;
  for (std::size_t index = 0; index < width; ++index) {
    const auto byte = static_cast<unsigned char>(path[start + index]);
    word |= static_cast<std::uint64_t>(byte) << (index * 8U);
  }
  return word;
}

template <typename Contract, typename Method>
consteval RouteKey KeyFor() {
  constexpr auto& stored = grpcxx::reflection_detail::kMethodPath<Contract, Method>;
  constexpr std::string_view path{stored.data(), stored.size()};
  return RouteKey{SuffixWord(path), path.size()};
}

template <typename Contract>
consteval auto RouteTable() {
  constexpr auto methods = grpcxx::reflection_detail::Methods<Contract>();
  return std::apply(
      [](auto... descriptor) {
        return std::array<RouteKey, sizeof...(descriptor)>{
            KeyFor<Contract, decltype(descriptor)>()...};
      },
      methods);
}

static_assert(RouteTable<ProbeContract>().size() == 1);
static_assert(grpcxx::reflection_detail::ServiceName<ProbeContract>() == "probe.v1.Route");

struct IncrementTarget {
  [[nodiscard]] constexpr std::uint64_t operator()(std::uint64_t value) const noexcept {
    return value + 1U;
  }
};

}  // namespace

namespace grpcxx::probes_detail {

template <typename T>
union Immortal {
  T value;

  template <typename... Args>
  constexpr explicit Immortal(Args&&... args) noexcept : value(std::forward<Args>(args)...) {}
  constexpr ~Immortal() noexcept {}
};

// External linkage keeps the function-pointer load live while constinit places it in .data.
extern Immortal<grpcxx::InplaceDelegate<std::uint64_t(std::uint64_t) noexcept>> g_delegate;
constinit Immortal<grpcxx::InplaceDelegate<std::uint64_t(std::uint64_t) noexcept>> g_delegate{
    IncrementTarget{}};

}  // namespace grpcxx::probes_detail

extern "C" std::uint64_t grpcxx_probe_inplace_delegate_invoke(std::uint64_t value) noexcept {
  return grpcxx::probes_detail::g_delegate.value(value);
}

extern "C" std::uint64_t grpcxx_probe_reflection_route_match(std::uint64_t suffix,
                                                             std::uint64_t length) noexcept {
  constexpr auto table = RouteTable<ProbeContract>();
  std::uint64_t hits = 0;
  for (std::uint64_t index = 0; index < table.size(); ++index) {
    const auto key = table[index];
    const auto miss = (key.suffix ^ suffix) | (key.length ^ length);
    hits |= static_cast<std::uint64_t>(miss == 0U) << index;
  }
  return hits;
}
