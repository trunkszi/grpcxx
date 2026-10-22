#pragma once

#include <grpcxx/core/metadata.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
#include <meta>
#endif

namespace grpcxx {

/** Per-call server metadata and caller-supplied monotonic deadline. */
class ServerContext final {
 public:
  MetadataMap metadata{};
  std::optional<std::uint64_t> deadline{};
  bool cancelled{false};
};

/** Client call metadata and caller-supplied monotonic deadline. */
class ClientContext final {
 public:
  MetadataMap metadata{};
  std::optional<std::uint64_t> deadline{};
  bool cancelled{false};
};

enum class RpcKind : std::uint8_t { kUnary, kServerStreaming, kClientStreaming, kBidiStreaming };

struct ServiceAnnotation {
  std::string_view name;
};
struct RpcAnnotation {
  std::string_view name;
  RpcKind kind{RpcKind::kUnary};
};
/** Structural annotation text; std::string_view itself is not a structural NTTP type. */
struct AnnotationText {
  std::array<char, 512> text_{};
  std::size_t length_{};
  template <std::size_t N>
  consteval AnnotationText(const char (&source)[N]) : length_(N - 1) {
    static_assert(N <= 512, "annotation name exceeds capacity");
    for (auto i : std::views::iota(std::size_t{0}, N)) text_[i] = source[i];
  }
  constexpr std::string_view View() const noexcept { return {text_.data(), length_}; }
};
struct ServiceMarker {
  AnnotationText name;
};
struct RpcMarker {
  AnnotationText name;
  RpcKind kind{RpcKind::kUnary};
};
using service = ServiceMarker;
using rpc = RpcMarker;

/** A typed message together with inbound application metadata. */
template <typename T>
class Request final {
 public:
  T message{};
  MetadataMap metadata{};
};

/** A typed response together with initial and terminal metadata. */
template <typename T>
class Response final {
 public:
  T message{};
  MetadataMap initial_metadata{};
  MetadataMap trailing_metadata{};
};

/** Structural string suitable for compile-time method names. */
template <std::size_t N>
struct MethodName {
  char text_[N]{};
  consteval MethodName(const char (&source)[N]) {
    for (auto i : std::views::iota(std::size_t{0}, N)) text_[i] = source[i];
  }
  constexpr std::string_view View() const noexcept { return {text_, N - 1}; }
};

/** Explicit method descriptor used when annotations are unavailable. */
template <MethodName Name, typename Req, typename Resp, auto InvokeHandler, bool IsDeferred = false>
struct UnaryMethod {
  using RequestType = Req;
  using ResponseType = Resp;
  static constexpr std::string_view kName = Name.View();
  static constexpr RpcKind kKind = RpcKind::kUnary;
  template <typename>
  static consteval bool Deferred() {
    return IsDeferred;
  }
  template <typename Impl, typename... Args>
  static decltype(auto) Invoke(Impl& impl, Args&&... args) noexcept(
      noexcept(InvokeHandler(impl, std::forward<Args>(args)...))) {
    static_assert(noexcept(InvokeHandler(impl, std::forward<Args>(args)...)),
                  "RPC handlers must be noexcept");
    return InvokeHandler(impl, std::forward<Args>(args)...);
  }
};

/** Specialize with kServiceName and a consteval Methods() tuple for explicit contracts. */
template <typename Contract>
struct ContractMethods;

namespace reflection_detail {
template <typename Contract>
concept ExplicitContract = requires { ContractMethods<Contract>::Methods(); } &&
    (requires {
      { ContractMethods<Contract>::kServiceName } -> std::convertible_to<std::string_view>;
    } || requires {
      { ContractMethods<Contract>::ServiceName } -> std::convertible_to<std::string_view>;
    });

#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
consteval auto Access() { return std::meta::access_context::unchecked(); }

template <typename Contract>
consteval std::string_view ReflectedServiceName() {
  template for (constexpr auto annotation :
                std::define_static_array(std::meta::annotations_of(^^Contract))) {
    if constexpr (std::meta::remove_cv(std::meta::type_of(annotation)) == ^^ServiceMarker) {
      constexpr auto value = std::meta::extract<ServiceMarker>(annotation);
      return std::string_view{std::define_static_array(value.name.text_).data(),
                              value.name.length_};
    }
  }
  return {};
}

template <std::meta::info Member>
consteval RpcAnnotation MethodAnnotation() {
  template for (constexpr auto annotation :
                std::define_static_array(std::meta::annotations_of(Member))) {
    if constexpr (std::meta::remove_cv(std::meta::type_of(annotation)) == ^^RpcMarker) {
      constexpr auto value = std::meta::extract<RpcMarker>(annotation);
      return RpcAnnotation{{std::define_static_array(value.name.text_).data(), value.name.length_},
                           value.kind};
    }
  }
  return {};
}

template <typename Impl, std::meta::info Member>
consteval std::meta::info ImplementationMember() {
  template for (constexpr auto candidate :
                std::define_static_array(std::meta::members_of(^^Impl, Access()))) {
    if constexpr (std::meta::is_function(candidate) && std::meta::has_identifier(candidate)) {
      if constexpr (std::meta::identifier_of(candidate) == std::meta::identifier_of(Member)) {
        return candidate;
      }
    }
  }
  return {};
}

template <std::meta::info Member>
struct ReflectedMethod {
  static constexpr auto kParameters = std::define_static_array(std::meta::parameters_of(Member));
  static_assert(kParameters.size() == 1, "contract methods require exactly one request parameter");
  using RequestType = [:std::meta::remove_cvref(std::meta::type_of(kParameters[0])):];
  using ResponseType = [:std::meta::return_type_of(Member):];
  static constexpr auto kAnnotation = MethodAnnotation<Member>();
  static constexpr auto kName = kAnnotation.name;
  static constexpr auto kKind = kAnnotation.kind;
  template <typename Impl>
  static consteval bool Deferred() {
    return std::meta::parameters_of(ImplementationMember<Impl, Member>()).size() == 3;
  }
  template <typename Impl, typename... Args>
  static decltype(auto) Invoke(Impl& impl, Args&&... args) noexcept {
    constexpr auto method = ImplementationMember<Impl, Member>();
    static_assert(method != std::meta::info{}, "implementation is missing a contract method");
    static_assert(noexcept((impl.[:method:])(std::forward<Args>(args)...)),
                  "RPC handlers must be noexcept");
    return (impl.[:method:])(std::forward<Args>(args)...);
  }
};

template <typename Contract>
consteval auto ReflectedMethods() {
  constexpr auto members = std::define_static_array(std::meta::members_of(^^Contract, Access()));
  /** The recursive tuple builder preserves heterogeneous request and response types. */
  return [=]<std::size_t Index>(this auto self, std::integral_constant<std::size_t, Index>) {
    if constexpr (Index == members.size()) {
      return std::tuple{};
    } else {
      constexpr auto member = members[Index];
      if constexpr (std::meta::is_function(member)) {
        if constexpr (!MethodAnnotation<member>().name.empty()) {
          return std::tuple_cat(std::tuple{ReflectedMethod<member>{}},
                                self(std::integral_constant<std::size_t, Index + 1>{}));
        } else {
          return self(std::integral_constant<std::size_t, Index + 1>{});
        }
      } else {
        return self(std::integral_constant<std::size_t, Index + 1>{});
      }
    }
  }(std::integral_constant<std::size_t, 0>{});
}
#endif

template <typename Contract>
consteval auto Methods() {
  if constexpr (ExplicitContract<Contract>) {
    return ContractMethods<Contract>::Methods();
  } else {
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
    return ReflectedMethods<Contract>();
#else
    static_assert(ExplicitContract<Contract>, "provide ContractMethods for this compiler");
#endif
  }
}

template <typename Contract>
consteval std::string_view ServiceName() {
  if consteval {
    if constexpr (ExplicitContract<Contract>) {
      if constexpr (requires { ContractMethods<Contract>::kServiceName; })
        return ContractMethods<Contract>::kServiceName;
      else
        return ContractMethods<Contract>::ServiceName;
    }
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
    else
      return ReflectedServiceName<Contract>();
#endif
  }
  return {};
}
}  // namespace reflection_detail

template <typename Contract>
concept ServiceContract = !reflection_detail::ServiceName<Contract>().empty();

}  // namespace grpcxx
