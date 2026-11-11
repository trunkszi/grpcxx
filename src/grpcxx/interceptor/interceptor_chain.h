#pragma once

#include <array>
#include <charconv>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <uarch/uarch.hpp>
#include <utility>

#include "grpcxx/interceptor/call_types.h"

namespace grpcxx {
namespace interceptor_detail {

[[nodiscard, gnu::cold, gnu::noinline]] inline Status EmptyMetadataKeyError() noexcept {
  uarch::cold_path();
  return Status{StatusCode::kInvalidArgument, "metadata key is empty"};
}

[[nodiscard, gnu::cold, gnu::noinline]] inline Status MetadataCapacityError(
    std::size_t capacity) noexcept {
  uarch::cold_path();
  constexpr std::string_view kPrefix{"metadata capacity "};
  constexpr std::string_view kSuffix{" exhausted"};
  std::array<char, 80> buffer{};
  std::memcpy(buffer.data(), kPrefix.data(), kPrefix.size());
  char* cursor = buffer.data() + kPrefix.size();
  const auto conv = std::to_chars(cursor, buffer.data() + buffer.size() - kSuffix.size(), capacity);
  if (conv.ec == std::errc{}) {
    cursor = conv.ptr;
  }
  std::memcpy(cursor, kSuffix.data(), kSuffix.size());
  cursor += kSuffix.size();
  return Status{StatusCode::kResourceExhausted,
                std::string(buffer.data(), static_cast<std::size_t>(cursor - buffer.data()))};
}

}  // namespace interceptor_detail

/** One metadata pair carried on a call. Views are not copied. */
struct MetadataEntry {
  std::string_view key{};
  std::string_view value{};
};
static_assert(uarch::NoPadding<MetadataEntry>);

/**
 * Decision returned by one interceptor.
 *
 * `kStop` skips every interceptor later in the chain.
 */
enum class InterceptorAction : std::uint8_t {
  kContinue = 0,
  kStop,
};

/**
 * Reports whether `action` ends the chain.
 *
 * The compile-time path avoids a switch so a consteval caller can fold the
 * predicate into a constant. The runtime path is an exhaustive switch.
 */
[[nodiscard]] constexpr bool StopsChain(InterceptorAction action) noexcept {
  if consteval {
    return action == InterceptorAction::kStop;
  }
  switch (action) {
    case InterceptorAction::kContinue:
      return false;
    case InterceptorAction::kStop:
      return true;
  }
  return true;
}

/**
 * Mutable per-call state shared by every interceptor in one chain run.
 *
 * Metadata storage is a fixed array of 16 entries. `AddMetadata` never
 * allocates; a full array returns `kResourceExhausted`.
 */
struct CallContext {
  static constexpr std::size_t kMetadataCapacity = 16;

  std::array<MetadataEntry, kMetadataCapacity> metadata{};
  std::uint8_t metadata_size{0};
  bool short_circuited{false};

  /**
   * Appends one metadata pair.
   *
   * `key` and `value` are stored as views. An empty key is `kInvalidArgument`.
   */
  [[nodiscard]] std::expected<void, Status> AddMetadata(std::string_view key,
                                                        std::string_view value) noexcept {
    if (uarch::unlikely(key.empty())) {
      return std::unexpected(interceptor_detail::EmptyMetadataKeyError());
    }
    if (uarch::unlikely(metadata_size >= kMetadataCapacity)) {
      return std::unexpected(interceptor_detail::MetadataCapacityError(kMetadataCapacity));
    }
    metadata[metadata_size] = MetadataEntry{key, value};
    ++metadata_size;
    return {};
  }

  /** Views the metadata appended so far, in insertion order. */
  [[nodiscard]] std::span<const MetadataEntry> Metadata() const noexcept {
    return std::span<const MetadataEntry>{metadata.data(), metadata_size};
  }
};

/** Interceptor that handles a client call through `OnClient`. */
template <typename Interceptor>
concept ClientInterceptor = requires(Interceptor& interceptor, CallContext& context) {
  { interceptor.OnClient(context) } noexcept -> std::same_as<InterceptorAction>;
};

/** Interceptor that handles a server call through `OnServer`. */
template <typename Interceptor>
concept ServerInterceptor = requires(Interceptor& interceptor, CallContext& context) {
  { interceptor.OnServer(context) } noexcept -> std::same_as<InterceptorAction>;
};

/** Interceptor invoked for either direction through `operator()`. */
template <typename Interceptor>
concept CallableInterceptor = requires(Interceptor& interceptor, CallContext& context) {
  { interceptor(context) } noexcept -> std::same_as<InterceptorAction>;
};

/**
 * Compile-time interceptor.
 *
 * At least one of `OnClient`, `OnServer`, or `operator()` must be noexcept.
 * The concrete object is stored by value inside `InterceptorChain`.
 */
template <typename Interceptor>
concept InterceptorConcept = (ClientInterceptor<Interceptor> || ServerInterceptor<Interceptor> ||
                              CallableInterceptor<Interceptor>) &&
                             std::is_nothrow_move_constructible_v<std::decay_t<Interceptor>>;

/**
 * Zero-allocation client and server interceptor pipeline.
 *
 * Interceptors are a template parameter pack stored inline. Nothing is
 * heap-allocated and nothing is type-erased. `RunClient` and `RunServer`
 * stop invoking later interceptors after the first `kStop`.
 */
template <typename... Interceptors>
  requires(InterceptorConcept<Interceptors> && ...)
class InterceptorChain final {
 public:
  /** Stores `interceptors` by move. An empty pack is a chain that always continues. */
  constexpr explicit InterceptorChain(Interceptors... interceptors) noexcept
      : interceptors_{std::move(interceptors)...} {}

  /**
   * Runs the chain as a client call.
   *
   * Sets `context.short_circuited` when an interceptor returns `kStop`.
   * Metadata already on `context` is left in place.
   */
  [[nodiscard]] InterceptorAction RunClient(CallContext& context) noexcept {
    return Run<true>(context);
  }

  /**
   * Runs the chain as a server call.
   *
   * Direction-specific `OnServer` is preferred over `operator()`.
   */
  [[nodiscard]] InterceptorAction RunServer(CallContext& context) noexcept {
    return Run<false>(context);
  }

  /** Number of interceptors in this chain. */
  [[nodiscard]] static consteval std::size_t Size() noexcept { return sizeof...(Interceptors); }

 private:
  template <bool kClient, typename Interceptor>
  [[nodiscard]] static InterceptorAction Dispatch(Interceptor& interceptor,
                                                  CallContext& context) noexcept {
    if constexpr (kClient && ClientInterceptor<Interceptor>) {
      return interceptor.OnClient(context);
    } else if constexpr (!kClient && ServerInterceptor<Interceptor>) {
      return interceptor.OnServer(context);
    } else if constexpr (CallableInterceptor<Interceptor>) {
      return interceptor(context);
    } else {
      return InterceptorAction::kContinue;
    }
  }

  template <bool kClient>
  [[nodiscard]] InterceptorAction Run(CallContext& context) noexcept {
    context.short_circuited = false;
    InterceptorAction action = InterceptorAction::kContinue;
    static constexpr auto kIndices = [] {
      std::array<std::size_t, sizeof...(Interceptors)> indices{};
      for (std::size_t index = 0; index < indices.size(); ++index) {
        indices[index] = index;
      }
      return indices;
    }();
    template for (constexpr auto Index : kIndices) {
      using Interceptor = Interceptors...[Index];
      action =
          Dispatch<kClient>(static_cast<Interceptor&>(std::get<Index>(interceptors_)), context);
      if (StopsChain(action)) {
        break;
      }
    }
    if (StopsChain(action)) {
      context.short_circuited = true;
    }
    return action;
  }

  [[no_unique_address]] std::tuple<Interceptors...> interceptors_{};
};

}  // namespace grpcxx
