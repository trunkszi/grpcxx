#include <benchmark/benchmark.h>
#include <fmt/format.h>
#include <grpcxx/reflection/service_router.h>

#include <array>
#include <cstring>

namespace dispatch_bench {
struct Message {
  std::uint64_t order_id{};
};
struct Codec {
  template <typename T>
  static std::expected<T, grpcxx::Status> Decode(std::span<const std::byte> bytes) noexcept {
    if (bytes.size() != sizeof(T))
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
    T message;
    std::memcpy(&message, bytes.data(), sizeof(T));
    return message;
  }
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T& message,
                                                           std::span<std::byte> bytes) noexcept {
    if (bytes.size() < sizeof(T))
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kResourceExhausted});
    std::memcpy(bytes.data(), &message, sizeof(T));
    return sizeof(T);
  }
};
struct Implementation {
  std::expected<grpcxx::Response<Message>, grpcxx::Status> Submit(grpcxx::ServerContext&,
                                                                  const Message& message) noexcept {
    return grpcxx::Response<Message>{
        .message = Message{message.order_id + 1}, .initial_metadata = {}, .trailing_metadata = {}};
  }
};
struct ExplicitContract {};
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
struct[[= grpcxx::service{.name = "order.Order"}]] ReflectedContract {
  [[= grpcxx::rpc{.name = "Submit"}]] Message Submit(Message);
};
#endif
}  // namespace dispatch_bench
namespace grpcxx {
template <>
struct ContractMethods<dispatch_bench::ExplicitContract> {
  static constexpr std::string_view kServiceName = "order.Order";
  static consteval auto Methods() {
    using dispatch_bench::Message;
    return std::tuple{
        UnaryMethod<"Submit", Message, Message,
                    [](auto& implementation, auto& context, const Message& message) noexcept {
                      return implementation.Submit(context, message);
                    }>{}};
  }
};
}  // namespace grpcxx
namespace dispatch_bench {
struct Completion {
  std::uint64_t sum{};
  std::uint64_t calls{};
  grpcxx::UnaryResponderSink Hooks() noexcept {
    return {.context = this,
            .is_current = [](void*, std::uint64_t, std::int32_t,
                             std::uint64_t) noexcept { return true; },
            .complete = [](void* opaque, std::uint64_t, std::int32_t, std::uint64_t,
                           std::span<const std::byte> bytes, grpcxx::MetadataMap&&,
                           grpcxx::MetadataMap&&) noexcept -> std::expected<void, grpcxx::Status> {
              auto& self = *static_cast<Completion*>(opaque);
              auto response = Codec::Decode<Message>(bytes);
              if (!response) return std::unexpected(response.error());
              self.sum += response->order_id;
              ++self.calls;
              return {};
            },
            .fail = [](void*, std::uint64_t, std::int32_t, std::uint64_t,
                       grpcxx::Status error) noexcept -> std::expected<void, grpcxx::Status> {
              return std::unexpected(std::move(error));
            }};
  }
};

template <typename Contract>
void Dispatch(benchmark::State& state) {
  Implementation implementation;
  grpcxx::ServiceRouter<Contract, Implementation, Codec> router(implementation);
  grpcxx::ServerContext context;
  Completion completion;
  std::array<std::byte, sizeof(Message)> input{};
  auto encoded = Codec::Encode(Message{47}, input);
  if (!encoded) {
    state.SkipWithError("message encoding failed");
    return;
  }
  const auto sink = completion.Hooks();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    benchmark::DoNotOptimize(input);
    auto result = router.Dispatch("/order.Order/Submit", context, input, sink, 1, 1, 1);
    benchmark::DoNotOptimize(result);
    if (!result) {
      const auto error = fmt::format("router failed: {}", static_cast<int>(result.error().Code()));
      state.SkipWithError(error.c_str());
      break;
    }
    benchmark::ClobberMemory();
  }
  benchmark::DoNotOptimize(completion.sum);
  state.SetItemsProcessed(static_cast<std::int64_t>(completion.calls));
}
BENCHMARK_TEMPLATE(Dispatch, ExplicitContract);
#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
BENCHMARK_TEMPLATE(Dispatch, ReflectedContract);
#endif
}  // namespace dispatch_bench
