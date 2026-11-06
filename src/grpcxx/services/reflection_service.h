#pragma once

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <ranges>
#include <span>
#include <string_view>
#include <utility>

#include "grpcxx/interceptor/call_types.h"
#include "grpcxx/reflection/annotations.h"

namespace grpcxx {

/**
 * One method of a C++26 service contract. `name` views static contract data.
 *
 * `kind` uses the canonical reflection `RpcKind`.
 */
struct ContractMethod {
  std::string_view name{};
  RpcKind kind{RpcKind::kUnary};
};

/**
 * Contract that can be listed by `ReflectionService`.
 *
 * `kServiceName` and `kMethods` must be static data. `Register` stores views
 * into that data and does not copy the strings.
 */
template <typename Contract>
concept ReflectionTableContract = requires {
  { std::string_view{Contract::kServiceName} } -> std::same_as<std::string_view>;
  requires std::ranges::contiguous_range<std::remove_cvref_t<decltype(Contract::kMethods)>>;
  requires std::ranges::sized_range<std::remove_cvref_t<decltype(Contract::kMethods)>>;
  requires std::same_as<
      std::ranges::range_value_t<std::remove_cvref_t<decltype(Contract::kMethods)>>,
      ContractMethod>;
};

/**
 * Non-blocking `grpc.reflection.v1alpha.ServerReflection` list surface.
 *
 * `ListServices` answers `list_services`. `ListMethods` answers a symbol
 * lookup for a service name or a `service/method` or `service.Method` symbol.
 * No protobuf descriptor is parsed.
 *
 * The service is single-threaded: the caller drives the same lifecycle as
 * `HealthService`.
 */
class ReflectionService final {
 public:
  static constexpr std::size_t kMaxServices = 32;
  static constexpr std::size_t kMaxMethodsPerService = 64;

  /** Constructs a service in `ServiceState::kStopped` with an empty registry. */
  ReflectionService() noexcept;
  ~ReflectionService() = default;

  ReflectionService(const ReflectionService&) = delete;
  ReflectionService& operator=(const ReflectionService&) = delete;
  ReflectionService(ReflectionService&&) = delete;
  ReflectionService& operator=(ReflectionService&&) = delete;

  /**
   * Enters `kRunning`.
   *
   * A start while running, starting, or stopping returns
   * `kFailedPrecondition`.
   */
  [[nodiscard]] std::expected<void, Status> Start() noexcept;

  /** Returns the lifecycle phase. */
  [[nodiscard]] ServiceState State() const noexcept;

  /**
   * Records one caller tick.
   *
   * Reflection has no sockets. The call succeeds only while running.
   */
  [[nodiscard]] std::expected<PollResult, Status> PollOnce(PollBudget budget,
                                                           MonotonicTimestamp now) noexcept;

  /**
   * Stops new registration and listing.
   *
   * The service enters `ServiceState::kStopping`.
   */
  [[nodiscard]] std::expected<void, Status> BeginShutdown(MonotonicTimestamp now) noexcept;

  /**
   * Reports whether shutdown has drained.
   *
   * Returns true once the registry is quiescent. `Stopped()` is then true.
   */
  [[nodiscard]] std::expected<bool, Status> PollShutdown(PollBudget budget,
                                                         MonotonicTimestamp now) noexcept;

  /** Reports whether shutdown has drained or the service was shut down. */
  [[nodiscard]] bool Stopped() const noexcept;

  /**
   * Drops registered contracts.
   *
   * Idempotent after the service has stopped. Also succeeds from `kStopped`.
   * A running service must drain first.
   */
  [[nodiscard]] std::expected<void, Status> Shutdown() noexcept;

  /**
   * Stores views of `Contract::kServiceName` and `Contract::kMethods`.
   *
   * A repeated name returns `kAlreadyExists`. The contract data must outlive
   * the registration; static contract members do.
   */
  template <typename Contract>
    requires ReflectionTableContract<Contract>
  [[nodiscard]] std::expected<void, Status> Register() noexcept;

  /**
   * Writes registered service names into `out`.
   *
   * Returns the number of names written. A short span returns
   * `kResourceExhausted` and writes nothing.
   */
  [[nodiscard]] std::expected<std::size_t, Status> ListServices(
      std::span<std::string_view> out) noexcept;

  /**
   * Resolves `symbol` to methods without protobuf.
   *
   * A registered service name lists every method. `service/method` and
   * `service.Method` list that one method. An unknown service returns
   * `kNotFound`.
   */
  [[nodiscard]] std::expected<std::size_t, Status> ListMethods(
      std::string_view symbol, std::span<ContractMethod> out) noexcept;

 private:
  struct Slot {
    std::string_view name{};
    const ContractMethod* methods{nullptr};
    std::uint16_t method_count{0};
    bool used{false};
  };

  [[nodiscard]] std::expected<void, Status> RequireRunning() const noexcept;
  [[nodiscard]] Slot* FindMutable(std::string_view name) noexcept;
  [[nodiscard]] const Slot* Find(std::string_view name) const noexcept;
  [[nodiscard]] std::expected<std::size_t, Status> WriteMethods(
      const Slot& slot, std::span<ContractMethod> out) const noexcept;
  void ClearRegistry() noexcept;

  std::array<Slot, kMaxServices> slots_{};
  std::uint16_t slot_count_{0};
  ServiceState state_{ServiceState::kStopped};
  bool quiescent_{false};
  MonotonicTimestamp last_poll_{0};
};

template <typename Contract>
  requires ReflectionTableContract<Contract>
std::expected<void, Status> ReflectionService::Register() noexcept {
  if (auto running = RequireRunning(); !running) {
    return std::unexpected(running.error());
  }
  constexpr std::string_view service_name{Contract::kServiceName};
  if (service_name.empty()) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "service name is empty"});
  }
  const auto& methods = Contract::kMethods;
  const auto method_count = std::ranges::size(methods);
  if (method_count > kMaxMethodsPerService) {
    return std::unexpected(
        Status{StatusCode::kResourceExhausted, "contract publishes too many methods"});
  }
  if (std::ranges::any_of(methods,
                          [](const ContractMethod& method) { return method.name.empty(); })) {
    return std::unexpected(Status{StatusCode::kInvalidArgument, "method name is empty"});
  }
  if (FindMutable(service_name) != nullptr) {
    return std::unexpected(
        Status{StatusCode::kAlreadyExists, "service contract is already registered"});
  }
  if (slot_count_ >= slots_.size()) {
    return std::unexpected(Status{StatusCode::kResourceExhausted, "reflection registry is full"});
  }
  Slot& slot = slots_[slot_count_];
  slot.name = service_name;
  slot.methods = std::ranges::data(methods);
  slot.method_count = static_cast<std::uint16_t>(method_count);
  slot.used = true;
  ++slot_count_;
  return {};
}

static_assert(ServiceConcept<ReflectionService>);

}  // namespace grpcxx
