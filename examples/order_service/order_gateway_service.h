#pragma once

#include <array>
#include <memory>
#include <optional>

#include "grpcxx/framing/length_prefixed_frame.h"
#include "grpcxx/reflection/unary_responder.h"
#include "order_contract.h"

namespace order_gateway {
class LoopbackTransport;

/** Fixed capacities bound accepted work and retained observations. */
struct LoopConfig {
  std::uint32_t max_receive_message_size{1024};
  std::uint32_t stream_window{2};
  std::uint32_t risk_check_ticks{1};
};

/** Caller-owned, single-thread loopback composition of canonical RPC implementations. */
class TradingLoop final {
 public:
  explicit TradingLoop(LoopConfig config = {}) noexcept;
  ~TradingLoop();
  TradingLoop(const TradingLoop&) = delete;
  TradingLoop& operator=(const TradingLoop&) = delete;
  [[nodiscard]] std::expected<void, grpcxx::Status> Start() noexcept;
  [[nodiscard]] std::expected<grpcxx::PollResult, grpcxx::Status> PollOnce(
      grpcxx::PollBudget budget, grpcxx::MonotonicTimestamp now) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> BeginShutdown(
      grpcxx::MonotonicTimestamp now) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> BeginShutdown() noexcept {
    return BeginShutdown(now_);
  }
  [[nodiscard]] std::expected<bool, grpcxx::Status> PollShutdown(
      grpcxx::PollBudget budget, grpcxx::MonotonicTimestamp now) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> Shutdown() noexcept;
  [[nodiscard]] grpcxx::ServiceState State() const noexcept { return state_; }
  [[nodiscard]] bool Stopped() const noexcept { return state_ == grpcxx::ServiceState::kStopped; }

  [[nodiscard]] std::expected<void, grpcxx::Status> NewOrderSingle(
      const order_gateway::NewOrderSingle& order, grpcxx::MonotonicTimestamp deadline) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> NewOrderWithTimeout(
      const order_gateway::NewOrderSingle& order, std::string_view timeout) noexcept;
  [[nodiscard]] std::expected<grpcxx::Response<CancelAck>, grpcxx::Status> CancelOrder(
      const order_gateway::CancelOrder& request) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> SubscribeMarketData(
      std::uint32_t count) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> OpenOrderStream() noexcept;
  [[nodiscard]] std::expected<grpcxx::WriteStatus, grpcxx::Status> SendOrderEntry(
      const OrderEntryRequest& request) noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> HalfCloseOrderStream() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> CancelOrderStream() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> DrainStreamWindow() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> PushRawFrame(
      std::span<const std::byte> wire) noexcept;

  /** Concrete handlers invoked by ServiceRouter, including its retained responder capability. */
  void NewOrderSingle(grpcxx::ServerContext&, const order_gateway::NewOrderSingle&,
                      grpcxx::UnaryResponder<ExecutionReport>) noexcept;
  [[nodiscard]] std::expected<grpcxx::Response<CancelAck>, grpcxx::Status> CancelOrder(
      grpcxx::ServerContext&, const order_gateway::CancelOrder&) noexcept;

  [[nodiscard]] std::span<const ExecutionReport> Reports() const noexcept {
    return {reports_.data(), report_count_};
  }
  [[nodiscard]] std::span<const MarketDataTick> Ticks() const noexcept {
    return {ticks_.data(), tick_count_};
  }
  [[nodiscard]] std::span<const CancelAck> Cancels() const noexcept {
    return {cancels_.data(), cancel_count_};
  }
  [[nodiscard]] std::span<const grpcxx::StatusCode> Failures() const noexcept {
    return {failures_.data(), failure_count_};
  }
  [[nodiscard]] std::uint64_t RejectedTransitions() const noexcept { return rejected_transitions_; }
  [[nodiscard]] bool HighWaterSeen() const noexcept { return high_water_seen_; }
  [[nodiscard]] bool WriteReadySeen() const noexcept { return write_ready_seen_; }
  [[nodiscard]] bool DeadlineExceededSeen() const noexcept;
  [[nodiscard]] bool ResourceExhaustedSeen() const noexcept { return resource_exhausted_seen_; }
  [[nodiscard]] bool ShutdownDrainedResponder() const noexcept {
    return shutdown_drained_responder_;
  }
  [[nodiscard]] std::size_t PendingCount() const noexcept;
  [[nodiscard]] grpcxx::MonotonicTimestamp Now() const noexcept { return now_; }
  [[nodiscard]] LoopConfig Config() const noexcept { return config_; }
  [[nodiscard]] bool Accepting() const noexcept { return state_ == grpcxx::ServiceState::kRunning; }

 private:
  friend class LoopbackTransport;
  struct PendingOrder {
    order_gateway::NewOrderSingle order{};
    std::uint32_t polls_left{};
    grpcxx::MonotonicTimestamp deadline{};
    std::optional<grpcxx::UnaryResponder<ExecutionReport>> responder{};
  };
  [[nodiscard]] std::expected<void, grpcxx::Status> CheckAdmission() noexcept;
  [[nodiscard]] std::expected<void, grpcxx::Status> FinishOrder(PendingOrder&) noexcept;
  void RecordFailure(grpcxx::StatusCode code) noexcept;
  static ExecutionReport MakeReport(const order_gateway::NewOrderSingle&) noexcept;
  LoopConfig config_;
  grpcxx::ServiceState state_{grpcxx::ServiceState::kStopped};
  grpcxx::MonotonicTimestamp now_{};
  std::unique_ptr<LoopbackTransport> transport_;
  grpcxx::LengthPrefixedFrame frames_;
  std::array<PendingOrder, 8> pending_{};
  std::array<ExecutionReport, 64> reports_{};
  std::array<MarketDataTick, 64> ticks_{};
  std::array<CancelAck, 64> cancels_{};
  std::array<grpcxx::StatusCode, 64> failures_{};
  std::size_t report_count_{};
  std::size_t tick_count_{};
  std::size_t cancel_count_{};
  std::size_t failure_count_{};
  std::uint64_t rejected_transitions_{};
  bool high_water_seen_{};
  bool write_ready_seen_{};
  bool resource_exhausted_seen_{};
  bool shutdown_drained_responder_{};
};
static_assert(grpcxx::ServiceConcept<TradingLoop>);
}  // namespace order_gateway
