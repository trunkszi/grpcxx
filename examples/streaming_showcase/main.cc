/** @file Single-threaded four-cardinality streaming showcase.
 * The transport is an explicit in-memory event seam. No kernel sockets, HTTP/2
 * framing or reflection router are implied by this standalone demonstration.
 */
#include <fmt/format.h>

#include <array>
#include <cstddef>
#include <cstdio>
#include <span>
#include <string_view>
#include <utility>

#include "grpcxx/streaming/client_stream_observer.h"
#include "grpcxx/streaming/client_stream_writer.h"
#include "grpcxx/streaming/server_stream_responder.h"

namespace {
using namespace grpcxx;

/** A reusable one-byte encoding buffer, borrowed only until Send returns. */
auto EncodeInto(std::array<std::byte, 1>& storage) noexcept
    -> ClientStreamWriter<unsigned>::Encode {
  return [&storage](
             const unsigned& value) noexcept -> std::expected<std::span<const std::byte>, Status> {
    if (value > 255)
      return std::unexpected(Status{StatusCode::kInvalidArgument, "one-byte value overflow"});
    storage[0] = std::byte{static_cast<unsigned char>(value)};
    return std::span<const std::byte>{storage};
  };
}

/** Drives the same typed responder/writer API for each RPC cardinality. */
auto Showcase(RpcKind kind, std::string_view name) noexcept -> bool {
  const StreamMultiplexer::Options options{.max_streams = 1,
                                           .queue_depth = 16,
                                           .max_message_bytes = 8,
                                           .high_water_bytes = 8,
                                           .low_water_bytes = 2,
                                           .initial_window = 64};
  StreamMultiplexer client{options}, server{options};
  auto client_stream = client.Open(kind);
  auto server_stream = server.Open(kind, true);
  if (!client_stream || !server_stream) return false;
  client.SetWireHandler([&server, handle = *server_stream](const StreamFrame& frame) noexcept {
    return server.Feed(handle, frame);
  });
  server.SetWireHandler([&client, handle = *client_stream](const StreamFrame& frame) noexcept {
    return client.Feed(handle, frame);
  });
  std::array<std::byte, 1> request_bytes{}, response_bytes{};
  ClientStreamWriter<unsigned> upload{RawClientStreamWriter{client, *client_stream},
                                      EncodeInto(request_bytes)};
  ServerStreamResponder<unsigned> response{RawServerStreamResponder{server, *server_stream},
                                           EncodeInto(response_bytes)};
  unsigned total{};
  std::size_t responses{};
  bool done{}, valid = true;
  ClientStreamObserver<unsigned> observer;
  observer.OnHeaders = [](const MetadataMap&) noexcept {};
  observer.OnMessage = [&responses, name](const unsigned& value) noexcept {
    ++responses;
    fmt::print("{} response={}\n", name, value);
  };
  observer.OnClose = [&done, &valid](Status status, MetadataMap) noexcept {
    done = true;
    valid = valid && status.Ok();
  };
  if (!client.SetOnHeaders(*client_stream, std::move(observer.OnHeaders))) return false;
  if (!client.SetOnMessage(*client_stream,
                           [&observer, &valid](std::span<const std::byte> bytes) noexcept {
                             if (bytes.size() != 1) {
                               valid = false;
                               return;
                             }
                             observer.OnMessage(std::to_integer<unsigned>(bytes.front()));
                           }))
    return false;
  if (!client.SetOnClose(*client_stream,
                         [&observer](const Status& status, const MetadataMap& metadata) noexcept {
                           observer.OnClose(status, metadata);
                         }))
    return false;
  if (!server.SetOnMessage(*server_stream, [&response, &total, &valid,
                                            kind](std::span<const std::byte> bytes) noexcept {
        if (bytes.size() != 1) {
          valid = false;
          return;
        }
        const auto value = std::to_integer<unsigned>(bytes.front());
        total += value;
        if (kind == RpcKind::kServerStreaming) {
          for (const auto tick : {101U, 102U, 103U})
            valid = response.Send(tick).has_value() && valid;
        } else if (kind == RpcKind::kBidiStreaming) {
          valid = response.Send(value + 10).has_value() && valid;
        }
      }))
    return false;
  if (!server.SetOnHalfClose(*server_stream, [&response, &total, &valid, kind]() noexcept {
        if (kind == RpcKind::kUnary || kind == RpcKind::kClientStreaming)
          valid = response.Send(total).has_value() && valid;
        valid = response.Finish(Status{}).has_value() && valid;
      }))
    return false;
  if (!response.SendHeaders(MetadataMap{})) return false;
  const auto count = kind == RpcKind::kUnary || kind == RpcKind::kServerStreaming ? 1U : 3U;
  for (unsigned message = 1; message <= count; ++message) {
    const auto queued = upload.Send(message);
    if (!queued) return false;
    /** HighWaterMarkReached is accepted; the next producer tick must wait for readiness. */
    if (*queued == WriteStatus::kHighWaterMarkReached) return false;
  }
  if (!upload.HalfClose()) return false;
  PollBudget budget;
  budget.frames = 1;
  for (MonotonicTimestamp now = 0; now < 32 && !done; ++now) {
    if (!client.PollOnce(budget, now) || !server.PollOnce(budget, now)) return false;
  }
  fmt::print("{} complete={} messages={}\n", name, done, responses);
  return valid && done && responses != 0;
}
}  // namespace

int main() {
  fmt::print("Caller-driven streaming showcase: in-memory event transport (Seam 2)\n");
  constexpr std::array shapes{
      std::pair{RpcKind::kUnary, std::string_view{"Unary"}},
      std::pair{RpcKind::kServerStreaming, std::string_view{"MarketData"}},
      std::pair{RpcKind::kClientStreaming, std::string_view{"BatchOrders"}},
      std::pair{RpcKind::kBidiStreaming, std::string_view{"QuotesAndExecutions"}}};
  for (const auto& [kind, name] : shapes) {
    if (!Showcase(kind, name)) {
      fmt::print(stderr, "Streaming showcase failed: {}\n", name);
      return 1;
    }
  }
  return 0;
}
