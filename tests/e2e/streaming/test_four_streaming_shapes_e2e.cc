/** @file Seam 2 streaming integration through caller-driven in-memory transport events.
 * This verifies typed handles and the multiplexer, not io_uring, nghttp2 or reflection routing.
 */
#include <fmt/format.h>

#include <array>
#include <cstddef>
#include <cstdio>
#include <span>
#include <utility>

#include "grpcxx/streaming/client_stream_observer.h"
#include "grpcxx/streaming/client_stream_writer.h"
#include "grpcxx/streaming/server_stream_responder.h"

namespace {
using namespace grpcxx;

auto Check(bool success, const char* reason) noexcept -> bool {
  if (!success) fmt::print(stderr, "Streaming integration failed: {}\n", reason);
  return success;
}

auto Options() noexcept -> StreamMultiplexer::Options {
  return {.max_streams = 1,
          .queue_depth = 16,
          .max_message_bytes = 8,
          .high_water_bytes = 8,
          .low_water_bytes = 2,
          .initial_window = 64};
}

/** A one-byte domain codec uses caller-owned storage reused after every Send. */
auto Encoder(std::array<std::byte, 1>& storage) noexcept -> ClientStreamWriter<unsigned>::Encode {
  return [&storage](
             const unsigned& value) noexcept -> std::expected<std::span<const std::byte>, Status> {
    if (value > 255) return std::unexpected(Status{StatusCode::kInvalidArgument, "codec overflow"});
    storage[0] = std::byte{static_cast<unsigned char>(value)};
    return std::span<const std::byte>{storage};
  };
}

auto RunShape(RpcKind kind) noexcept -> bool {
  StreamMultiplexer client{Options()}, server{Options()};
  auto client_handle = client.Open(kind);
  auto server_handle = server.Open(kind, true);
  if (!Check(client_handle.has_value() && server_handle.has_value(), "stream slots")) return false;
  client.SetWireHandler([&server, handle = *server_handle](const StreamFrame& frame) noexcept {
    return server.Feed(handle, frame);
  });
  server.SetWireHandler([&client, handle = *client_handle](const StreamFrame& frame) noexcept {
    return client.Feed(handle, frame);
  });
  std::array<std::byte, 1> request_storage{}, response_storage{};
  ClientStreamWriter<unsigned> writer{RawClientStreamWriter{client, *client_handle},
                                      Encoder(request_storage)};
  ServerStreamResponder<unsigned> responder{RawServerStreamResponder{server, *server_handle},
                                            Encoder(response_storage)};
  std::size_t responses{}, headers{}, closes{}, requests{}, half_closes{};
  unsigned request_sum{}, response_sum{};
  bool callback_ok = true;
  ClientStreamObserver<unsigned> observer;
  observer.OnHeaders = [&headers, &callback_ok](const MetadataMap& metadata) noexcept {
    ++headers;
    callback_ok = callback_ok && metadata.Get("x-feed") == "ticks";
  };
  observer.OnMessage = [&responses, &response_sum](const unsigned& value) noexcept {
    ++responses;
    response_sum += value;
  };
  observer.OnClose = [&closes, &callback_ok](Status status, MetadataMap trailers) noexcept {
    ++closes;
    callback_ok = callback_ok && status.Ok() && trailers.Get("x-complete") == "yes";
  };
  if (!Check(client.SetOnHeaders(*client_handle, std::move(observer.OnHeaders)).has_value(),
             "header handler"))
    return false;
  if (!Check(
          client
              .SetOnMessage(*client_handle,
                            [&observer, &callback_ok](std::span<const std::byte> payload) noexcept {
                              if (payload.size() != 1) {
                                callback_ok = false;
                                return;
                              }
                              observer.OnMessage(std::to_integer<unsigned>(payload.front()));
                            })
              .has_value(),
          "message handler"))
    return false;
  if (!Check(
          client
              .SetOnClose(*client_handle,
                          [&observer](const Status& status, const MetadataMap& trailers) noexcept {
                            observer.OnClose(status, trailers);
                          })
              .has_value(),
          "close handler"))
    return false;
  if (!Check(server
                 .SetOnMessage(*server_handle,
                               [&responder, &requests, &request_sum, &callback_ok,
                                kind](std::span<const std::byte> payload) noexcept {
                                 if (payload.size() != 1) {
                                   callback_ok = false;
                                   return;
                                 }
                                 ++requests;
                                 const auto value = std::to_integer<unsigned>(payload.front());
                                 request_sum += value;
                                 if (kind == RpcKind::kBidiStreaming) {
                                   callback_ok =
                                       responder.Send(2 * value).has_value() && callback_ok;
                                 } else if (kind == RpcKind::kServerStreaming) {
                                   for (const auto tick : {10U, 11U, 12U})
                                     callback_ok = responder.Send(tick).has_value() && callback_ok;
                                 }
                               })
                 .has_value(),
             "server message handler"))
    return false;
  if (!Check(server
                 .SetOnHalfClose(
                     *server_handle,
                     [&responder, &half_closes, &request_sum, &callback_ok, kind]() noexcept {
                       ++half_closes;
                       if (kind == RpcKind::kUnary || kind == RpcKind::kClientStreaming) {
                         const auto value = kind == RpcKind::kUnary ? 2 * request_sum : request_sum;
                         callback_ok = responder.Send(value).has_value() && callback_ok;
                       }
                       MetadataMap trailers;
                       callback_ok = trailers.Add("x-complete", "yes").has_value() && callback_ok;
                       callback_ok =
                           responder.Finish(Status{}, trailers).has_value() && callback_ok;
                     })
                 .has_value(),
             "server half-close handler"))
    return false;
  MetadataMap initial_metadata;
  if (!Check(initial_metadata.Add("x-feed", "ticks").has_value(), "metadata construction"))
    return false;
  if (!Check(responder.SendHeaders(initial_metadata).has_value(), "response headers")) return false;
  initial_metadata.Clear();
  const auto request_count = kind == RpcKind::kUnary || kind == RpcKind::kServerStreaming ? 1U : 3U;
  for (unsigned value = 1; value <= request_count; ++value) {
    if (!Check(writer.Send(value).has_value(), "enqueue request")) return false;
  }
  if (!Check(writer.HalfClose().has_value(), "upload half-close")) return false;
  PollBudget budget;
  budget.frames = 1;
  for (MonotonicTimestamp now = 0; now < 32 && closes == 0; ++now) {
    auto sent = client.PollOnce(budget, now);
    auto received = server.PollOnce(budget, now);
    if (!Check(sent.has_value() && received.has_value(), "bounded caller poll")) return false;
    if (!Check(sent->frames <= 1 && received->frames <= 1, "frame budget")) return false;
  }
  const auto expected_count =
      kind == RpcKind::kServerStreaming || kind == RpcKind::kBidiStreaming ? 3U : 1U;
  const auto expected_sum = kind == RpcKind::kUnary             ? 2U
                            : kind == RpcKind::kServerStreaming ? 33U
                            : kind == RpcKind::kClientStreaming ? 6U
                                                                : 12U;
  return Check(callback_ok && headers == 1 && closes == 1 && half_closes == 1,
               "callbacks complete exactly once") &&
         Check(requests == request_count && responses == expected_count &&
                   response_sum == expected_sum,
               "all requests and responses preserve values") &&
         Check(*client.State(*client_handle) == StreamState::kClosed &&
                   *server.State(*server_handle) == StreamState::kClosed,
               "both directions closed");
}

auto CancelEitherEndpoint(bool client_cancels) noexcept -> bool {
  StreamMultiplexer client{Options()}, server{Options()};
  auto left = client.Open(RpcKind::kBidiStreaming);
  auto right = server.Open(RpcKind::kBidiStreaming, true);
  if (!left || !right) return false;
  client.SetWireHandler([&server, handle = *right](const StreamFrame& frame) noexcept {
    return server.Feed(handle, frame);
  });
  server.SetWireHandler([&client, handle = *left](const StreamFrame& frame) noexcept {
    return client.Feed(handle, frame);
  });
  std::size_t client_closes{}, server_cancels{};
  bool cancelled = false;
  if (!client.SetOnClose(
          *left, [&client_closes, &cancelled](const Status& status, const MetadataMap&) noexcept {
            ++client_closes;
            cancelled = status.Code() == StatusCode::kCancelled;
          }))
    return false;
  if (!server.SetOnCancel(*right, [&server_cancels]() noexcept { ++server_cancels; })) return false;
  auto& initiator = client_cancels ? client : server;
  auto handle = client_cancels ? *left : *right;
  if (!initiator.Cancel(handle)) return false;
  for (MonotonicTimestamp now = 0; now < 4; ++now) {
    if (!client.PollOnce(PollBudget{}, now) || !server.PollOnce(PollBudget{}, now)) return false;
  }
  return Check(client_closes == 1 && cancelled && server_cancels == 1,
               "symmetric cancellation callbacks");
}
}  // namespace

int main() {
  std::size_t passed{};
  for (const auto kind : {RpcKind::kUnary, RpcKind::kServerStreaming, RpcKind::kClientStreaming,
                          RpcKind::kBidiStreaming}) {
    if (!RunShape(kind)) return 1;
    ++passed;
  }
  if (!CancelEitherEndpoint(true) || !CancelEitherEndpoint(false)) return 1;
  fmt::print(
      "Streaming Seam 2: {} shapes and both cancel directions passed; in-memory events, no "
      "sockets/reflection\n",
      passed);
  return 0;
}
