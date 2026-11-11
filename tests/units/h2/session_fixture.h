#pragma once
#include <fmt/format.h>
#include <grpcxx/h2/h2_client_session.h>
#include <grpcxx/h2/h2_server_session.h>
#include <grpcxx/streaming/stream_multiplexer.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <type_traits>
#include <uarch/uarch.hpp>
#include <vector>

namespace {
using grpcxx::H2ClientSession;
using grpcxx::H2ServerSession;
struct Result {
  int calls{};
  int status{-1};
  H2ClientSession* client{};
  int recursion_rejected{};
  std::vector<std::byte> payload;
  static void Complete(void* context, std::int32_t, int code,
                       std::span<const std::byte> bytes) noexcept {
    auto& self = *static_cast<Result*>(context);
    ++self.calls;
    self.status = code;
    self.payload.assign_range(bytes);
    if (self.client != nullptr) {
      auto send = self.client->Send();
      auto receive = self.client->Receive({});
      if (!send && send.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
      if (!receive && receive.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
    }
  }
};

/** Both directions use native HTTP/2 serialization; transport segmentation is controlled here. */
class MemoryPair {
 public:
  explicit MemoryPair(std::string_view content_type = "application/grpc") : server(content_type) {}
  H2ClientSession client;
  H2ServerSession server;
  std::size_t chunk{std::numeric_limits<std::size_t>::max()};
  void* mutation_context{};
  void (*mutate)(void*, bool, std::vector<std::byte>&){};
  bool Drive() {
    for (auto tick : std::views::iota(0, 256)) {
      static_cast<void>(tick);
      auto outbound = Transfer(client, server);
      if (!outbound) return false;
      auto inbound = Transfer(server, client);
      if (!inbound) return false;
      if (!*outbound && !*inbound) return true;
    }
    ADD_FAILURE() << "HTTP/2 session did not quiesce";
    return false;
  }

 private:
  template <typename From, typename To>
  std::optional<bool> Transfer(From& from, To& to) {
    auto output = from.Send();
    if (!output) {
      ADD_FAILURE() << fmt::format("native serialization failed: {}",
                                   static_cast<int>(output.error().Code()));
      return std::nullopt;
    }
    if (output->empty()) return false;
    std::vector<std::byte> stable(std::from_range, *output);
    if (mutate != nullptr) {
      std::vector<std::byte> rewritten;
      std::size_t offset = 0;
      if (stable.size() >= 24 && stable[0] == std::byte{'P'}) {
        rewritten.append_range(std::span(stable).first(24));
        offset = 24;
      }
      while (offset < stable.size()) {
        if (stable.size() - offset < 9) return std::nullopt;
        auto frame = std::span(stable).subspan(offset);
        const auto length = (std::to_integer<std::size_t>(frame[0]) << 16U) |
                            (std::to_integer<std::size_t>(frame[1]) << 8U) |
                            std::to_integer<std::size_t>(frame[2]);
        if (length + 9 > frame.size()) return std::nullopt;
        std::vector<std::byte> isolated(std::from_range, frame.first(length + 9));
        mutate(mutation_context, std::is_same_v<From, H2ClientSession>, isolated);
        rewritten.append_range(isolated);
        offset += length + 9;
      }
      stable = std::move(rewritten);
    }
    for (std::size_t offset = 0; offset < stable.size();) {
      const auto length = std::min(chunk, stable.size() - offset);
      auto input = std::span<const std::byte>(stable).subspan(offset, length);
      auto consumed = to.Receive(input);
      if (!consumed || *consumed != input.size()) {
        ADD_FAILURE() << "native input did not consume full fragment";
        return std::nullopt;
      }
      offset += length;
    }
    return true;
  }
};

struct ServerHandler {
  H2ServerSession* server{};
  int calls{};
  int failures{};
  bool deferred{};
  bool probe_recursion{};
  int recursion_rejected{};
  std::int32_t stream{};
  std::uint64_t generation{};
  std::vector<std::byte> pending{};
  static void Invoke(void* context, std::int32_t id, std::uint64_t gen, std::string_view path,
                     std::span<const std::byte> payload) noexcept {
    auto& self = *static_cast<ServerHandler*>(context);
    ++self.calls;
    self.stream = id;
    self.generation = gen;
    if (self.probe_recursion) {
      auto send = self.server->Send();
      auto receive = self.server->Receive({});
      if (!send && send.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
      if (!receive && receive.error().Code() == grpcxx::StatusCode::kFailedPrecondition)
        ++self.recursion_rejected;
    }
    if (self.deferred) {
      self.pending.assign_range(payload);
      return;
    }
    const int code = path == "/rpc.Test/Echo" ? 0 : 12;
    auto completed =
        self.server->Complete(id, gen, code == 0 ? payload : std::span<const std::byte>{}, code);
    if (!completed) ++self.failures;
  }
};

/** Extends one native DATA sequence past the application cap without invalidating HTTP/2 framing.
 */
struct ExcessData {
  std::int32_t stream{};
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<ExcessData*>(opaque);
    if (!from_client || self.changed || wire.size() < 9 || wire[3] != std::byte{0}) return;
    std::uint32_t id{};
    for (auto byte : std::span{wire}.subspan(5, 4))
      id = (id << 8) | std::to_integer<std::uint32_t>(byte);
    if (id != static_cast<std::uint32_t>(self.stream) || (wire[4] & std::byte{1}) == std::byte{0})
      return;
    wire[4] &= std::byte{0xfe};
    const std::array extra{std::byte{0}, std::byte{0}, std::byte{1}, std::byte{0}, std::byte{1},
                           wire[5],      wire[6],      wire[7],      wire[8],      std::byte{0x55}};
    wire.append_range(extra);
    self.changed = true;
  }
};

/** Adds grpc-status to nonterminal initial headers without changing the HPACK dynamic table. */
struct PrematureStatus {
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<PrematureStatus*>(opaque);
    if (from_client || self.changed || wire.size() < 9 || wire[3] != std::byte{1} ||
        (wire[4] & std::byte{1}) != std::byte{0})
      return;
    const std::string_view name = "grpc-status";
    wire.push_back(std::byte{0});
    wire.push_back(std::byte{11});
    for (const auto character : name) wire.push_back(static_cast<std::byte>(character));
    wire.push_back(std::byte{1});
    wire.push_back(std::byte{'0'});
    const auto length = wire.size() - 9;
    wire[0] = std::byte{static_cast<unsigned char>((length >> 16) & 0xff)};
    wire[1] = std::byte{static_cast<unsigned char>((length >> 8) & 0xff)};
    wire[2] = std::byte{static_cast<unsigned char>(length & 0xff)};
    self.changed = true;
  }
};

struct HttpStatusMutation {
  int status{};
  bool omit_grpc_status{true};
  bool changed{};
  static void Mutate(void* opaque, bool from_client, std::vector<std::byte>& wire) {
    auto& self = *static_cast<HttpStatusMutation*>(opaque);
    if (from_client || wire.size() < 9) return;
    if (wire[3] == std::byte{1} && !self.changed) {
      if (wire.size() < 10 || wire[9] != std::byte{0x88}) return;
      const std::array literal{std::byte{8}, std::byte{3},
                               static_cast<std::byte>('0' + self.status / 100),
                               static_cast<std::byte>('0' + (self.status / 10) % 10),
                               static_cast<std::byte>('0' + self.status % 10)};
      std::vector<std::byte> replacement;
      replacement.append_range(std::span{wire}.first(9));
      replacement.append_range(literal);
      replacement.append_range(std::span{wire}.subspan(10));
      wire.swap(replacement);
      const auto length = wire.size() - 9;
      wire[0] = std::byte{static_cast<unsigned char>((length >> 16) & 0xff)};
      wire[1] = std::byte{static_cast<unsigned char>((length >> 8) & 0xff)};
      wire[2] = std::byte{static_cast<unsigned char>(length & 0xff)};
      self.changed = true;
    } else if (self.omit_grpc_status && wire[3] == std::byte{1}) {
      wire.clear();
    }
    if (self.omit_grpc_status && !wire.empty() && wire[3] == std::byte{0}) wire[4] |= std::byte{1};
  }
};

}  // namespace
