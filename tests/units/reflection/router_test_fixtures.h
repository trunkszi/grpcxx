#pragma once

#include <fmt/format.h>
#include <grpcxx/framing/aligned_message_buf.h>
#include <grpcxx/h2/h2_client_session.h>
#include <grpcxx/h2/h2_server_session.h>
#include <grpcxx/reflection/client_stub.h>
#include <grpcxx/reflection/service_router.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

namespace router_test {

struct Message {
  std::uint32_t value{};
};

struct MemoryCodec {
  template <typename T>
  static std::expected<T, grpcxx::Status> Decode(std::span<const std::byte> input) noexcept {
    if (input.size() != sizeof(T)) {
      return std::unexpected(
          grpcxx::Status{grpcxx::StatusCode::kInvalidArgument, "invalid message length"});
    }
    T value{};
    std::memcpy(&value, input.data(), sizeof(T));
    return value;
  }
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T& value,
                                                           std::span<std::byte> output) noexcept {
    if (output.size() < sizeof(T)) {
      return std::unexpected(
          grpcxx::Status{grpcxx::StatusCode::kResourceExhausted, "small output"});
    }
    std::memcpy(output.data(), &value, sizeof(T));
    return sizeof(T);
  }
};

struct FailingEncodeCodec : MemoryCodec {
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T&,
                                                           std::span<std::byte>) noexcept {
    return std::unexpected(
        grpcxx::Status{grpcxx::StatusCode::kResourceExhausted, "serializer rejected response"});
  }
};

struct OversizedEncodeCodec : MemoryCodec {
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T&,
                                                           std::span<std::byte> bytes) noexcept {
    return bytes.size() + 1;
  }
};

struct AlignedCodec : MemoryCodec {
  inline static int encodes{};
  template <typename T>
  static std::expected<std::size_t, grpcxx::Status> Encode(const T& message,
                                                           std::span<std::byte> bytes) noexcept {
    ++encodes;
    if (std::bit_cast<std::uintptr_t>(bytes.data()) % 8 != 0) {
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
    }
    return MemoryCodec::Encode(message, bytes);
  }
};

struct CanonicalCodec {
  static std::expected<void, grpcxx::Status> Encode(const Message& message,
                                                    grpcxx::AlignedMessageBuf& output) noexcept {
    auto frame = grpcxx::AlignedMessageBuf::Allocate(sizeof(Message));
    if (!frame) return std::unexpected(std::move(frame.error()));
    std::memcpy(frame->Payload().data(), &message, sizeof(Message));
    output = std::move(*frame);
    return {};
  }
  static std::expected<void, grpcxx::Status> Decode(std::span<const std::byte> bytes,
                                                    Message& message) noexcept {
    if (bytes.size() != sizeof(Message)) {
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
    }
    std::memcpy(&message, bytes.data(), sizeof(Message));
    return {};
  }
};

struct CanonicalEncodeFailure : CanonicalCodec {
  static std::expected<void, grpcxx::Status> Encode(const Message&,
                                                    grpcxx::AlignedMessageBuf&) noexcept {
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kResourceExhausted});
  }
};

struct CanonicalOversized : CanonicalCodec {
  static std::expected<void, grpcxx::Status> Encode(const Message&,
                                                    grpcxx::AlignedMessageBuf& output) noexcept {
    auto frame = grpcxx::AlignedMessageBuf::Allocate(grpcxx::H2ClientSession::kMaxMessageBytes + 1);
    if (!frame) return std::unexpected(std::move(frame.error()));
    output = std::move(*frame);
    return {};
  }
};

struct CanonicalDecodeFailure : CanonicalCodec {
  static std::expected<void, grpcxx::Status> Decode(std::span<const std::byte>, Message&) noexcept {
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kDataLoss});
  }
};

struct CanonicalCompressed : CanonicalCodec {
  static std::expected<void, grpcxx::Status> Encode(const Message& message,
                                                    grpcxx::AlignedMessageBuf& output) noexcept {
    auto result = CanonicalCodec::Encode(message, output);
    if (result) output.SetCompressed(true);
    return result;
  }
};

class ConfiguredCodec final {
 public:
  explicit ConfiguredCodec(std::uint32_t key) noexcept : key_(key) {}
  ConfiguredCodec(const ConfiguredCodec&) =
      delete("ConfiguredCodec tests non-copyable injected codec instances");
  ConfiguredCodec& operator=(const ConfiguredCodec&) =
      delete("ConfiguredCodec tests non-copyable injected codec instances");
  void SetKey(std::uint32_t key) noexcept { key_ = key; }
  std::expected<void, grpcxx::Status> Encode(const Message& message,
                                             grpcxx::AlignedMessageBuf& output) noexcept {
    ++encodes_;
    return CanonicalCodec::Encode(Message{message.value ^ key_}, output);
  }
  std::expected<void, grpcxx::Status> Decode(std::span<const std::byte> bytes,
                                             Message& message) noexcept {
    ++decodes_;
    auto result = CanonicalCodec::Decode(bytes, message);
    if (result) message.value ^= key_;
    return result;
  }
  int encodes_{};
  int decodes_{};

 private:
  std::uint32_t key_;
};

struct ExplicitContract {};

struct Impl {
  int calls{};
  std::optional<grpcxx::UnaryResponder<Message>> pending;
  std::expected<grpcxx::Response<Message>, grpcxx::Status> Echo(grpcxx::ServerContext&,
                                                                const Message& request) noexcept {
    ++calls;
    return grpcxx::Response<Message>{
        .message = Message{request.value + 1}, .initial_metadata = {}, .trailing_metadata = {}};
  }
  void Later(grpcxx::ServerContext&, const Message& request,
             grpcxx::UnaryResponder<Message> responder) noexcept {
    ++calls;
    expected_value = request.value + 2;
    pending.emplace(std::move(responder));
  }
  std::expected<grpcxx::Response<Message>, grpcxx::Status> Reject(grpcxx::ServerContext&,
                                                                  const Message&) noexcept {
    ++calls;
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kPermissionDenied, "denied"});
  }
  std::uint32_t expected_value{};
};

}  // namespace router_test

namespace grpcxx {
template <>
struct ContractMethods<router_test::ExplicitContract> {
  static constexpr std::string_view kServiceName = "rpc.Test";
  static consteval auto Methods() {
    using namespace router_test;
    return std::tuple{
        UnaryMethod<"Echo", Message, Message,
                    [](auto& impl, auto& context, const Message& request) noexcept {
                      return impl.Echo(context, request);
                    }>{},
        UnaryMethod<"Later", Message, Message,
                    [](auto& impl, auto& context, const Message& request, auto responder) noexcept {
                      impl.Later(context, request, std::move(responder));
                    },
                    true>{},
        UnaryMethod<"Reject", Message, Message,
                    [](auto& impl, auto& context, const Message& request) noexcept {
                      return impl.Reject(context, request);
                    }>{}};
  }
};
}  // namespace grpcxx

namespace router_test {

#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
struct[[= grpcxx::service{.name = "rpc.Reflected"}]] ReflectedContract;
struct ReflectedContract {
  [[= grpcxx::rpc{.name = "Echo"}]] Message Echo(Message);
  [[= grpcxx::rpc{.name = "Later"}]] Message Later(Message);
};
static_assert(grpcxx::ServiceContract<ReflectedContract>);
#endif
static_assert(grpcxx::ServiceContract<ExplicitContract>);
static_assert(uarch::NoPadding<grpcxx::UnaryResponderSink>);
using Router = grpcxx::ServiceRouter<ExplicitContract, Impl, MemoryCodec>;

struct Sink {
  bool current{true};
  int completions{};
  int failures{};
  grpcxx::StatusCode code{grpcxx::StatusCode::kOk};
  std::array<std::byte, 16> bytes{};
  std::size_t size{};
  static bool Current(void* context, std::uint64_t session, std::int32_t stream,
                      std::uint64_t generation) noexcept {
    return static_cast<Sink*>(context)->current && session == 1 && stream == 3 && generation == 7;
  }
  grpcxx::UnaryResponderSink Hooks() noexcept {
    return {.context = this,
            .is_current = &Current,
            .complete = [](void* context, std::uint64_t, std::int32_t, std::uint64_t,
                           std::span<const std::byte> input, grpcxx::MetadataMap&&,
                           grpcxx::MetadataMap&&) noexcept -> std::expected<void, grpcxx::Status> {
              auto& self = *static_cast<Sink*>(context);
              ++self.completions;
              std::ranges::copy(input, self.bytes.data());
              self.size = input.size();
              self.current = false;
              return {};
            },
            .fail = [](void* context, std::uint64_t, std::int32_t, std::uint64_t,
                       grpcxx::Status status) noexcept -> std::expected<void, grpcxx::Status> {
              auto& self = *static_cast<Sink*>(context);
              ++self.failures;
              self.code = status.Code();
              self.current = false;
              return {};
            }};
  }
};

inline std::array<std::byte, sizeof(Message)> Encode(Message message) {
  std::array<std::byte, sizeof(Message)> bytes{};
  auto encoded = MemoryCodec::Encode(message, bytes);
  EXPECT_TRUE(encoded);
  return bytes;
}

/** The bridge drives the typed router over HTTP/2 bytes, without socket substitutes. */
template <typename ResponseCodec = MemoryCodec>
struct HttpBridge {
  grpcxx::H2ServerSession server;
  grpcxx::H2ClientSession client;
  Impl implementation;
  grpcxx::ServiceRouter<ExplicitContract, Impl, ResponseCodec> router{implementation};
  grpcxx::ServerContext context;
  int dispatch_failures{};
  int completed{};
  int grpc_status{-1};
  Message response{};
  HttpBridge() { Attach(); }
  explicit HttpBridge(ResponseCodec& codec) : router(implementation, codec) { Attach(); }
  void Attach() {
    server.SetRequestHandler(
        this, [](void* opaque, std::int32_t stream, std::uint64_t gen, std::string_view path,
                 std::span<const std::byte> bytes) noexcept {
          auto& self = *static_cast<HttpBridge*>(opaque);
          auto dispatched =
              self.router.Dispatch(path, self.context, bytes, self.Hooks(), 1, stream, gen);
          if (!dispatched) ++self.dispatch_failures;
        });
  }
  grpcxx::UnaryResponderSink Hooks() noexcept {
    return {.context = this,
            .is_current =
                [](void* opaque, std::uint64_t session, std::int32_t stream,
                   std::uint64_t gen) noexcept {
                  return session == 1 &&
                         static_cast<HttpBridge*>(opaque)->server.IsCurrent(stream, gen);
                },
            .complete = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t gen,
                           std::span<const std::byte> bytes, grpcxx::MetadataMap&&,
                           grpcxx::MetadataMap&&) noexcept -> std::expected<void, grpcxx::Status> {
              auto sent = static_cast<HttpBridge*>(opaque)->server.Complete(stream, gen, bytes);
              if (!sent) {
                return std::unexpected(
                    grpcxx::Status{grpcxx::StatusCode::kInternal, "HTTP/2 completion failed"});
              }
              return {};
            },
            .fail = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t gen,
                       grpcxx::Status status) noexcept -> std::expected<void, grpcxx::Status> {
              auto sent = static_cast<HttpBridge*>(opaque)->server.Complete(
                  stream, gen, {}, static_cast<int>(std::to_underlying(status.Code())));
              if (!sent) {
                return std::unexpected(
                    grpcxx::Status{grpcxx::StatusCode::kInternal, "HTTP/2 trailers failed"});
              }
              return {};
            }};
  }
  void Submit(std::string_view path) {
    auto call = client.Submit(
        path, Encode(Message{17}), this,
        [](void* opaque, std::int32_t, int status, std::span<const std::byte> bytes) noexcept {
          auto& self = *static_cast<HttpBridge*>(opaque);
          ++self.completed;
          self.grpc_status = status;
          if (status == 0) {
            auto decoded = MemoryCodec::Decode<Message>(bytes);
            if (decoded) {
              self.response = *decoded;
            } else {
              ++self.dispatch_failures;
            }
          }
        });
    ASSERT_TRUE(call);
  }
  template <typename From, typename To>
  bool Transfer(From& from, To& to) {
    auto bytes = from.Send();
    EXPECT_TRUE(bytes);
    if (!bytes || bytes->empty()) return false;
    auto received = to.Receive(*bytes);
    EXPECT_TRUE(received);
    if (received) {
      EXPECT_EQ(*received, bytes->size());
    }
    return true;
  }
  void Drive() {
    for (auto tick : std::views::iota(0, 128)) {
      static_cast<void>(tick);
      const bool outgoing = Transfer(client, server);
      const bool incoming = Transfer(server, client);
      if (!outgoing && !incoming) return;
    }
    FAIL() << "HTTP/2 pump exceeded bounded ticks";
  }
};

}  // namespace router_test
