#include <fmt/format.h>
#include <grpcxx/framing/aligned_message_buf.h>
#include <grpcxx/h2/nghttp2_client_session.h>
#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/reflection/client_stub.h>
#include <grpcxx/reflection/service_router.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <ranges>
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
    if (reinterpret_cast<std::uintptr_t>(bytes.data()) % 8 != 0) {
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
    if (bytes.size() != sizeof(Message))
      return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInvalidArgument});
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
    auto frame =
        grpcxx::AlignedMessageBuf::Allocate(grpcxx::Nghttp2ClientSession::kMaxMessageBytes + 1);
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
  ConfiguredCodec(const ConfiguredCodec&) = delete;
  ConfiguredCodec& operator=(const ConfiguredCodec&) = delete;
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
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kPermissionDenied, "risk denied"});
  }
  std::uint32_t expected_value{};
};
}  // namespace router_test
namespace grpcxx {
template <>
struct ContractMethods<router_test::ExplicitContract> {
  static constexpr std::string_view kServiceName = "orders.Test";
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
struct[[= grpcxx::service{.name = "orders.Reflected"}]] ReflectedContract {
  [[= grpcxx::rpc{.name = "Echo"}]] Message Echo(Message);
  [[= grpcxx::rpc{.name = "Later"}]] Message Later(Message);
};
static_assert(grpcxx::ServiceContract<ReflectedContract>);
#endif
static_assert(grpcxx::ServiceContract<ExplicitContract>);
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

std::array<std::byte, sizeof(Message)> Encode(Message message) {
  std::array<std::byte, sizeof(Message)> bytes{};
  auto encoded = MemoryCodec::Encode(message, bytes);
  EXPECT_TRUE(encoded);
  return bytes;
}

TEST(Cpp26Router, ExplicitStaticRoutesDecodeAndInvokeConcreteHandler) {
  Impl implementation;
  Router router(implementation);
  Sink sink;
  grpcxx::ServerContext context;
  const auto request = Encode(Message{42});
  ASSERT_EQ(Router::RouteCount(), 3);
  ASSERT_TRUE(router.Dispatch("/orders.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(implementation.calls, 1);
  EXPECT_EQ(sink.completions, 1);
  auto response = MemoryCodec::Decode<Message>(std::span{sink.bytes}.first(sink.size));
  ASSERT_TRUE(response);
  EXPECT_EQ(response->value, 43);
}

TEST(Cpp26Router, DeferredCompletionMoveAndStaleGenerationAreEnforced) {
  Impl implementation;
  Router router(implementation);
  Sink sink;
  grpcxx::ServerContext context;
  auto request = Encode(Message{8});
  ASSERT_TRUE(router.Dispatch("/orders.Test/Later", context, request, sink.Hooks(), 1, 3, 7));
  ASSERT_TRUE(implementation.pending);
  EXPECT_EQ(sink.completions, 0);
  auto moved = std::move(*implementation.pending);
  EXPECT_FALSE(implementation.pending->Current());
  auto invalid_status = moved.Fail(grpcxx::Status::OkStatus());
  ASSERT_FALSE(invalid_status);
  EXPECT_EQ(invalid_status.error().Code(), grpcxx::StatusCode::kInvalidArgument);
  EXPECT_TRUE(moved.Current());
  EXPECT_EQ(sink.failures, 0);
  ASSERT_TRUE(moved.Complete(grpcxx::Response<Message>{
      .message = Message{10}, .initial_metadata = {}, .trailing_metadata = {}}));
  EXPECT_EQ(sink.completions, 1);
  EXPECT_FALSE(moved.Fail(grpcxx::Status{grpcxx::StatusCode::kCancelled, "late failure"}));
  EXPECT_EQ(sink.failures, 0);
  Sink stale;
  auto handle = grpcxx::UnaryResponder<Message>::Bind<MemoryCodec>(stale.Hooks(), 1, 3, 7);
  stale.current = false;
  EXPECT_FALSE(handle.Complete(grpcxx::Response<Message>{
      .message = Message{1}, .initial_metadata = {}, .trailing_metadata = {}}));
  EXPECT_EQ(stale.completions, 0);
}

TEST(Cpp26Router, UnknownPathMalformedPayloadAndHandlerFailureHaveTerminalStatuses) {
  Impl implementation;
  Router router(implementation);
  grpcxx::ServerContext context;
  for (const auto name : {"Missing", "Echo", "Reject"}) {
    Sink sink;
    auto request = Encode(Message{8});
    auto bytes = name == std::string_view{"Echo"} ? std::span<const std::byte>{}
                                                  : std::span<const std::byte>{request};
    ASSERT_TRUE(router.Dispatch(fmt::format("/orders.Test/{}", name), context, bytes, sink.Hooks(),
                                1, 3, 7));
    EXPECT_EQ(sink.completions, 0);
    EXPECT_EQ(sink.failures, 1);
    EXPECT_EQ(sink.code, name == std::string_view{"Missing"} ? grpcxx::StatusCode::kUnimplemented
                         : name == std::string_view{"Echo"}
                             ? grpcxx::StatusCode::kInvalidArgument
                             : grpcxx::StatusCode::kPermissionDenied);
  }
  EXPECT_EQ(implementation.calls, 1);
}

#if defined(__cpp_impl_reflection) && __cpp_impl_reflection >= 202500L
TEST(Cpp26Router, NativeAnnotationsReflectImmediateAndDeferredMethods) {
  Impl implementation;
  grpcxx::ServiceRouter<ReflectedContract, Impl, MemoryCodec> router(implementation);
  grpcxx::ServerContext context;
  EXPECT_EQ(decltype(router)::RouteCount(), 2);
  Sink immediate;
  ASSERT_TRUE(router.Dispatch("/orders.Reflected/Echo", context, Encode(Message{2}),
                              immediate.Hooks(), 1, 3, 7));
  EXPECT_EQ(immediate.completions, 1);
  Sink deferred;
  ASSERT_TRUE(router.Dispatch("/orders.Reflected/Later", context, Encode(Message{2}),
                              deferred.Hooks(), 1, 3, 7));
  ASSERT_TRUE(implementation.pending);
  EXPECT_EQ(deferred.completions, 0);
  ASSERT_TRUE(implementation.pending->Complete(grpcxx::Response<Message>{
      .message = Message{4}, .initial_metadata = {}, .trailing_metadata = {}}));
  EXPECT_EQ(deferred.completions, 1);
}
#endif

/** The bridge drives the typed router over native nghttp2 bytes, without socket or service
 * substitutes. */
template <typename ResponseCodec = MemoryCodec>
struct HttpBridge {
  grpcxx::Nghttp2ServerSession server;
  grpcxx::Nghttp2ClientSession client;
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
              if (!sent)
                return std::unexpected(
                    grpcxx::Status{grpcxx::StatusCode::kInternal, "HTTP/2 completion failed"});
              return {};
            },
            .fail = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t gen,
                       grpcxx::Status status) noexcept -> std::expected<void, grpcxx::Status> {
              auto sent = static_cast<HttpBridge*>(opaque)->server.Complete(
                  stream, gen, {}, static_cast<int>(status.Code()));
              if (!sent)
                return std::unexpected(
                    grpcxx::Status{grpcxx::StatusCode::kInternal, "HTTP/2 trailers failed"});
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
            if (decoded)
              self.response = *decoded;
            else
              ++self.dispatch_failures;
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

TEST(Cpp26Router, NativeHttp2UnaryRoundtripsImmediateDeferredAndTrailersOnlyFailures) {
  for (const auto path :
       {"/orders.Test/Echo", "/orders.Test/Later", "/orders.Test/Missing", "/orders.Test/Reject"}) {
    HttpBridge pair;
    ASSERT_TRUE(pair.client.Ready());
    ASSERT_TRUE(pair.server.Ready());
    pair.Submit(path);
    pair.Drive();
    if (std::string_view{path}.ends_with("Later")) {
      EXPECT_EQ(pair.completed, 0);
      ASSERT_TRUE(pair.implementation.pending);
      ASSERT_TRUE(pair.implementation.pending->Complete(grpcxx::Response<Message>{
          .message = Message{19}, .initial_metadata = {}, .trailing_metadata = {}}));
      pair.Drive();
      EXPECT_FALSE(pair.implementation.pending->Fail(
          grpcxx::Status{grpcxx::StatusCode::kCancelled, "duplicate terminal"}));
    }
    EXPECT_EQ(pair.completed, 1);
    EXPECT_EQ(pair.dispatch_failures, 0);
    if (std::string_view{path}.ends_with("Missing"))
      EXPECT_EQ(pair.grpc_status, 12);
    else if (std::string_view{path}.ends_with("Reject"))
      EXPECT_EQ(pair.grpc_status, 7);
    else {
      EXPECT_EQ(pair.grpc_status, 0);
      EXPECT_EQ(pair.response.value, std::string_view{path}.ends_with("Later") ? 19 : 18);
    }
  }
}
TEST(Cpp26Router, TypedClientStubUsesInlineCompletionAndCompileTimeMethodPaths) {
  HttpBridge pair;
  grpcxx::ClientStub<ExplicitContract, MemoryCodec> stub(pair.client);
  grpcxx::ClientContext context;
  int callbacks = 0;
  Message response;
  auto call = stub.CallUnary<0>(
      context, Message{91},
      [&callbacks, &response, &context](
          grpcxx::ClientContext& seen,
          std::expected<grpcxx::Response<Message>, grpcxx::Status> result) noexcept {
        EXPECT_EQ(&seen, &context);
        ++callbacks;
        EXPECT_TRUE(result);
        if (result) response = result->message;
      });
  ASSERT_TRUE(call);
  EXPECT_EQ(callbacks, 0);
  pair.Drive();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(response.value, 92);
  EXPECT_EQ(pair.implementation.calls, 1);
}

TEST(Cpp26Router, ClientStubDestructionDetachesDeferredCompletion) {
  HttpBridge pair;
  grpcxx::ClientContext context;
  int callbacks = 0;
  {
    grpcxx::ClientStub<ExplicitContract, MemoryCodec> stub(pair.client);
    auto call = stub.CallUnary<1>(
        context, Message{91},
        [&callbacks](grpcxx::ClientContext&,
                     std::expected<grpcxx::Response<Message>, grpcxx::Status>) noexcept {
          ++callbacks;
        });
    ASSERT_TRUE(call);
    pair.Drive();
    ASSERT_TRUE(pair.implementation.pending);
    EXPECT_EQ(callbacks, 0);
  }
  pair.Drive();
  EXPECT_EQ(callbacks, 0);
  EXPECT_FALSE(pair.implementation.pending->Current());
}

template <typename ResponseCodec>
void CheckResponseSerializationFailure(grpcxx::StatusCode expected) {
  Impl implementation;
  grpcxx::ServiceRouter<ExplicitContract, Impl, ResponseCodec> router(implementation);
  grpcxx::ServerContext context;
  Sink sink;
  auto request = Encode(Message{6});
  ASSERT_TRUE(router.Dispatch("/orders.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(implementation.calls, 1);
  EXPECT_EQ(sink.completions, 0);
  EXPECT_EQ(sink.failures, 1);
  EXPECT_EQ(sink.code, expected);
  auto stale = grpcxx::UnaryResponder<Message>::Bind<MemoryCodec>(sink.Hooks(), 1, 3, 7);
  EXPECT_FALSE(stale.Current());
  EXPECT_FALSE(stale.Fail(grpcxx::Status{grpcxx::StatusCode::kCancelled}));
  EXPECT_FALSE(router.Dispatch("/orders.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(sink.failures, 1);

  HttpBridge<ResponseCodec> pair;
  pair.Submit("/orders.Test/Echo");
  pair.Drive();
  EXPECT_EQ(pair.completed, 1);
  EXPECT_EQ(pair.grpc_status, static_cast<int>(expected));
  EXPECT_EQ(pair.dispatch_failures, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  pair.Drive();
  EXPECT_EQ(pair.completed, 1);
}

TEST(Cpp26Router, ResponseSerializationFailureAlwaysEmitsOneTerminalStatus) {
  CheckResponseSerializationFailure<FailingEncodeCodec>(grpcxx::StatusCode::kResourceExhausted);
  CheckResponseSerializationFailure<OversizedEncodeCodec>(grpcxx::StatusCode::kInternal);
}

TEST(Cpp26Router, CodecBuffersAreAlignedForBothClientRequestAndServerResponse) {
  AlignedCodec::encodes = 0;
  HttpBridge<AlignedCodec> pair;
  grpcxx::ClientStub<ExplicitContract, AlignedCodec> stub(pair.client);
  grpcxx::ClientContext context;
  int callbacks = 0;
  auto call = stub.CallUnary<0>(
      context, Message{63},
      [&callbacks](grpcxx::ClientContext&,
                   std::expected<grpcxx::Response<Message>, grpcxx::Status> response) noexcept {
        ++callbacks;
        ASSERT_TRUE(response);
        EXPECT_EQ(response->message.value, 64);
      });
  ASSERT_TRUE(call);
  pair.Drive();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(AlignedCodec::encodes, 2);
  EXPECT_EQ(pair.dispatch_failures, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

TEST(Cpp26Router, CanonicalCodecSupportsTypedImmediateAndDeferredNativeRoundtrip) {
  HttpBridge<CanonicalCodec> pair;
  grpcxx::ClientStub<ExplicitContract, CanonicalCodec> stub(pair.client);
  grpcxx::ClientContext context;
  int callbacks = 0;
  std::vector<std::uint32_t> values;
  auto completion =
      [&callbacks, &values](
          grpcxx::ClientContext&,
          std::expected<grpcxx::Response<Message>, grpcxx::Status> response) noexcept {
        ++callbacks;
        ASSERT_TRUE(response);
        values.push_back(response->message.value);
      };
  auto immediate = stub.CallUnary<0>(context, Message{28}, completion);
  ASSERT_TRUE(immediate);
  EXPECT_EQ(callbacks, 0);
  pair.Drive();
  ASSERT_EQ(callbacks, 1);
  EXPECT_EQ(values.front(), 29);
  auto deferred = stub.CallUnary<1>(context, Message{43}, completion);
  ASSERT_TRUE(deferred);
  pair.Drive();
  EXPECT_EQ(callbacks, 1);
  ASSERT_TRUE(pair.implementation.pending);
  ASSERT_TRUE(pair.implementation.pending->Complete(grpcxx::Response<Message>{
      .message = Message{45}, .initial_metadata = {}, .trailing_metadata = {}}));
  pair.Drive();
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(values, (std::vector<std::uint32_t>{29, 45}));
  EXPECT_EQ(pair.implementation.calls, 2);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

TEST(Cpp26Router, CanonicalResponseEncodeFailureAndOversizeAreTerminal) {
  CheckResponseSerializationFailure<CanonicalEncodeFailure>(grpcxx::StatusCode::kResourceExhausted);
  CheckResponseSerializationFailure<CanonicalOversized>(grpcxx::StatusCode::kResourceExhausted);
}

template <typename Codec>
void CheckCanonicalRequestRejectedBeforeSubmit(
    grpcxx::StatusCode expected = grpcxx::StatusCode::kResourceExhausted) {
  HttpBridge<CanonicalCodec> pair;
  grpcxx::ClientStub<ExplicitContract, Codec> stub(pair.client);
  grpcxx::ClientContext context;
  int callbacks = 0;
  auto call = stub.template CallUnary<0>(
      context, Message{7},
      [&callbacks](grpcxx::ClientContext&,
                   std::expected<grpcxx::Response<Message>, grpcxx::Status>) noexcept {
        ++callbacks;
      });
  ASSERT_FALSE(call);
  EXPECT_EQ(call.error().Code(), expected);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  pair.Drive();
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pair.implementation.calls, 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

TEST(Cpp26Router, CanonicalCompressedOutputIsRejectedAtBothBoundaries) {
  CheckResponseSerializationFailure<CanonicalCompressed>(grpcxx::StatusCode::kInvalidArgument);
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalCompressed>(
      grpcxx::StatusCode::kInvalidArgument);
}

TEST(Cpp26Router, CanonicalRequestEncodeFailureAndOversizeNeverSubmit) {
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalEncodeFailure>();
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalOversized>();
}

TEST(Cpp26Router, CanonicalDecodeErrorsPropagateOnServerAndClient) {
  {
    HttpBridge<CanonicalDecodeFailure> pair;
    pair.Submit("/orders.Test/Echo");
    pair.Drive();
    EXPECT_EQ(pair.completed, 1);
    EXPECT_EQ(pair.grpc_status, static_cast<int>(grpcxx::StatusCode::kDataLoss));
    EXPECT_EQ(pair.implementation.calls, 0);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
  {
    HttpBridge<CanonicalCodec> pair;
    grpcxx::ClientStub<ExplicitContract, CanonicalDecodeFailure> stub(pair.client);
    grpcxx::ClientContext context;
    int callbacks = 0;
    auto call = stub.CallUnary<0>(
        context, Message{7},
        [&callbacks](grpcxx::ClientContext&,
                     std::expected<grpcxx::Response<Message>, grpcxx::Status> response) noexcept {
          ++callbacks;
          ASSERT_FALSE(response);
          EXPECT_EQ(response.error().Code(), grpcxx::StatusCode::kDataLoss);
        });
    ASSERT_TRUE(call);
    pair.Drive();
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(pair.implementation.calls, 1);
    EXPECT_EQ(pair.client.ActiveStreams(), 0);
    EXPECT_EQ(pair.server.ActiveStreams(), 0);
  }
}

TEST(Cpp26Router, InjectedCanonicalCodecInstanceRemainsLiveAcrossDeferredCompletion) {
  ConfiguredCodec codec(71);
  HttpBridge<ConfiguredCodec> pair(codec);
  grpcxx::ClientStub<ExplicitContract, ConfiguredCodec> stub(pair.client, codec);
  grpcxx::ClientContext context;
  int callbacks = 0;
  auto call = stub.CallUnary<1>(
      context, Message{48},
      [&callbacks](grpcxx::ClientContext&,
                   std::expected<grpcxx::Response<Message>, grpcxx::Status> response) noexcept {
        ++callbacks;
        ASSERT_TRUE(response);
        EXPECT_EQ(response->message.value, 50);
      });
  ASSERT_TRUE(call);
  pair.Drive();
  EXPECT_EQ(callbacks, 0);
  ASSERT_TRUE(pair.implementation.pending);
  EXPECT_EQ(pair.implementation.expected_value, 50);
  codec.SetKey(93);
  ASSERT_TRUE(pair.implementation.pending->Complete(grpcxx::Response<Message>{
      .message = Message{50}, .initial_metadata = {}, .trailing_metadata = {}}));
  pair.Drive();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(codec.encodes_, 2);
  EXPECT_EQ(codec.decodes_, 2);
  EXPECT_EQ(pair.dispatch_failures, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

TEST(Cpp26Router, InstanceOnlyCodecWithoutInjectionFailsBeforeSubmit) {
  HttpBridge<CanonicalCodec> pair;
  grpcxx::ClientStub<ExplicitContract, ConfiguredCodec> stub(pair.client);
  grpcxx::ClientContext context;
  int callbacks = 0;
  auto call = stub.CallUnary<0>(
      context, Message{1},
      [&callbacks](grpcxx::ClientContext&,
                   std::expected<grpcxx::Response<Message>, grpcxx::Status>) noexcept {
        ++callbacks;
      });
  ASSERT_FALSE(call);
  EXPECT_EQ(call.error().Code(), grpcxx::StatusCode::kFailedPrecondition);
  pair.Drive();
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pair.implementation.calls, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
}

}  // namespace router_test
