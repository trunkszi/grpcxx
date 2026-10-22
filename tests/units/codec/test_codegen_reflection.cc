/** Generated contracts exercise reflection and real codecs over in-memory HTTP/2. */
#include <grpcxx/codec/capnp_codec.h>
#include <grpcxx/codec/fory_codec.h>
#include <grpcxx/codec/protobuf_codec.h>
#include <grpcxx/h2/nghttp2_server_session.h>
#include <grpcxx/reflection/client_stub.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstdint>
#include <meta>
#include <optional>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <vector>

#if defined(GRPCXX_TEST_PROTO_CONTRACT)
#include "proto/showcase.grpcxx.h"
#endif
#if defined(GRPCXX_TEST_FORY_CONTRACT)
#include "fdl/showcase.grpcxx.h"
#endif
#if defined(GRPCXX_TEST_CAPNP_CONTRACT)
#include "capnp/showcase.grpcxx.h"
#endif

namespace {
/** Read the canonical annotation's structural text. */
constexpr auto AnnotationNameView(const auto& name) -> std::string_view { return name.View(); }
/** Verify annotations survive parsing and describe four distinct RPC shapes. */
template <typename Contract>
consteval auto HasFourShapes(std::string_view service_name) -> bool {
  bool matching_service = false;
  template for (constexpr auto annotation :
                std::define_static_array(std::meta::annotations_of(^^Contract))) {
    if constexpr (std::meta::remove_cv(std::meta::type_of(annotation)) == ^^grpcxx::service) {
      matching_service |=
          AnnotationNameView(std::meta::extract<grpcxx::service>(annotation).name) == service_name;
    }
  }
  std::array<unsigned int, 4> kinds{};
  template for (constexpr auto member : std::define_static_array(
                    std::meta::members_of(^^Contract, std::meta::access_context::unchecked()))) {
    if constexpr (std::meta::is_function(member)) {
      template for (constexpr auto annotation :
                    std::define_static_array(std::meta::annotations_of(member))) {
        if constexpr (std::meta::remove_cv(std::meta::type_of(annotation)) == ^^grpcxx::rpc) {
          constexpr auto rpc = std::meta::extract<grpcxx::rpc>(annotation);
          ++kinds[static_cast<std::size_t>(rpc.kind)];
          if (AnnotationNameView(rpc.name).empty()) return false;
        }
      }
    }
  }
  return matching_service && std::ranges::all_of(kinds, [](auto count) { return count == 1; });
}
/** Generated declarations expose the exact typed noexcept method seam. */
template <typename Contract, typename Request, typename Response>
consteval auto HasMessageSignatures() -> bool {
  using Signature = Response (Contract::*)(const Request&) noexcept;
  return std::same_as<decltype(&Contract::Unary), Signature> &&
         std::same_as<decltype(&Contract::ServerStream), Signature> &&
         std::same_as<decltype(&Contract::ClientStream), Signature> &&
         std::same_as<decltype(&Contract::Bidi), Signature>;
}
#if defined(GRPCXX_TEST_PROTO_CONTRACT)
static_assert(HasMessageSignatures<codec_fixture::ShowcaseContract, codec_fixture::Request,
                                   codec_fixture::Response>());
static_assert(HasFourShapes<codec_fixture::ShowcaseContract>("codec_fixture.Showcase"));
#if __has_include(<grpcxx/reflection/annotations.h>)
static_assert(grpcxx::ServiceContract<codec_fixture::ShowcaseContract>);
#endif
static_assert(std::same_as<decltype(&codec_fixture::CollisionContract::CollisionContract_),
                           codec_fixture::Response (codec_fixture::CollisionContract::*)(
                               const codec_fixture::Request&) noexcept>);
#endif
#if defined(GRPCXX_TEST_FORY_CONTRACT)
static_assert(HasMessageSignatures<fory_fixture::ShowcaseContract, fory_fixture::Request,
                                   fory_fixture::Response>());
static_assert(HasFourShapes<fory_fixture::ShowcaseContract>("fory_fixture.Showcase"));
#if __has_include(<grpcxx/reflection/annotations.h>)
static_assert(grpcxx::ServiceContract<fory_fixture::ShowcaseContract>);
#endif
#endif
#if defined(GRPCXX_TEST_CAPNP_CONTRACT)
using CapnpContract = capnp_fixture::nested::ShowcaseContract;
using CapnpService = capnp_fixture::nested::Showcase;
using RenamedContract = capnp_fixture::nested::RenamedInterfaceContract;
using RenamedService = capnp_fixture::nested::RenamedInterface;
static_assert(
    std::same_as<decltype(&RenamedContract::RenamedCall),
                 grpcxx::CapnpMessage<RenamedService::RenamedCallResults> (RenamedContract::*)(
                     const grpcxx::CapnpMessage<RenamedService::RenamedCallParams>&) noexcept>);
static_assert(std::same_as<decltype(&CapnpContract::Unary),
                           grpcxx::CapnpMessage<CapnpService::UnaryResults> (CapnpContract::*)(
                               const grpcxx::CapnpMessage<CapnpService::UnaryParams>&) noexcept>);
static_assert(
    std::same_as<decltype(&CapnpContract::ServerStream),
                 grpcxx::CapnpMessage<CapnpService::ServerStreamResults> (CapnpContract::*)(
                     const grpcxx::CapnpMessage<CapnpService::ServerStreamParams>&) noexcept>);
static_assert(
    std::same_as<decltype(&CapnpContract::ClientStream),
                 grpcxx::CapnpMessage<CapnpService::ClientStreamResults> (CapnpContract::*)(
                     const grpcxx::CapnpMessage<CapnpService::ClientStreamParams>&) noexcept>);
static_assert(std::same_as<decltype(&CapnpContract::Bidi),
                           grpcxx::CapnpMessage<CapnpService::BidiResults> (CapnpContract::*)(
                               const grpcxx::CapnpMessage<CapnpService::BidiParams>&) noexcept>);
static_assert(
    HasFourShapes<capnp_fixture::nested::ShowcaseContract>("capnp_fixture.nested.Showcase"));
#if __has_include(<grpcxx/reflection/annotations.h>)
static_assert(grpcxx::ServiceContract<capnp_fixture::nested::ShowcaseContract>);
#endif
#endif
}  // namespace

namespace {
#if defined(GRPCXX_TEST_PROTO_CONTRACT)
struct ProtoTraits {
  using Contract = codec_fixture::ShowcaseContract;
  using Codec = grpcxx::ProtobufCodec;
  using Request = codec_fixture::Request;
  using Response = codec_fixture::Response;
  static void Configure(Codec&) {}
  static Request MakeRequest() {
    Request request;
    request.set_id(73);
    request.set_symbol("EURUSD");
    return request;
  }
  static std::int64_t RequestId(const Request& request) { return request.id(); }
  static Response MakeResponse(std::int64_t value) {
    Response response;
    response.set_id(value);
    return response;
  }
  static std::int64_t ResponseId(const Response& response) { return response.id(); }
};
#endif
#if defined(GRPCXX_TEST_FORY_CONTRACT)
struct ForyTraits {
  using Contract = fory_fixture::ShowcaseContract;
  using Codec = grpcxx::ForyCodec;
  using Request = fory_fixture::Request;
  using Response = fory_fixture::Response;
  static void Configure(Codec& codec) {
    codec.Engine().register_struct<Request>(1);
    codec.Engine().register_struct<Response>(2);
  }
  static Request MakeRequest() {
    Request request;
    request.set_id(73);
    request.set_symbol("EURUSD");
    return request;
  }
  static std::int64_t RequestId(const Request& request) { return request.id(); }
  static Response MakeResponse(std::int64_t value) {
    Response response;
    response.set_id(value);
    return response;
  }
  static std::int64_t ResponseId(const Response& response) { return response.id(); }
};
#endif
#if defined(GRPCXX_TEST_CAPNP_CONTRACT)
struct CapnpTraits {
  using Contract = CapnpContract;
  using Codec = grpcxx::CapnpCodec;
  using Request = grpcxx::CapnpMessage<CapnpService::UnaryParams>;
  using Response = grpcxx::CapnpMessage<CapnpService::UnaryResults>;
  static void Configure(Codec&) {}
  static Request MakeRequest() {
    Request request;
    auto root = request.GetBuilder().initRequest();
    root.setId(73);
    root.setSymbol("EURUSD");
    return request;
  }
  static std::int64_t RequestId(const Request& request) {
    return request.GetReader().getRequest().getId();
  }
  static Response MakeResponse(std::int64_t value) {
    Response response;
    response.GetBuilder().initResponse().setId(value);
    return response;
  }
  static std::int64_t ResponseId(const Response& response) {
    return response.GetReader().getResponse().getId();
  }
};
static_assert(grpcxx::Codec<grpcxx::CapnpCodec, CapnpTraits::Request>);
static_assert(std::default_initializable<CapnpTraits::Request>);
static_assert(std::is_nothrow_move_constructible_v<CapnpTraits::Request>);
static_assert(!std::copy_constructible<CapnpTraits::Request>);
#endif

/** Immediate and deferred implementations share the generated method names. */
template <typename Traits>
struct ImmediateHandler {
  int calls_{};
  auto Unary(grpcxx::ServerContext&, const typename Traits::Request& request) noexcept
      -> std::expected<grpcxx::Response<typename Traits::Response>, grpcxx::Status> {
    ++calls_;
    return grpcxx::Response<typename Traits::Response>{
        Traits::MakeResponse(Traits::RequestId(request) + 1), {}, {}};
  }
};
template <typename Traits>
struct DeferredHandler {
  int calls_{};
  std::optional<grpcxx::UnaryResponder<typename Traits::Response>> pending_;
  std::optional<typename Traits::Response> response_;
  void Unary(grpcxx::ServerContext&, const typename Traits::Request& request,
             grpcxx::UnaryResponder<typename Traits::Response> responder) noexcept {
    ++calls_;
    response_.emplace(Traits::MakeResponse(Traits::RequestId(request) + 2));
    pending_.emplace(std::move(responder));
  }
};

/** Bounded caller-driven byte pumping runs actual nghttp2 sessions and typed routes. */
template <typename Traits, typename Handler>
struct GeneratedBridge {
  using Router = grpcxx::ServiceRouter<typename Traits::Contract, Handler, typename Traits::Codec>;
  grpcxx::Nghttp2ClientSession client_;
  grpcxx::Nghttp2ServerSession server_;
  Handler handler_;
  Router router_;
  grpcxx::ServerContext context_;
  int dispatch_failures_{};
  std::string_view response_content_type_;
  explicit GeneratedBridge(typename Traits::Codec& codec,
                           std::string_view content_type = Router::kContentType,
                           std::string_view response_content_type = {})
      : server_(content_type),
        router_(handler_, codec),
        response_content_type_(response_content_type) {
    server_.SetRequestHandler(this, [](void* opaque, std::int32_t stream, std::uint64_t generation,
                                       std::string_view path,
                                       std::span<const std::byte> payload) noexcept {
      auto& self = *static_cast<GeneratedBridge*>(opaque);
      auto dispatched =
          self.router_.Dispatch(path, self.context_, payload, self.Hooks(), 1, stream, generation);
      if (!dispatched) ++self.dispatch_failures_;
    });
  }
  grpcxx::UnaryResponderSink Hooks() noexcept {
    return {
        .context = this,
        .is_current =
            [](void* opaque, std::uint64_t session, std::int32_t stream,
               std::uint64_t generation) noexcept {
              return session == 1 &&
                     static_cast<GeneratedBridge*>(opaque)->server_.IsCurrent(stream, generation);
            },
        .complete = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                       std::span<const std::byte> payload, grpcxx::MetadataMap&&,
                       grpcxx::MetadataMap&&) noexcept -> std::expected<void, grpcxx::Status> {
          auto& self = *static_cast<GeneratedBridge*>(opaque);
          return self.server_.Complete(stream, generation, payload, 0, self.response_content_type_);
        },
        .fail = [](void* opaque, std::uint64_t, std::int32_t stream, std::uint64_t generation,
                   grpcxx::Status status) noexcept -> std::expected<void, grpcxx::Status> {
          return static_cast<GeneratedBridge*>(opaque)->server_.Complete(
              stream, generation, {}, static_cast<int>(status.Code()));
        }};
  }
  template <typename From, typename To>
  static auto Transfer(From& from, To& to) -> std::expected<bool, grpcxx::Status> {
    auto bytes = from.Send();
    if (!bytes) return std::unexpected(std::move(bytes.error()));
    if (bytes->empty()) return false;
    auto remaining = *bytes;
    while (!remaining.empty()) {
      auto fragment = remaining.first(std::min(remaining.size(), std::size_t{7}));
      auto received = to.Receive(fragment);
      if (!received) return std::unexpected(std::move(received.error()));
      if (*received != fragment.size())
        return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kInternal});
      remaining = remaining.subspan(*received);
    }
    return true;
  }
  auto Drive() -> std::expected<void, grpcxx::Status> {
    for (auto tick : std::views::iota(0, 512)) {
      static_cast<void>(tick);
      auto outgoing = Transfer(client_, server_);
      if (!outgoing) return std::unexpected(std::move(outgoing.error()));
      auto incoming = Transfer(server_, client_);
      if (!incoming) return std::unexpected(std::move(incoming.error()));
      if (!*outgoing && !*incoming) return {};
    }
    return std::unexpected(grpcxx::Status{grpcxx::StatusCode::kDeadlineExceeded});
  }
};

template <typename Traits, bool Deferred>
void CheckGeneratedRoundTrip() {
  typename Traits::Codec client_codec, server_codec;
  Traits::Configure(client_codec);
  Traits::Configure(server_codec);
  using Handler = std::conditional_t<Deferred, DeferredHandler<Traits>, ImmediateHandler<Traits>>;
  GeneratedBridge<Traits, Handler> pair(server_codec);
  ASSERT_TRUE(pair.client_.Ready());
  ASSERT_TRUE(pair.server_.Ready());
  grpcxx::ClientStub<typename Traits::Contract, typename Traits::Codec> stub(pair.client_,
                                                                             client_codec);
  grpcxx::ClientContext context;
  std::optional<typename Traits::Response> retained;
  int callbacks = 0;
  auto request = Traits::MakeRequest();
  ASSERT_TRUE(stub.template CallUnary<0>(
      context, request,
      [&callbacks, &retained](
          grpcxx::ClientContext&,
          std::expected<grpcxx::Response<typename Traits::Response>, grpcxx::Status>
              response) noexcept {
        ++callbacks;
        ASSERT_TRUE(response);
        retained.emplace(std::move(response->message));
      }));
  ASSERT_TRUE(pair.Drive());
  if constexpr (Deferred) {
    EXPECT_EQ(callbacks, 0);
    ASSERT_TRUE(pair.handler_.pending_);
    ASSERT_TRUE(pair.handler_.response_);
    ASSERT_TRUE(pair.handler_.pending_->Complete(
        grpcxx::Response<typename Traits::Response>{std::move(*pair.handler_.response_), {}, {}}));
    ASSERT_TRUE(pair.Drive());
    EXPECT_FALSE(pair.handler_.pending_->Current());
  }
  ASSERT_EQ(callbacks, 1);
  ASSERT_TRUE(retained);
  EXPECT_EQ(Traits::ResponseId(*retained), Deferred ? 75 : 74);
  EXPECT_EQ(pair.handler_.calls_, 1);
  EXPECT_EQ(pair.dispatch_failures_, 0);
  EXPECT_EQ(pair.client_.ActiveStreams(), 0);
  EXPECT_EQ(pair.server_.ActiveStreams(), 0);
  EXPECT_EQ(pair.client_.RejectedTransitions(), 0);
  EXPECT_EQ(pair.server_.RejectedTransitions(), 0);
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(Traits::ResponseId(*retained), Deferred ? 75 : 74);
}

struct TerminalResult {
  int callbacks_{};
  int status_{-1};
  static void Complete(void* opaque, std::int32_t, int status,
                       std::span<const std::byte>) noexcept {
    auto& self = *static_cast<TerminalResult*>(opaque);
    ++self.callbacks_;
    self.status_ = status;
  }
};

template <typename Traits>
void CheckMalformedPayload() {
  typename Traits::Codec codec;
  Traits::Configure(codec);
  GeneratedBridge<Traits, ImmediateHandler<Traits>> pair(codec);
  TerminalResult result;
  const std::array invalid{std::byte{0x0a}, std::byte{0xff}};
  using Method = std::tuple_element_t<
      0, decltype(grpcxx::reflection_detail::Methods<typename Traits::Contract>())>;
  constexpr auto& path = grpcxx::reflection_detail::kMethodPath<typename Traits::Contract, Method>;
  ASSERT_TRUE(pair.client_.Submit({path.data(), path.size()}, invalid, &result,
                                  &TerminalResult::Complete, Traits::Codec::kContentType));
  ASSERT_TRUE(pair.Drive());
  EXPECT_EQ(result.callbacks_, 1);
  EXPECT_EQ(result.status_, 3);
  EXPECT_EQ(pair.handler_.calls_, 0);
  EXPECT_EQ(pair.client_.ActiveStreams(), 0);
  EXPECT_EQ(pair.server_.ActiveStreams(), 0);
}

#if defined(GRPCXX_TEST_PROTO_CONTRACT)
TEST(GeneratedCodecs, ProtobufImmediate) { CheckGeneratedRoundTrip<ProtoTraits, false>(); }
TEST(GeneratedCodecs, ProtobufDeferred) { CheckGeneratedRoundTrip<ProtoTraits, true>(); }
TEST(GeneratedCodecs, ProtobufMalformedIsTerminal) { CheckMalformedPayload<ProtoTraits>(); }
TEST(GeneratedCodecs, RejectsMismatchedRequestAndResponseSubtypes) {
  grpcxx::ProtobufCodec codec;
  {
    GeneratedBridge<ProtoTraits, ImmediateHandler<ProtoTraits>> pair(codec);
    TerminalResult result;
    grpcxx::AlignedMessageBuf request;
    ASSERT_TRUE(codec.Encode(ProtoTraits::MakeRequest(), request));
    ASSERT_TRUE(pair.client_.Submit("/codec_fixture.Showcase/Unary", request.Payload(), &result,
                                    &TerminalResult::Complete, "application/grpc+fory"));
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(result.callbacks_, 1);
    EXPECT_EQ(result.status_, 13);
    EXPECT_EQ(pair.handler_.calls_, 0);
    EXPECT_EQ(pair.client_.ActiveStreams(), 0);
    EXPECT_EQ(pair.server_.ActiveStreams(), 0);
  }
  {
    GeneratedBridge<ProtoTraits, ImmediateHandler<ProtoTraits>> pair(
        codec, grpcxx::ProtobufCodec::kContentType, "application/grpc+fory");
    grpcxx::ClientStub<ProtoTraits::Contract, grpcxx::ProtobufCodec> stub(pair.client_);
    grpcxx::ClientContext context;
    int callbacks = 0;
    ASSERT_TRUE(stub.CallUnary<0>(
        context, ProtoTraits::MakeRequest(),
        [&callbacks](grpcxx::ClientContext&,
                     std::expected<grpcxx::Response<ProtoTraits::Response>, grpcxx::Status>
                         response) noexcept {
          ++callbacks;
          ASSERT_FALSE(response);
          EXPECT_EQ(response.error().Code(), grpcxx::StatusCode::kInternal);
        }));
    ASSERT_TRUE(pair.Drive());
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(pair.handler_.calls_, 1);
    EXPECT_EQ(pair.client_.ActiveStreams(), 0);
    EXPECT_EQ(pair.server_.ActiveStreams(), 0);
  }
}
#endif
#if defined(GRPCXX_TEST_FORY_CONTRACT)
TEST(GeneratedCodecs, ForyImmediate) { CheckGeneratedRoundTrip<ForyTraits, false>(); }
TEST(GeneratedCodecs, ForyDeferred) { CheckGeneratedRoundTrip<ForyTraits, true>(); }
TEST(GeneratedCodecs, ForyMalformedIsTerminal) { CheckMalformedPayload<ForyTraits>(); }
#endif
#if defined(GRPCXX_TEST_CAPNP_CONTRACT)
TEST(GeneratedCodecs, CapnpImmediate) { CheckGeneratedRoundTrip<CapnpTraits, false>(); }
TEST(GeneratedCodecs, CapnpDeferred) { CheckGeneratedRoundTrip<CapnpTraits, true>(); }
TEST(GeneratedCodecs, CapnpMalformedIsTerminal) { CheckMalformedPayload<CapnpTraits>(); }
TEST(GeneratedCodecs, CapnpOwnersMoveAndDecodeWithoutBorrowingWireStorage) {
  auto source = CapnpTraits::MakeRequest();
  auto view = source.GetReader();
  auto moved = std::move(source);
  EXPECT_EQ(view.getRequest().getId(), 73);
  EXPECT_EQ(CapnpTraits::RequestId(moved), 73);
  source.GetBuilder().initRequest().setId(99);
  EXPECT_EQ(CapnpTraits::RequestId(source), 99);
  grpcxx::AlignedMessageBuf wire;
  ASSERT_TRUE(grpcxx::CapnpCodec::Encode(moved, wire));
  CapnpTraits::Request decoded;
  ASSERT_TRUE(grpcxx::CapnpCodec::Decode(wire.Payload(), decoded));
  std::ranges::fill(wire.Payload(), std::byte{});
  EXPECT_EQ(CapnpTraits::RequestId(decoded), 73);
  EXPECT_EQ(decoded.GetReader().getRequest().getSymbol(), "EURUSD");
  EXPECT_FALSE(grpcxx::CapnpCodec::Decode(wire.Payload(), decoded));
  EXPECT_EQ(CapnpTraits::RequestId(decoded), 73);
}
#endif
}  // namespace
