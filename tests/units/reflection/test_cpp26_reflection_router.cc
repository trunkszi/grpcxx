#include "router_test_fixtures.h"

namespace router_test {

TEST(Cpp26Router, StaticRoutesInvokeHandler) {
  Impl implementation;
  Router router(implementation);
  Sink sink;
  grpcxx::ServerContext context;
  const auto request = Encode(Message{42});
  ASSERT_EQ(Router::RouteCount(), 3);
  ASSERT_TRUE(router.Dispatch("/rpc.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(implementation.calls, 1);
  EXPECT_EQ(sink.completions, 1);
  auto response = MemoryCodec::Decode<Message>(std::span{sink.bytes}.first(sink.size));
  ASSERT_TRUE(response);
  EXPECT_EQ(response->value, 43);
}

TEST(Cpp26Router, DeferredMoveRejectsStaleGeneration) {
  Impl implementation;
  Router router(implementation);
  Sink sink;
  grpcxx::ServerContext context;
  auto request = Encode(Message{8});
  ASSERT_TRUE(router.Dispatch("/rpc.Test/Later", context, request, sink.Hooks(), 1, 3, 7));
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

TEST(Cpp26Router, RoutingFailuresProduceTerminalStatus) {
  Impl implementation;
  Router router(implementation);
  grpcxx::ServerContext context;
  for (const auto name : {"Missing", "Echo", "Reject"}) {
    Sink sink;
    auto request = Encode(Message{8});
    auto bytes = name == std::string_view{"Echo"} ? std::span<const std::byte>{}
                                                  : std::span<const std::byte>{request};
    ASSERT_TRUE(
        router.Dispatch(fmt::format("/rpc.Test/{}", name), context, bytes, sink.Hooks(), 1, 3, 7));
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
TEST(Cpp26Router, AnnotationsReflectDeferredMethods) {
  Impl implementation;
  grpcxx::ServiceRouter<ReflectedContract, Impl, MemoryCodec> router(implementation);
  grpcxx::ServerContext context;
  EXPECT_EQ(decltype(router)::RouteCount(), 2);
  Sink immediate;
  ASSERT_TRUE(router.Dispatch("/rpc.Reflected/Echo", context, Encode(Message{2}), immediate.Hooks(),
                              1, 3, 7));
  EXPECT_EQ(immediate.completions, 1);
  Sink deferred;
  ASSERT_TRUE(router.Dispatch("/rpc.Reflected/Later", context, Encode(Message{2}), deferred.Hooks(),
                              1, 3, 7));
  ASSERT_TRUE(implementation.pending);
  EXPECT_EQ(deferred.completions, 0);
  ASSERT_TRUE(implementation.pending->Complete(grpcxx::Response<Message>{
      .message = Message{4}, .initial_metadata = {}, .trailing_metadata = {}}));
  EXPECT_EQ(deferred.completions, 1);
}
#endif

TEST(Cpp26Router, NativeUnaryResponseShapes) {
  for (const auto path :
       {"/rpc.Test/Echo", "/rpc.Test/Later", "/rpc.Test/Missing", "/rpc.Test/Reject"}) {
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
    if (std::string_view{path}.ends_with("Missing")) {
      EXPECT_EQ(pair.grpc_status, 12);
    } else if (std::string_view{path}.ends_with("Reject")) {
      EXPECT_EQ(pair.grpc_status, 7);
    } else {
      EXPECT_EQ(pair.grpc_status, 0);
      EXPECT_EQ(pair.response.value, std::string_view{path}.ends_with("Later") ? 19 : 18);
    }
  }
}

TEST(Cpp26Router, TypedStubUsesStaticPaths) {
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
  ASSERT_TRUE(router.Dispatch("/rpc.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(implementation.calls, 1);
  EXPECT_EQ(sink.completions, 0);
  EXPECT_EQ(sink.failures, 1);
  EXPECT_EQ(sink.code, expected);
  auto stale = grpcxx::UnaryResponder<Message>::Bind<MemoryCodec>(sink.Hooks(), 1, 3, 7);
  EXPECT_FALSE(stale.Current());
  EXPECT_FALSE(stale.Fail(grpcxx::Status{grpcxx::StatusCode::kCancelled}));
  EXPECT_FALSE(router.Dispatch("/rpc.Test/Echo", context, request, sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(sink.failures, 1);

  HttpBridge<ResponseCodec> pair;
  pair.Submit("/rpc.Test/Echo");
  pair.Drive();
  EXPECT_EQ(pair.completed, 1);
  EXPECT_EQ(pair.grpc_status, static_cast<int>(std::to_underlying(expected)));
  EXPECT_EQ(pair.dispatch_failures, 0);
  EXPECT_EQ(pair.client.ActiveStreams(), 0);
  EXPECT_EQ(pair.server.ActiveStreams(), 0);
  pair.Drive();
  EXPECT_EQ(pair.completed, 1);
}

TEST(Cpp26Router, SerializationFailureTerminatesOnce) {
  CheckResponseSerializationFailure<FailingEncodeCodec>(grpcxx::StatusCode::kResourceExhausted);
  CheckResponseSerializationFailure<OversizedEncodeCodec>(grpcxx::StatusCode::kInternal);
}

TEST(Cpp26Router, CodecBuffersRemainAligned) {
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

TEST(Cpp26Router, CanonicalCodecRoundtrip) {
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

TEST(Cpp26Router, ResponseEncodeFailuresTerminate) {
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

TEST(Cpp26Router, CompressedOutputRejected) {
  CheckResponseSerializationFailure<CanonicalCompressed>(grpcxx::StatusCode::kInvalidArgument);
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalCompressed>(
      grpcxx::StatusCode::kInvalidArgument);
}

TEST(Cpp26Router, RequestEncodeFailuresNeverSubmit) {
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalEncodeFailure>();
  CheckCanonicalRequestRejectedBeforeSubmit<CanonicalOversized>();
}

TEST(Cpp26Router, DecodeErrorsPropagate) {
  {
    HttpBridge<CanonicalDecodeFailure> pair;
    pair.Submit("/rpc.Test/Echo");
    pair.Drive();
    EXPECT_EQ(pair.completed, 1);
    EXPECT_EQ(pair.grpc_status,
              static_cast<int>(std::to_underlying(grpcxx::StatusCode::kDataLoss)));
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

TEST(Cpp26Router, InjectedCodecSurvivesDeferredCompletion) {
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

TEST(Cpp26Router, MissingCodecFailsBeforeSubmit) {
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

TEST(Cpp26Router, RouteMissRejectsStaleSink) {
  Impl implementation;
  Router router(implementation);
  grpcxx::ServerContext context;
  auto request = Encode(Message{5});
  Sink same_suffix;
  ASSERT_TRUE(router.Dispatch("/alt.Test/Echo", context, request, same_suffix.Hooks(), 1, 3, 7));
  EXPECT_EQ(same_suffix.completions, 0);
  EXPECT_EQ(same_suffix.failures, 1);
  EXPECT_EQ(same_suffix.code, grpcxx::StatusCode::kUnimplemented);

  grpcxx::UnaryResponderSink null_sink{};
  auto failed = router.Dispatch("/rpc.Test/Unknown", context, request, null_sink, 1, 3, 7);
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().Code(), grpcxx::StatusCode::kFailedPrecondition);
}

[[nodiscard]] constexpr std::uint64_t SuffixWord(std::string_view path) noexcept {
  std::uint64_t word = 0;
  const auto width = path.size() < 8U ? path.size() : 8U;
  const auto start = path.size() - width;
  for (std::size_t index = 0; index < width; ++index) {
    const auto byte = static_cast<unsigned char>(path[start + index]);
    word |= static_cast<std::uint64_t>(byte) << (index * 8U);
  }
  return word;
}

[[nodiscard]] std::uint64_t ProbeRouteHits(
    std::uint64_t suffix, std::uint64_t length,
    std::span<const std::pair<std::uint64_t, std::uint64_t>> table) noexcept {
  std::uint64_t hits = 0;
  for (std::uint64_t index = 0; index < table.size(); ++index) {
    const auto miss = (table[index].first ^ suffix) | (table[index].second ^ length);
    hits |= static_cast<std::uint64_t>(miss == 0U) << index;
  }
  return hits;
}

TEST(Cpp26Router, ProbeFingerprintMatchesDispatch) {
  constexpr std::array<std::string_view, 3> paths{
      "/rpc.Test/Echo",
      "/rpc.Test/Later",
      "/rpc.Test/Reject",
  };
  std::array<std::pair<std::uint64_t, std::uint64_t>, paths.size()> table{};
  for (std::size_t index = 0; index < paths.size(); ++index) {
    table[index] = {SuffixWord(paths[index]), paths[index].size()};
  }

  Impl implementation;
  Router router(implementation);
  grpcxx::ServerContext context;
  for (std::size_t index = 0; index < paths.size(); ++index) {
    EXPECT_EQ(ProbeRouteHits(table[index].first, table[index].second, table),
              std::uint64_t{1} << index);
    Sink sink;
    const auto calls_before = implementation.calls;
    ASSERT_TRUE(router.Dispatch(paths[index], context, Encode(Message{1}), sink.Hooks(), 1, 3, 7));
    EXPECT_EQ(implementation.calls, calls_before + 1);
    if (index == 1) {
      ASSERT_TRUE(implementation.pending);
      implementation.pending.reset();
    } else if (index == 0) {
      EXPECT_EQ(sink.completions, 1);
    } else {
      EXPECT_EQ(sink.failures, 1);
      EXPECT_EQ(sink.code, grpcxx::StatusCode::kPermissionDenied);
    }
  }

  constexpr std::string_view missing = "/rpc.Test/Missing";
  EXPECT_EQ(ProbeRouteHits(SuffixWord(missing), missing.size(), table), 0U);
  Sink sink;
  ASSERT_TRUE(router.Dispatch(missing, context, Encode(Message{1}), sink.Hooks(), 1, 3, 7));
  EXPECT_EQ(implementation.calls, 3);
  EXPECT_EQ(sink.failures, 1);
  EXPECT_EQ(sink.code, grpcxx::StatusCode::kUnimplemented);
}

}  // namespace router_test
