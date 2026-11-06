# C++26 API reference

Headers below are relative to `src/`; public symbols are in `grpcxx` unless
otherwise stated. Link the corresponding `grpcxx::` CMake target to obtain its
include paths, dependencies, and feature definitions. The framework uses
`std::expected<T, Status>` with exceptions disabled. Consult the headers for
complete overload sets and fixed-capacity limits.

## Status, metadata, contexts, and lifecycle

| Header | Public interface |
| --- | --- |
| `grpcxx/core/status.h` | `Status`, `StatusCode`, and status conversion helpers |
| `grpcxx/core/metadata.h` | `MetadataMap` |
| `grpcxx/core/inplace_delegate.h` | `InplaceDelegate<Signature, Capacity = 48>` |
| `grpcxx/core/service.h` | `ServiceConcept`, `ServiceState`, `PollBudget`, `PollResult`, `MonotonicTimestamp` |
| `grpcxx/core/timeout.h` | `GrpcTimeout` |
| `grpcxx/reflection/annotations.h` | `ClientContext`, `ServerContext`, `Request<T>`, `Response<T>` |

`Status{}` and `Status::OkStatus()` represent success. Construct an error with a
`StatusCode` and an optional message; inspect `Ok()`, `Code()`, and `Message()`.
`StatusCode` covers the canonical gRPC codes from `kOk` through
`kUnauthenticated`.

`MetadataMap` preserves insertion order and repeated keys. `Add` adds text;
`AddBinary` adds binary metadata using wire Base64 encoding. `Get` and
`GetBinary` return optional owning results; `WireValue` exposes a borrowed wire
value. Lookup returns the first matching key. Keys are normalized to lowercase; observe printable-value validation and
binary-key requirements.

`ClientContext` and `ServerContext` carry metadata, an optional deadline, and
cancellation state. They are declared in the reflection annotations header,
not in a separate core context header. `Response<T>` contains `message`,
`initial_metadata`, and `trailing_metadata`.

`InplaceDelegate` is a move-only inline callable with bounded capture storage.
The default 48-byte capture specialization occupies 64 bytes. Match the required
signature, capacity, and `noexcept` qualification at each call site; individual
APIs may choose another capacity. Captured references must outlive invocation.

`ServiceConcept` checks the following lifecycle operations:

- `Start()` returns `std::expected<void, Status>`.
- `State()` returns `ServiceState`.
- `PollOnce(PollBudget, MonotonicTimestamp)` returns `std::expected<PollResult, Status>`.
- `BeginShutdown(MonotonicTimestamp)` returns `std::expected<void, Status>`.
- `PollShutdown(PollBudget, MonotonicTimestamp)` returns either
  `std::expected<bool, Status>` or `std::expected<PollResult, Status>`.
- `Stopped()` returns `bool`; `Shutdown()` returns `std::expected<void, Status>`.

These concept operations are `noexcept`. `MonotonicTimestamp` is an unsigned
64-bit nanosecond value; use one consistent monotonic clock domain for supplied
values. Concrete services document shutdown deadline interpretation.
`PollBudget` has nonzero command, socket-event, completion, frame, and timer
budgets. `PollResult` reports those work counts, `pending`, and `deadline`.

`GrpcTimeout::Parse` accepts one to eight digits followed by `H`, `M`, `S`, `m`,
`u`, or `n`, returning nanoseconds or an error. `FormatTo(nanoseconds, buffer)`
returns a view into caller storage, rounds up where unit conversion requires it,
and returns an empty view if the supplied buffer is too small.

## Aligned framing

Link `grpcxx::framing`.

| Header | Public interface |
| --- | --- |
| `grpcxx/framing/aligned_message_buf.h` | `AlignedMessageBuf` |
| `grpcxx/framing/length_prefixed_frame.h` | `LengthPrefixedFrame` |

`AlignedMessageBuf::Allocate(uint32_t size, bool compressed = false)` returns a
fallible, move-only owner. Its allocation contains three padding bytes, one flag
byte, four big-endian length bytes, and the payload. `Payload()` is eight-byte
aligned. `WireBytes()` omits the padding; use it for transport output.
`PayloadSize()` and `Size()` report payload length; `WireSize()` includes the
five-byte message prefix. `Compressed()` and `SetCompressed()` access the flag.
Prefer `Allocate` when size or allocation failure must be reported as a status.

`LengthPrefixedFrame(max_receive_message_size)` accepts bytes through `Feed`,
returning `std::expected<std::vector<AlignedMessageBuf>, Status>`. It handles
split prefixes, split payloads, multiple messages, and zero-length messages.
An oversized advertised length fails before payload allocation. Failure is
latched until `Reset()`; a later malformed frame in one `Feed` call makes that
call fail as a whole. Inspect `State()`, `RejectedFrames()`,
`RejectedTransitions()`, and `BufferedPayloadBytes()` for progress and telemetry.

The parser's public name is `LengthPrefixedFrame`; writing uses
`AlignedMessageBuf::WireBytes()` rather than a separate frame-writer function.

## Reflection contracts and unary dispatch

Link `grpcxx::reflection` and the chosen codec target.

| Header | Public interface |
| --- | --- |
| `grpcxx/reflection/annotations.h` | `service`, `rpc`, `RpcKind`, `ServiceContract`, contexts and envelopes |
| `grpcxx/reflection/unary_responder.h` | `UnaryResponder<T>`, `UnaryResponderSink` |
| `grpcxx/reflection/service_router.h` | `ServiceRouter<Contract, Implementation, Codec>` |
| `grpcxx/reflection/client_stub.h` | `ClientStub<Contract, Codec>` |

A contract is a struct annotated with
`[[= grpcxx::service{.name = "package.Service"}]]`. Annotate methods with
`[[= grpcxx::rpc{.name = "Method", .kind = grpcxx::RpcKind::kUnary}]]`.
The method declaration supplies its request parameter and response return type.
`RpcKind` also includes server-streaming, client-streaming, and bidirectional
streaming kinds. Reflection describes these kinds, while `ServiceRouter`'s
current encoded request dispatch path handles unary methods.

Construct a router with a reference to its implementation and, when needed,
a reference to a codec instance. Immediate handlers have the shape
`std::expected<Response<Reply>, Status> Method(ServerContext&, const Request&) noexcept`.
Deferred handlers take an additional move-only `UnaryResponder<Reply>` and
return `void`. Complete a deferred response with `Complete(Response<Reply>)`,
or fail it with `Fail(Status)`. Both return a fallible completion result.

`Dispatch` receives the full RPC path, context, encoded payload, responder sink,
session ID, stream ID, and generation. The sink and server generation checks
reject stale completion. Keep implementations, codec instances, contexts, and
completion state alive until outstanding responders finish.

`ClientStub` borrows a `Nghttp2ClientSession`, optionally a codec instance.
`CallUnary<MethodIndex>(context, request, completion)` returns a stream ID or
status. Completion receives the context and
`std::expected<Response<Reply>, Status>`. Keep borrowed session and context
objects alive through completion. The stub has bounded per-method call slots;
its destructor cancels outstanding calls and detaches callbacks.

## HTTP/2 sessions

Link `grpcxx::h2`; include `grpcxx/h2/nghttp2_client_session.h` or
`grpcxx/h2/nghttp2_server_session.h`.

`Nghttp2ClientSession` and `Nghttp2ServerSession` own nghttp2 sessions, accept
input with `Receive`, and expose pending output with `Send`. The returned output
span is borrowed and invalidated by a subsequent `Send` or `Receive`; consume or
copy it before advancing the session. These classes do no socket I/O.
The unary adapters currently bound streams to 16 and messages to 65,536 bytes.

Client `Submit` accepts a path, encoded payload, completion state, callback,
and content type. `Cancel` detaches the completion and queues a stream reset.
Server `SetRequestHandler` installs request dispatch. Server `Complete` requires
stream ID and generation; `IsCurrent` checks that pair before a delayed reply.
Both sides expose readiness, state, active streams, rejected transitions,
`Ping`, and `GoAway`.

## Streaming

Link `grpcxx::streaming`.

| Header | Public interface |
| --- | --- |
| `grpcxx/streaming/stream_multiplexer.h` | `StreamMultiplexer`, `StreamHandle`, stream options and wire events |
| `grpcxx/streaming/server_stream_responder.h` | `ServerStreamResponder<T>` |
| `grpcxx/streaming/client_stream_writer.h` | `ClientStreamWriter<T>` |
| `grpcxx/streaming/client_stream_observer.h` | `ClientStreamObserver<T>` |
| `grpcxx/streaming/write_status.h` | `WriteStatus` |

Open a stream using `Open(RpcKind, is_server)` and retain its generation-bearing
handle. The multiplexer supports headers, messages, half-close, trailers,
reset, cancellation, per-stream queues, and window updates.
`PollOnce` advances bounded work. Supply a wire handler and feed received wire
events; the caller owns the HTTP/2 transport adapter and connection-level flow
control.

Both `WriteStatus::kQueued` and `WriteStatus::kHighWaterMarkReached` mean the
message was accepted. Wait for write readiness when backpressured; do not replay
an accepted message. An error result, including queue exhaustion, means the send
was rejected. Wire-handler success must accept the whole event; a failure must
leave transport state unchanged.

Typed responders and writers adapt message encoding to multiplexer operations.
`ClientStreamWriter` provides `Send`, `HalfClose`, `Cancel`, and write-ready or
cancel callbacks. `ClientStreamObserver` supplies header, message, write-ready,
and close callbacks. Borrowed callback payloads and encoded spans must remain
valid for the documented callback duration. Release handles only after their
lifecycle permits it; stale generations are rejected.

## Proactor and socket services

| Target and header | Public interface |
| --- | --- |
| `grpcxx::uring`, `grpcxx/uring/proactor_service.h` | `ProactorService` |
| `grpcxx::client`, `grpcxx/client/client_channel_service.h` | `ClientChannelService` |
| `grpcxx::server`, `grpcxx/server/grpc_server_service.h` | `GrpcServerService` |

`ProactorService` owns an `io_uring` ring on its creating thread. Start it,
submit accept/connect/receive/send/poll operations or timers, and advance it
through `PollOnce`. Submission returns operation identifiers used by `Cancel`.
Callbacks report operation completion; cancellation still requires draining
completions. Keep submitted buffers alive until completion and handle partial
sends. Shutdown is an explicitly polled lifecycle.

`ClientChannelService` borrows the proactor and takes a socket address in its
options. It offers lifecycle operations and encoded `CallUnary` with an inline
completion or a context/function-pointer completion.
`GrpcServerService` borrows the proactor, accepts address and request-handler
options, reports its bound address through `LocalAddress`, and completes replies
with connection, stream, and generation tokens. These socket services currently
carry plaintext HTTP/2. Memory-BIO TLS is a separate composition interface.

## Channel, DNS, interceptors, and registries

| Target and header | Public interface |
| --- | --- |
| `grpcxx::channel`, `grpcxx/channel/channel_coordinator_service.h` | `ChannelCoordinatorService` |
| `grpcxx/channel/channel_connectivity_fsm.h` | `ChannelConnectivityFsm`, `ChannelState`, `ChannelEvent` |
| `grpcxx/channel/load_balancer.h` | `PickFirstLoadBalancer` |
| `grpcxx/channel/load_balancer.h` | `RoundRobinLoadBalancer` |
| `grpcxx::dns`, `grpcxx/dns/dns_service.h` | `DnsService` |
| `grpcxx::interceptor`, `grpcxx/interceptor/interceptor_chain.h` | `InterceptorChain` |
| `grpcxx::services`, `grpcxx/services/health_service.h` | `HealthService` |
| `grpcxx/services/reflection_service.h` | `ReflectionService` |

`ChannelState` covers idle, resolving, connecting, TLS handshaking, ready,
transient failure, and shutting down. `ChannelConnectivityFsm` advances through
`OnEvent` and `OnTime`, exposing deadlines and rejected transitions.
`ChannelCoordinatorService` tracks bounded endpoints and connectivity events.
The load balancers select an endpoint only when it is both healthy and ready;
PickFirst chooses the lowest eligible index, and RoundRobin rotates its cursor.

`DnsService` drives c-ares through caller polling. `Resolve(host, port, callback,
user)` submits a lookup; startup, cancellation, and shutdown remain on the owner
thread. Its public wrapper is named `DnsService`.

`InterceptorChain` composes interceptors as a template pack.
`HealthService` maintains serving state, supports `Check`, and queues watch
notifications. `ReflectionService::Register<Contract>()` registers explicit static
`kServiceName` / `kMethods` tables for `ListServices` and `ListMethods`; these
tables satisfy `ReflectionTableContract`, rather than being inferred from
annotations by this registry. These are service building blocks;
applications supply any required network protocol adapters and routing.

## Codecs, compression, and TLS

| Target and header | Public interface |
| --- | --- |
| `grpcxx::codec_protobuf`, `grpcxx/codec/protobuf_codec.h` | `ProtobufCodec` |
| `grpcxx::codec_fory`, `grpcxx/codec/fory_codec.h` | `ForyCodec` |
| `grpcxx::codec_capnp`, `grpcxx/codec/capnp_codec.h` | `CapnpCodec`, `CapnpMessage<Schema>` |
| `grpcxx::compression`, `grpcxx/compression/compression_registry.h` | `CompressionRegistry` |
| `grpcxx/compression/compression_algorithm.h` | `CompressionAlgorithm` |
| `grpcxx::tls`, `grpcxx/tls/tls_context.h` | `TlsContext`, `TlsContextOptions` |
| `grpcxx/tls/tls_session.h` | `TlsSession`, `TlsHandshakeState` |

Each codec provides `Encode` and `Decode` and its gRPC content type. Protobuf
uses native protobuf messages. Fory owns an engine; register reflected message
types through `Engine()` before use. `CapnpMessage` owns its message arena;
`GetBuilder` and `GetReader` borrow it. `CapnpCodec::ReadAligned` creates a
reader over aligned input that must outlive that reader. Owning decode copies
into a message arena; reader traversal and nesting are bounded.

`CompressionAlgorithm` includes identity, Gzip, Deflate, Zstd, LZ4, and Snappy.
`CompressionRegistry` supplies encoding-token parsing, advertisement,
negotiation, compression, and decompression. Compression defaults to bypassing
messages below 256 bytes and payloads that do not get smaller. A clear message
flag means identity even when another encoding was negotiated. Decompression
requires an explicit maximum receive size and returns aligned storage. LZ4 uses
a checksummed frame; Deflate uses the zlib wrapper.

`TlsContext::Create(options)` creates a move-only context for client or server
mode, certificate/key and CA loading, and optional mutual authentication.
Contexts enforce TLS 1.3, disable early data, and configure ALPN `h2`.
`TlsSession::Create(context, server_name)` creates a memory-BIO session.
`FeedCiphertext`, `DrainCiphertext`, `WritePlaintext`, and `ReadPlaintext` return
byte progress. `PollOnce` advances handshake or shutdown; `VerifyAlpnH2`
checks the negotiated protocol. Observe `State` and `RejectedTransitions`.

Zero progress means retry on a later tick. Retry a pending plaintext write with
the same bytes; successful writes may consume a prefix. Feed transport EOF
through `MarkTransportEof`; drain generated ciphertext during handshake and
shutdown. These wrappers perform no socket I/O and do not spawn threads.
