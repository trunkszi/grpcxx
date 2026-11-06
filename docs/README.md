# grpcxx documentation

This documentation covers the C++26 framework under `src/grpcxx/` and the
repository's root CMake build.

| Guide | Use it for |
| --- | --- |
| [API reference](api.md) | Headers, public types, ownership, and progress rules |
| [Contract generation](protoc-gen-grpcxx.md) | Protobuf, Apache Fory, and Cap'n Proto schema tools |
| [Development CLI](../scripts/cli/README.md) | Presets, build, CTest, benchmarks, formatting, and setup |
| [Examples](../examples/) | Complete contracts and runnable compositions |
| [Shared agent guide](../AGENTS.md) | Engineering invariants and repository navigation |

## Execution and lifecycle

The caller owns the thread and advances services through bounded polling.
`ProactorService` supplies Linux `io_uring` operations without an internal event
thread. `ServiceConcept` describes startup, polling, shutdown initiation,
shutdown polling, stopped-state inspection, and final shutdown. `PollBudget`
bounds commands, socket events, completions, frames, and timers; `PollResult`
reports work and an optional next deadline.

Keep services and their callback state alive until pending operations and
shutdown completions have drained. Submission is not completion: buffers
borrowed by asynchronous operations must remain valid until their callbacks run,
including cancellation callbacks. Partial writes and backpressure require
explicit progress on later caller ticks.

## Messages and HTTP/2

`AlignedMessageBuf` stores `[3B pad][1B flags][4B big-endian length][payload]`.
The payload starts at an eight-byte-aligned address. `WireBytes()` excludes the
three padding bytes and includes the five-byte gRPC message prefix.
`LengthPrefixedFrame` incrementally accepts fragmented or coalesced wire bytes
and checks its receive limit before allocating a message payload.

`Nghttp2ClientSession` and `Nghttp2ServerSession` adapt nghttp2 to caller-supplied
input and output buffers. Their bounded unary adapters correlate stream IDs,
response completion, cancellation, and server generation tokens. They perform no
socket I/O. `ClientChannelService` and `GrpcServerService` compose plaintext
unary HTTP/2 sessions with `ProactorService`.

`StreamMultiplexer` maintains streaming lifecycle, per-stream queues, flow-control
state and cancellation. Typed `ServerStreamResponder`,
`ClientStreamWriter`, and `ClientStreamObserver` wrap these stream handles.
Applications connect its wire callbacks to their transport; the standalone
streaming multiplexer is not automatically connected by the unary socket
services.

## Contracts and codecs

P2996 reflection and P3394 annotations describe services and methods:
`[[= grpcxx::service{...}]]` and `[[= grpcxx::rpc{...}]]`.
`ServiceRouter` dispatches unary methods to immediate or deferred handlers;
`ClientStub` provides typed unary calls over `Nghttp2ClientSession`.
Schema tools emit reflected contracts and native message bindings for Protobuf,
Apache Fory, and Cap'n Proto. Codec-specific CMake targets expose the associated
headers and dependencies.

Serialization and control-plane containers may allocate. The framework's
single-threaded execution model does not imply that every API is allocation-free.
Cap'n Proto's borrowed aligned reader requires the input buffer to outlive the
reader; owning decode uses a separate message arena.

## Compression and TLS

`CompressionRegistry` negotiates identity, Gzip, Deflate, Zstd, LZ4, and Snappy.
Small or incompressible messages keep a clear compressed flag. Decoders enforce
an explicit receive-message limit and return aligned output buffers.

`TlsContext` and `TlsSession` use OpenSSL TLS 1.3 and memory BIOs, disable early
data, and negotiate ALPN `h2`. Callers feed and drain ciphertext and advance
handshake or shutdown steps. These wrappers do not own sockets or automatically
wrap the plaintext unary socket services. The
[TLS and compression example](../examples/tls_and_compression/) demonstrates
memory-BIO composition on one thread.
