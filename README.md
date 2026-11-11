# grpcxx

[中文文档](README.zh-CN.md)

`grpcxx` is a Linux C++26 gRPC framework: wire framing, synchronous and Proactor client/server, streaming, codecs, compression, C++26 P2996 reflection, and codegen.

All targets compile with `-fno-exceptions` and C++26 P2996 reflection / P3394 annotations; fallible operations return `std::expected<T, grpcxx::Status>` by value.

## Execution Modes

`grpcxx` provides two execution modes over the same `nexora::Http2Connection` and gRPC framing/compression/metadata core:

### 1. Proactor Run-to-Completion Mode

- `ClientChannelService` and `GrpcServerService` drive `uring::Proactor&`, `nexora::CompletionRoutes<>&`, `nexora::IoUringSocket` / `nexora::TcpListener`, `nexora::Http2Connection`, and optional `nexora::TlsClientConfig` / `nexora::TlsServerConfig` with zero steady-state heap allocations on RPC frame dispatch.
- `ChannelCoordinatorService` coordinates `ChannelConnectivityFsm` transitions with `cares::Endpoint` lists and `nexora::EndpointFailover`.
- `ServiceRouter` and `ClientStub` bind reflected C++26 service contracts; unary handlers either return `std::expected<grpcxx::Response<T>, grpcxx::Status>` immediately or retain a move-only `UnaryResponder<T>`.
- `StreamMultiplexer` manages bounded per-stream queues and generation-checked handles for server-streaming, client-streaming, and bidirectional-streaming RPCs.

### 2. Caller-Thread Synchronous & Streaming Mode

- `SynchronousChannel` connects over `nexora::TcpTransport` (resolved via `cares::Resolver`), `nexora::UnixSocket`, adopted file descriptors, or `nexora::MemoryPipe`, with optional `nexora::TlsClientConfig` (`AlpnProtocol::kHttp2` enforced) and configurable connect/read/write stall deadlines.
- Configured with 256 KiB I/O buffers, 64 KiB header blocks, uncoalesced streaming delivery, and 16 MiB HTTP/2 flow-control windows so multi-megabyte payloads (including 4 MiB gRPC and Arrow Flight batches) and 64 KiB metadata blocks stream cleanly.
- Exposes `Unary`, `ClientStreamCall`, `ServerStreamCall`, `BidiStreamCall`, `Open`, `PollOnce`, `SetProgressHook`, `ActiveStreams`, and `NativeHandle` alongside `CallOptions` and `RpcResponse`.
- `RpcStream` provides `Send`, `HalfClose`, `Read` (`RecvNext`), `TryRecvNext`, `Finish`, `Cancel` (automatically emitting `RST_STREAM(kCancel)` if unfinished upon destruction), `Id`, `Done`, `InitialMetadata`, `TrailingMetadata`, and `FinalStatus`.
- `SynchronousServer` and `InMemoryChannelPair` provide a deterministic single-threaded HTTP/2 gRPC server peer (`SetInitialMetadata`, `Send`, `Finish`, `PollOnce`, `CancelledStreams`, `ActiveStreams`, `WindowUpdatesReceived`) over `nexora::MemoryPipe`, `TcpTransport`, or `UnixSocket` for unary and all three streaming RPC shapes.

## Codecs, Compression & Contract Generation

- **Codecs**: Protobuf (`grpcxx::codec_protobuf`), Cap'n Proto (`grpcxx::codec_capnp`, with 8-byte aligned borrowed reads), and Apache Fory (`grpcxx::codec_fory`).
- **Compression (`grpcxx::compression`)**: Identity, Gzip, Deflate, Zstd, LZ4, and Snappy with bounded receive-size enforcement and a 256-byte default compression bypass threshold.
- **Codegen Tools**: `protoc-gen-grpcxx`, `grpcxx-codegen`, and the CMake helper `grpcxx_generate_contracts()` emit C++26 reflected service contracts from `.proto`, `.capnp`, and `.fdl` schemas.

## Reflected Unary Example

```cpp
#include <grpcxx/reflection/service_router.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

struct EchoMessage {
  std::uint32_t sequence{};
};

struct [[= grpcxx::service{.name = "example.Echo"}]] EchoService {
  [[= grpcxx::rpc{.name = "Immediate", .kind = grpcxx::RpcKind::kUnary}]]
  EchoMessage Immediate(EchoMessage request);
  [[= grpcxx::rpc{.name = "Deferred", .kind = grpcxx::RpcKind::kUnary}]]
  EchoMessage Deferred(EchoMessage request);
};

struct EchoHandlers {
  std::optional<grpcxx::UnaryResponder<EchoMessage>> pending;
  EchoMessage deferred{};

  auto Immediate(grpcxx::ServerContext&, const EchoMessage& request) noexcept
      -> std::expected<grpcxx::Response<EchoMessage>, grpcxx::Status> {
    return grpcxx::Response<EchoMessage>{.message = {.sequence = request.sequence + 1}};
  }

  void Deferred(grpcxx::ServerContext&, const EchoMessage& request,
                grpcxx::UnaryResponder<EchoMessage> responder) noexcept {
    deferred.sequence = request.sequence + 2;
    pending.emplace(std::move(responder));
  }
};
```

See [`examples/cpp26_reflection_unary/main.cc`](examples/cpp26_reflection_unary/main.cc) for the full wiring with `ServiceRouter<EchoService, EchoHandlers, EchoCodec>` and caller-driven polling.

## Build & Verification

Requires CMake >= 4.0, Ninja, Linux `x86_64` with `io_uring` support, and a C++26 compiler supporting P2996 reflection and P3394 annotations (GCC 16 `-freflection` or Clang P2996 `-freflection-latest`). Dependencies resolve through vcpkg manifest mode with `VCPKG_OVERLAY_PORTS=$HOME/.local/vcpkg`.

```sh
./build.py --help
./build.py setup --help
./build.py build
./build.py test
./build.py bench
./build.py format --check src
```

| Preset | Configuration |
| --- | --- |
| `gcc-glibc` | GCC 16 reflection toolchain (default) |
| `gcc-asan` | GCC 16 with AddressSanitizer and UndefinedBehaviorSanitizer |
| `clang-glibc` | Clang P2996 reflection toolchain with `libc++` |
| `clang-asan` | Clang P2996 with AddressSanitizer and UndefinedBehaviorSanitizer |
| `gcc-musl` | GCC 16 / musl reflection toolchain |
| `clang-musl` | Clang P2996 / musl reflection toolchain |

## Consuming via CMake / vcpkg

```cmake
find_package(grpcxx CONFIG REQUIRED)
target_link_libraries(app PRIVATE grpcxx::grpcxx)
```

| Exported Targets | Responsibility |
| --- | --- |
| `grpcxx::grpcxx` | Umbrella interface target linking core services, streaming, channel, compression, and enabled codecs |
| `grpcxx::core`, `grpcxx::framing` | Status, metadata, deadlines, and 8-byte aligned length-prefixed gRPC framing |
| `grpcxx::h2`, `grpcxx::reflection` | gRPC HTTP/2 sessions + `SynchronousChannel`/`RpcStream`/`SynchronousServer` over `nexora::Http2Connection`, and C++26 reflected contracts |
| `grpcxx::client`, `grpcxx::server`, `grpcxx::channel` | Proactor `ClientChannelService`, `GrpcServerService`, and `ChannelCoordinatorService` |
| `grpcxx::streaming`, `grpcxx::interceptor`, `grpcxx::services` | Bounded stream multiplexing, call interceptors, health checking, and reflection registries |
| `grpcxx::compression` | Identity, Gzip, Deflate, Zstd, LZ4, and Snappy message compressors |
| `grpcxx::codec_protobuf`, `grpcxx::codec_capnp`, `grpcxx::codec_fory` | IDL codec backends |

## Examples & Benchmarks

| Example Directory | Binary | Description |
| --- | --- | --- |
| `examples/cpp26_reflection_unary` | `grpcxx-cpp26-reflection-unary` | Immediate and deferred reflected unary RPCs over `uring::Proactor` |
| `examples/multi_codec_showcase` | `multi_codec_showcase` | Generated record-store contracts across Protobuf, Cap'n Proto, and Apache Fory |
| `examples/streaming_showcase` | `streaming_showcase` | Caller-driven streaming RPCs and bounded flow control |
| `examples/tls_and_compression` | `tls_and_compression` | TLS 1.3 (`h2` ALPN) and multi-algorithm message compression |
| `examples/task_service` | `task_service` | End-to-end deferred unary calls, streaming, cancellation, and graceful drain |

Benchmark targets (`./build.py bench -t <target>`): `bench_aligned_framing`, `bench_service_router`, `bench_codecs` (`bench_multi_codec`), `bench_compression`, and `bench_rpc_pipeline`.

See [docs/README.md](docs/README.md), [docs/api.md](docs/api.md), and [docs/protoc-gen-grpcxx.md](docs/protoc-gen-grpcxx.md) for detailed API documentation.
