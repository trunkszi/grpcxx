# grpcxx

grpcxx is a Linux C++26 gRPC framework with caller-driven, single-threaded
`io_uring` services and compile-time RPC contracts. It uses P2996 reflection and
P3394 annotations to connect typed handlers, codecs, and HTTP/2 routing.

The application owns the polling loop. It supplies monotonic timestamps and
work budgets, completes deferred calls, and drains services before releasing
resources. Library targets compile with `-fno-exceptions`; fallible APIs use
`std::expected<T, grpcxx::Status>`.

## Capabilities and boundaries

- `ServiceRouter` and `ClientStub` consume reflected contracts. Unary handlers
  can return a response immediately or retain a move-only `UnaryResponder`.
- `Nghttp2ClientSession` and `Nghttp2ServerSession` exchange HTTP/2 bytes with
  bounded unary streams. `ClientChannelService` and `GrpcServerService` drive
  plaintext unary socket connections through a shared `ProactorService`.
- `StreamMultiplexer` provides bounded queues and generation-checked handles
  for server streaming, client streaming, and bidirectional streaming. Its
  transport adapter supplies validated events and connection-level flow control.
- Codecs support Protobuf, Apache Fory, and Cap'n Proto. Framed payload storage
  is aligned to eight bytes; Cap'n Proto also offers borrowed aligned reads.
- Compression supports identity, Gzip, Deflate, Zstd, LZ4, and Snappy, with a
  receive-size bound and a default 256-byte compression bypass threshold.
- OpenSSL memory BIOs provide caller-driven TLS 1.3, HTTP/2 ALPN, and optional
  client-certificate verification. Applications connect these wrappers to their
  transport; the plaintext unary services do not automatically enable TLS.
- Channel coordination, endpoint selection, caller-polled DNS, interceptors,
  health watching, and reflected-service registries are separate modules.

## Build and development

Use a reflection-enabled GCC 16 or Clang with the required experimental
reflection/annotation support. A stock compiler with only partial C++26 support
is insufficient. The default configuration uses GCC 16.2 and CMake 3.31.10. The root CMake minimum is 3.28; 3.30+ supplies native C++26 support.

The build also needs the enabled C++ dependencies: fmt, nghttp2, liburing,
c-ares, OpenSSL, compression libraries, Protobuf/libprotoc, Cap'n Proto, Apache
Fory, GoogleTest, and Google Benchmark. `./build.py setup` installs GCC and ABI-matched C++ dependencies under
`$HOME/.local`; `CMakePresets.json` selects that portable prefix.

`build.py` is a Bash launcher for a UV-managed Python 3.12 CLI. It downloads UV
from Astral when absent, manages the CLI environment from its lockfile, and
selects the cached CMake 3.31.10 when the current CMake is older than 3.30.
`setup` installs APT prerequisites, CMake 3.31.10, Ninja 1.13.2, the GCC
reflection toolchain, fmt, GoogleTest, Google Benchmark, nghttp2, and the pinned
Fory library/compiler. It supports Linux x86_64 with APT. The GitHub-hosted CI
uses Ubuntu 24.04 and the same setup and CLI commands.

```sh
./build.py --help
./build.py setup --help
./build.py build
./build.py test
./build.py bench
./build.py bench --list
./build.py build -t order_service -j 4
./build.py --format json test --no-build --filter test_core_types
./build.py format --check src
./build.py tidy --help
./build.py clean --help
```

| Preset | Configuration |
| --- | --- |
| `gcc-glibc` | GCC reflection compiler and dependencies under `$HOME/.local`; default |
| `gcc-glibc-vcpkg` | GCC with vcpkg manifest dependency installation |
| `gcc-musl` | Declared GCC/musl wrapper configuration |
| `clang-glibc` | Declared Clang/glibc reflection toolchain |
| `clang-musl` | Declared Clang/musl reflection wrapper configuration |

The default GCC preset uses the host sysroot and Ubuntu/Debian multiarch header
and startup-file directories with the downloaded cross-compiler. Other presets
need their matching compiler and dependencies. Artifacts land in
`build/<preset>/bin/`. See the [CLI guide](scripts/cli/README.md) for command
options, compiler aliases, test selection, and structured output.

## Reflected unary handlers

This excerpt uses the contract and handler signatures from
[the complete reflection example](examples/cpp26_reflection_unary/main.cc):

```cpp
#include <grpcxx/reflection/service_router.h>
#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

struct Quote { std::uint32_t price{}; };

struct [[= grpcxx::service{.name = "example.Quotes"}]] Quotes {
    [[= grpcxx::rpc{.name = "Immediate", .kind = grpcxx::RpcKind::kUnary}]]
    Quote Immediate(Quote request);
    [[= grpcxx::rpc{.name = "Deferred", .kind = grpcxx::RpcKind::kUnary}]]
    Quote Deferred(Quote request);
};

struct QuoteHandlers {
    std::optional<grpcxx::UnaryResponder<Quote>> pending;
    Quote deferred{};

    auto Immediate(grpcxx::ServerContext&, const Quote& request) noexcept
        -> std::expected<grpcxx::Response<Quote>, grpcxx::Status> {
        return grpcxx::Response<Quote>{.message = {.price = request.price + 1}};
    }

    void Deferred(grpcxx::ServerContext&, const Quote& request,
                  grpcxx::UnaryResponder<Quote> responder) noexcept {
        deferred.price = request.price + 2;
        pending.emplace(std::move(responder));
    }
};
```

The full program supplies `QuoteCodec`, a `ServiceRouter<Quotes, QuoteHandlers,
QuoteCodec>`, transport callbacks, and a polling loop. It completes the retained
responder on a later caller tick and drains the client, server, and Proactor.
The route names are `/example.Quotes/Immediate` and `/example.Quotes/Deferred`.

## Runnable examples

These five examples are registered by the current root build:

| Source directory under `examples/` | Binary under `build/<preset>/bin/` | Demonstration |
| --- | --- | --- |
| `cpp26_reflection_unary` | `grpcxx-cpp26-reflection-unary` | Immediate/deferred unary calls over the Proactor |
| `multi_codec_showcase` | `multi_codec_showcase` | Generated order-book IDL and mutual RPCs for three codecs |
| `streaming_showcase` | `streaming_showcase` | Caller-driven stream adapters and bounded queues |
| `tls_and_compression` | `tls_and_compression` | TLS 1.3 memory-BIO exchange and message compression |
| `order_service` | `order_service` | Deferred order checks, cancellation, streaming, and shutdown |

The socket/Proactor example requires a kernel/environment permitting io_uring.
The in-memory examples demonstrate composition without requiring remote peers.

## Benchmark suites

Use `./build.py bench -t <CLI target>`; optional Google Benchmark arguments
follow `--`. The default command runs aligned framing for 0.01 seconds.

| CLI target | CMake target and binary |
| --- | --- |
| `bench_aligned_framing` | `bench_aligned_framing` |
| `bench_service_router` | `bench_service_router` |
| `bench_codecs` | `bench_multi_codec` (also accepted directly) |
| `bench_compression` | `bench_compression` |
| `bench_order_pipeline` | `bench_order_pipeline` |

Measurements depend on build type, hardware, and workload. No comparison with
another gRPC implementation is implied by a successful benchmark run.

## CMake library targets

The root build exports these build-tree targets for `target_link_libraries`:

| Targets | Responsibility |
| --- | --- |
| `grpcxx::core`, `grpcxx::framing` | Values, lifecycle primitives, aligned framing |
| `grpcxx::reflection`, `grpcxx::h2` | Typed contracts/routing and nghttp2 sessions |
| `grpcxx::uring`, `grpcxx::client`, `grpcxx::server` | Proactor and plaintext unary transports |
| `grpcxx::channel`, `grpcxx::dns` | Connectivity, selection, and DNS |
| `grpcxx::interceptor`, `grpcxx::services` | Call interception, health, reflection registries |
| `grpcxx::streaming` | Bounded streaming adapters |
| `grpcxx::codec_protobuf`, `grpcxx::codec_fory`, `grpcxx::codec_capnp` | Enabled codec backends |
| `grpcxx::compression`, `grpcxx::tls` | Message compression and TLS memory BIOs |

See [the documentation index](docs/README.md), [API reference](docs/api.md),
[contract generation guide](docs/protoc-gen-grpcxx.md), and
[shared agent guide](AGENTS.md).
