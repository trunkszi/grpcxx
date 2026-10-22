# Generated order-book RPCs

This example owns its three IDLs in `idl/order_book.{proto,fdl,capnp}`.
CMake runs `grpcxx-codegen`, which invokes Protobuf's `protoc` and
`protoc-gen-grpcxx`, Apache Fory's `foryc`, or Cap'n Proto's `capnp` compiler.
Each backend generates its own `order_book.grpcxx.h` and message bindings in a
separate build directory. The example does not use test fixture messages.

Each enabled codec runs the same checks:

1. Encode and decode a generated quote request through `AlignedMessageBuf` and
   require an 8-byte-aligned payload. Cap'n Proto also checks that its aligned
   reader borrows text directly from that payload.
2. Connect peer 1's `Nghttp2ClientSession` to peer 2's `Nghttp2ServerSession`,
   and peer 2's client session to peer 1's server session. Each server uses a
   `ServiceRouter` for its generated contract, and each caller uses a typed
   `ClientStub`.
3. Initiate `Quote` and `PlaceOrder` from both peers. `Quote` completes inside
   dispatch. `PlaceOrder` stores an owning response and `UnaryResponder` until
   the caller explicitly completes deferred work after pumping to idle.
4. Require four successful callbacks, the opposite server's identity, matching
   request IDs and response values, no duplicate callbacks, no active streams,
   and no rejected session transitions.

The byte pump fragments traffic into at most seven bytes per receive operation.
It has a finite iteration budget and needs no sockets, threads, sleeps, or
external server. Deferred handlers keep owning messages, never borrowed request
views. Stubs are destroyed before their callback storage and sessions.

## Build and run

Use the root uv-managed CLI with the reflection compiler and enabled codec
dependencies configured by its preset:

```sh
./build.py --help
./build.py build -t multi_codec_showcase
./build.py test --filter multi_codec_showcase
./build/gcc-glibc/bin/multi_codec_showcase
```

The root build includes this example when `GRPCXX_BUILD_EXAMPLES=ON`.
Individual `GRPCXX_ENABLE_*` CMake options control its codec backends.
Compiler tools and Protobuf headers/runtime must use compatible versions.
See the [contract generator guide](../../docs/protoc-gen-grpcxx.md) for tool
locations, native output files, import paths, and CMake generation rules.

A successful all-codec run prints an aligned round-trip line and a mutual RPC
line for each codec and exits with status 0. CTest runs that same executable.
