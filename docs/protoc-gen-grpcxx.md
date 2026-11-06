# Reflected contract generation

The tools under `src/grpcxx/tools/` generate C++26 reflected service contracts
from Protobuf (`.proto`), Apache Fory (`.fdl`), and Cap'n Proto (`.capnp`) schemas.
They retain each backend's native message bindings. Generated contracts use
P2996 reflection and P3394 `grpcxx::service` / `grpcxx::rpc` annotations.
They are consumed by `ServiceRouter` and `ClientStub`, rather than generated
runtime service implementations.

## Build and dependencies

Use the repository CLI:

```sh
./build.py --help
./build.py build -t grpcxx-codegen
./build.py build -t protoc-gen-grpcxx
```

The tools are emitted under `build/<preset>/bin/`. They require the dependencies
configured by the selected preset, including Protobuf/libprotoc, Cap'n Proto,
and fmt. Generating a backend additionally requires its native compiler:
`protoc`, `foryc`, or `capnp`. Fory's descriptor adapter uses the Fory compiler's
Python package. Compile generated C++ using the matching message libraries and
a reflection-capable C++26 compiler with exceptions disabled.

## Multi-backend driver

The supported driver command line is:

```text
grpcxx-codegen --out DIR [-I DIR] [--protoc PATH] FILE.proto|FILE.fdl|FILE.capnp ...
```

Use `--out` to choose the output directory (the driver defaults to `.`).
`-I DIR` and `-IDIR` add schema import directories.
`--protoc` selects the Protobuf compiler; select a version compatible with your
linked Protobuf runtime. `--help` prints usage. Unknown options and colliding
output stems are rejected. Use separate output directories for same-stem
schemas from different backends, as in the showcase below.

Compiler resolution is portable across developer machines and CI runners:

| Tool | Environment override | User-local candidate | PATH fallback |
| --- | --- | --- | --- |
| Protobuf | `PROTOC` (or `--protoc`) | `$HOME/.local/bin/protoc` | `protoc` |
| Fory | `FORYC` | `$HOME/.local/bin/foryc` | `foryc` |
| Fory adapter Python | `FORY_PYTHON` | `$HOME/.local/bin/python3` | `python3` |
| Cap'n Proto | none | `$HOME/.local/bin/capnp` | `capnp` |

A nonempty environment override wins; otherwise an existing user-local file is
selected before the PATH fallback. `--protoc` overrides Protobuf's default
selection. Subprocesses use `execvp` with separate arguments, without a shell.
`./build.py setup` installs Fory's compiler and Python interpreter in a dedicated
venv and links them into `$HOME/.local/bin`.

The [multi-codec showcase](../examples/multi_codec_showcase/) contains three
schemas representing the same order-book operations:

| Input in `examples/multi_codec_showcase/idl/` | Native bindings | Contract |
| --- | --- | --- |
| `order_book.proto` | `order_book.pb.h`, `order_book.pb.cc` | `order_book.grpcxx.h` |
| `order_book.fdl` | `cpp/examples_order_book_fory.h` | `order_book.grpcxx.h` |
| `order_book.capnp` | `order_book.capnp.h`, `order_book.capnp.c++` | `order_book.grpcxx.h` |

For example, from the repository root, after building the tools:

```sh
build/gcc-glibc/bin/grpcxx-codegen \
  --out build/generated/proto \
  --protoc /usr/bin/protoc \
  -I examples/multi_codec_showcase/idl \
  examples/multi_codec_showcase/idl/order_book.proto

build/gcc-glibc/bin/grpcxx-codegen \
  --out build/generated/fory \
  -I examples/multi_codec_showcase/idl \
  examples/multi_codec_showcase/idl/order_book.fdl

build/gcc-glibc/bin/grpcxx-codegen \
  --out build/generated/capnp \
  -I examples/multi_codec_showcase/idl -I /usr/include \
  examples/multi_codec_showcase/idl/order_book.capnp
```

Use the actual installed compiler/include locations on your machine. In
particular, the Cap'n Proto import path must contain `capnp/c++.capnp`.

## Contract and wire names

The Protobuf schema declares package `examples.order_book.proto`, request and
response messages, and service `OrderBook` with `Quote` and `PlaceOrder` methods.
The emitted contract has the following form:

```cpp
#pragma once
#include <grpcxx/tools/service_annotations.h>
#include "order_book.pb.h"

namespace examples::order_book::proto {
struct [[= grpcxx::service{.name = "examples.order_book.proto.OrderBook"}]] OrderBookContract {
  [[= grpcxx::rpc{.name = "Quote", .kind = grpcxx::RpcKind::kUnary}]]
  ::examples::order_book::proto::QuoteResponse Quote(
      const ::examples::order_book::proto::QuoteRequest&) noexcept;
  [[= grpcxx::rpc{.name = "PlaceOrder", .kind = grpcxx::RpcKind::kUnary}]]
  ::examples::order_book::proto::OrderResponse PlaceOrder(
      const ::examples::order_book::proto::OrderRequest&) noexcept;
};
}
```

The wire paths include `/examples.order_book.proto.OrderBook/Quote` and
`/examples.order_book.proto.OrderBook/PlaceOrder`. The Fory schema uses package
`examples.order_book.fory`, and its contract uses that package in wire paths.
Native Fory types need registration with the `ForyCodec` engine as demonstrated
by the showcase.

The Cap'n Proto schema uses namespace `examples::order_book::capnp` and interface
methods `quote` and `placeOrder`. Their wire paths retain this spelling, for
example `/examples.order_book.capnp.OrderBook/quote`. Generated C++ contract
method names are capitalized. Request and response types wrap native method
parameter/result schemas in `grpcxx::CapnpMessage`, rather than replacing the
native Cap'n Proto representation.

Protobuf/Fory streaming declarations map to `RpcKind`. Cap'n Proto interfaces
can mark methods with the Bool annotations `grpcxxClientStreaming` and
`grpcxxServerStreaming`; combinations select the streaming kind. Nested,
generic, and inherited Cap'n Proto interfaces are explicitly rejected by the
current adapter. A generated streaming contract does not itself provide a
socket streaming transport; see the [API reference](api.md).

## Direct Protobuf plugin invocation

`protoc-gen-grpcxx` implements the protoc plugin protocol. Invoke it through
`protoc`, together with native C++ message generation:

```sh
mkdir -p build/generated/proto
protoc -I examples/multi_codec_showcase/idl \
  --cpp_out=build/generated/proto \
  --plugin=protoc-gen-grpcxx="$(pwd)/build/gcc-glibc/bin/protoc-gen-grpcxx" \
  --grpcxx_out=build/generated/proto \
  examples/multi_codec_showcase/idl/order_book.proto
```

This produces `order_book.pb.h`, `order_book.pb.cc`, and `order_book.grpcxx.h`.
The reflected contract header includes the native message header.

## CMake integration

The showcase generates each backend into a distinct directory, lists native
bindings as outputs, and depends on both framework generator targets. The
following Protobuf version follows that structure:

```cmake
find_package(Protobuf REQUIRED)
set(schema "${CMAKE_CURRENT_SOURCE_DIR}/idl/order_book.proto")
set(output_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/proto")
set(contract "${output_dir}/order_book.grpcxx.h")
set(bindings
    "${output_dir}/order_book.pb.h"
    "${output_dir}/order_book.pb.cc")

add_custom_command(
  OUTPUT "${contract}" ${bindings}
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${output_dir}"
  COMMAND $<TARGET_FILE:grpcxx-codegen>
          --out "${output_dir}"
          --protoc "$<TARGET_FILE:protobuf::protoc>"
          -I "${CMAKE_CURRENT_SOURCE_DIR}/idl" "${schema}"
  DEPENDS grpcxx-codegen protoc-gen-grpcxx "${schema}"
  VERBATIM COMMAND_EXPAND_LISTS)

target_sources(my_application PRIVATE "${contract}" ${bindings})
target_include_directories(my_application PRIVATE "${output_dir}")
target_link_libraries(my_application PRIVATE
  grpcxx::reflection grpcxx::h2 grpcxx::codec_protobuf)
```

`my_application` must already be an executable or library target. Add the
framework through its root CMake project before referring to its targets.
The actual [showcase CMake file](../examples/multi_codec_showcase/CMakeLists.txt)
also handles Fory's generated `cpp/` include directory and Python adapter
dependency, and obtains Cap'n Proto import directories from its CMake targets.
For Cap'n Proto, compile the generated `.capnp.c++` and link the required native
RPC/schema support along with `grpcxx::codec_capnp`. For Fory, include the
native generated header and link `grpcxx::codec_fory`.

The tools module also defines `grpcxx_generate_contracts(target FILES ...
OUTPUT_DIRECTORY ... IMPORT_DIRECTORIES ... PROTOC ...)`. This helper creates a
custom target for contract headers; applications must still list and compile
native message bindings as appropriate. The explicit `add_custom_command`
pattern above makes both kinds of generated output visible to the build graph.
