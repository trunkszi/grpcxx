# Contract generators

`protoc-gen-grpcxx` implements the normal Protobuf compiler plugin protocol.
`grpcxx-codegen` invokes the installed compiler with a distinct argument vector,
without a shell, and emits a `.grpcxx.h` contract alongside native message files:

```sh
grpcxx-codegen --out generated -I schemas schemas/orders.proto
grpcxx-codegen --out generated -I schemas schemas/orders.fdl
grpcxx-codegen --out generated -I schemas schemas/orders.capnp
```

Tool selection checks `PROTOC`, `FORYC`, and `FORY_PYTHON` overrides first,
then existing executables under `$HOME/.local/bin`, then PATH. Cap'n Proto
uses the user-local candidate or `capnp` on PATH. `--protoc PATH` overrides
Protobuf selection so the compiler can match the linked runtime. Subprocesses
use `execvp`; Fory's Python must be the interpreter containing its compiler
package. `./build.py setup` installs that interpreter in a dedicated venv.
Apache Fory's installed Python compiler package supplies the FDL AST and C++ type
resolver; no source-text regular expressions determine service declarations.
Fory native message headers reside in `generated/cpp/`.

Cap'n Proto contracts consume the compiler's `CodeGeneratorRequest`. Standard
Cap'n Proto methods are unary. Declare optional method annotations to express
the other gRPC streaming shapes:

```capnp
annotation grpcxxClientStreaming(method) :Bool;
annotation grpcxxServerStreaming(method) :Bool;
interface Orders {
  exchange @0 (request :Order) -> (response :Ack)
      $grpcxxClientStreaming(true) $grpcxxServerStreaming(true);
}
```

Cap'n Proto method parameter/result wrapper types retain their native wire
layout, and contracts preserve original wire method names in annotations.
Generic, inherited, or nested Cap'n Proto interfaces are rejected explicitly. Add the
Cap'n Proto include directory with `-I` if the installed compiler cannot find
`/capnp/c++.capnp`. Include both the generated directory and framework `src/`
directory when compiling generated headers with C++26 reflection enabled.

`grpcxx_generate_contracts(target FILES ... OUTPUT_DIRECTORY ...
IMPORT_DIRECTORIES ... PROTOC ...)` creates a CMake generation target. Runtime
codec feature macros do not control these build-time executables. Contracts use
the framework's canonical reflection annotations when available; the small
fallback annotation definitions support isolated codec builds.
