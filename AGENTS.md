# grpcxx — Shared Agent Guide

## Project overview

grpcxx is a single-threaded C++26 gRPC framework built around an `io_uring`
Proactor and caller-driven Run-to-Completion (RTC) services. P2996 reflection
(`^^T`, `[: :]`) and P3394 annotations (`[[= grpcxx::service{...}]]`,
`[[= grpcxx::rpc{...}]]`) describe typed RPC contracts.

This guide applies to Claude Code, Codex, Gemini, and human maintainers. The
current framework lives in `src/grpcxx/`; inspect its headers before changing an
API or documenting its behavior.

## Non-negotiable engineering invariants

- Drive `ProactorService` and `ServiceConcept` lifecycle methods from one owner
  thread. RTC callbacks finish on that caller; services create no internal
  threads or mutexes and perform no blocking syscalls on the hot path.
- Compile with global `-fno-exceptions`. Fallible operations return
  `std::expected<T, Status>`; explicit runtime allocations use `std::nothrow`
  and report failures. Keep allocation and ownership changes visible in review.
- Use the 64-byte `InplaceDelegate<Sig, 48>` for inline callbacks. Hot paths must
  not introduce `std::function` or hidden heap-backed callback storage.
- Model lifecycle and protocol transitions with `enum class` FSMs. Switches over
  states are exhaustive, have no `default:` branch, and compile with `-Wswitch`.
  Reject illegal transitions explicitly and count them in `rejected_transitions_`.
- Keep borrowed buffers and callback contexts alive through completion,
  cancellation, and shutdown drain. Handle partial writes and backpressure;
  never retry a message that was already accepted.
- Keep every `.h`, `.cc`, `CMakeLists.txt`, and `.py` file at most 800 lines;
  check with `wc -l` before completing a change.
- Build and test exclusively through `./build.py`. Read its help before choosing
  options; use existing options rather than inventing flags or bypassing the CLI.

## Quick start

`build.py` is an executable Bash launcher for a UV-managed Typer/Rich/Invoke
package, not a Python script. It bootstraps UV when missing. Presets select the
reflection compiler and dependency prefixes; provision those before compiling.

```sh
./build.py --help
./build.py build --help
./build.py build
./build.py test
./build.py bench
./build.py clean
./build.py format --help
./build.py tidy --help
./build.py setup --help
./build.py --format json test --no-build --filter test_core_types
```

Bare `./build.py` selects `build`; the default preset is `gcc-glibc`. Read
[scripts/cli/README.md](scripts/cli/README.md) when selecting another compiler,
filtering tests, running a benchmark target, or using JSON output. Formatting
can modify files; scope it to the files authorized for the task.

## Repository architecture

| Path | Responsibility |
| --- | --- |
| `src/grpcxx/core/` | Status, metadata, inline delegates, lifecycle concepts, timeout encoding |
| `src/grpcxx/framing/` | Aligned message storage and incremental gRPC frame parsing |
| `src/grpcxx/reflection/` | Annotated contracts, contexts, typed routing, stubs, unary responders |
| `src/grpcxx/h2/` | Caller-driven nghttp2 unary sessions |
| `src/grpcxx/uring/` | Proactor, ring/socket/acceptor wrappers, operation and timer lifetimes |
| `src/grpcxx/client/`, `src/grpcxx/server/` | Plaintext unary client/server services on the Proactor |
| `src/grpcxx/channel/` | Connectivity FSM, deadlines, endpoint selection, coordinator |
| `src/grpcxx/dns/` | Caller-polled c-ares resolution |
| `src/grpcxx/interceptor/` | Compile-time interceptor chains and call metadata |
| `src/grpcxx/services/` | Bounded health and reflected-contract registries |
| `src/grpcxx/streaming/` | Stream slots, queues, half-close, cancellation, typed adapters |
| `src/grpcxx/codec/` | Protobuf, Apache Fory, Cap'n Proto payload adapters |
| `src/grpcxx/compression/`, `src/grpcxx/tls/` | Bounded compression and TLS 1.3 memory-BIO wrappers |
| `src/grpcxx/tools/` | IDL front end and Protobuf contract generator |
| `examples/` | Applications registered by the current root CMake build |
| `tests/`, `benches/` | Current tests and benchmark suites |
| `scripts/cli/` | Build/test/development CLI and its Python tests |
| `docs/` | Architecture, API, and contract-generation references |

## Context pointers

- Read [docs/README.md](docs/README.md) before changing service composition,
  framing, or transport boundaries.
- Read [docs/api.md](docs/api.md) when implementing callers or changing public
  types; confirm names and ownership rules in the linked headers.
- Read [docs/protoc-gen-grpcxx.md](docs/protoc-gen-grpcxx.md) before changing IDL
  generation, generated contracts, or their CMake dependency graph.
- Read [scripts/cli/README.md](scripts/cli/README.md) before changing build
  commands, presets, command execution, or machine-readable output.

After a change, run the affected CLI checks and report the commands and their
results. Separate source/API checks from runtime evidence and performance
measurements; passing a smoke benchmark is not a latency guarantee.
