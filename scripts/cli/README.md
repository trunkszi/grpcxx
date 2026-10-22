# grpcxx development CLI

Run `./build.py` from the repository root or invoke it by absolute path from
another directory. The Bash launcher adds user-local tools to PATH, selects the
cached CMake 3.31.10 when the installed version is older than 3.30, downloads UV
from Astral when it is missing, and launches this package through `uv run`.
UV manages Python 3.12, `.venv/`, and the locked dependencies automatically.

```sh
./build.py                         # Configure and build using gcc-glibc
./build.py build -t order_service -j 4
./build.py --preset gcc-glibc test
./build.py test -t test_core_types
./build.py test --no-build -R test_core_types -L unit
./build.py test --list
./build.py bench                   # bench_aligned_framing, 0.01 seconds
./build.py bench --list
./build.py bench -t bench_codecs
./build.py bench -t bench_order_pipeline -- --benchmark_min_time=0.001s
./build.py clean --preset gcc-glibc
./build.py format --check src
./build.py tidy src
./build.py setup
```

`--preset` and `-c/--compiler` work before or after the subcommand. The compiler
aliases `gcc`, `clang`, `musl-gcc`, and `musl-clang` select the corresponding
presets in `CMakePresets.json`. Exact preset names, including `gcc-glibc-vcpkg`,
are also accepted. Conflicting preset/compiler selections fail before any build.
`-j/--jobs` controls build concurrency; `test` also accepts `--parallel`.

`test --list` configures without compiling. Discovered cases may be marked
not built until their executables exist. `-t` selects cases by their actual
CTest command executable. Regex filters match CTest names or executable names,
so `--filter test_core_types` works with GoogleTest discovery. Empty execution
selections fail. `--no-build` never configures or compiles.

`bench_codecs` is the CLI alias for the CMake target `bench_multi_codec`.
`bench --list` displays both names without configuring or building anything.
The default benchmark and its minimum duration preserve the previous CLI behavior.

Use the global `--format json` option for one JSON result containing success,
exit code, elapsed seconds, events, and captured child stdout/stderr:

```sh
./build.py --format json test --no-build --filter test_core_types
```

Formatting uses clang-format 18 when installed, matching the Ubuntu 24.04 CI.
Text output uses Rich; commands execute through Invoke with quoted argv.
`--verbose` enables verbose CMake build and CTest output. Child failures retain
their exit codes. Formatting and cleanup stay inside the current repository;
cleanup refuses redirected build directories.

`setup` installs user-local UV, Ninja 1.13.2, CMake 3.31.10, the GCC reflection
compiler, and ABI-matched fmt, GoogleTest, Google Benchmark, nghttp2, and pinned
Fory dependencies. Missing system prerequisites are installed through APT. `setup --vcpkg` additionally bootstraps
vcpkg without resetting existing checkouts. The default preset uses `$HOME/.local/toolchain/gcc-glibc` and
`$HOME/.local/grpcxx-deps/gcc-glibc`. Fory tooling uses a dedicated venv under
`$HOME/.local/share/fory-venv`; `PROTOC`, `FORYC`, and `FORY_PYTHON` override
code-generation tools when needed.

```sh
uv run --directory scripts/cli ruff check .
uv run --directory scripts/cli pytest
```
