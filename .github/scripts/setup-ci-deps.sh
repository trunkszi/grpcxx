#!/usr/bin/env bash
# Install the reflection compiler and its matching native C++ dependencies.
set -euo pipefail

prefix="$HOME/.local/grpcxx-deps/gcc-glibc"
compiler_root="$HOME/.local/toolchain/gcc-glibc"
compiler="$compiler_root/bin/x86_64-unknown-linux-gnu-g++"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/grpcxx-deps"
jobs="${GRPCXX_SETUP_JOBS:-2}"
fory_commit=99fd6b3a4d23ca78f99070d3fe8555bb8fa48afa
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$prefix" "$cache_root"

fetch() {
    curl --fail --location --retry 3 --connect-timeout 30 --output "$2" "$1"
}

install_compiler() {
    if [[ -x "$compiler" ]]; then
        "$compiler" --version | head -n 1
        return
    fi
    local release="$cache_root/gcc-release.json"
    local asset=x86_64-unknown-linux-gnu-ubuntu22.tar.xz
    fetch https://api.github.com/repos/Central-Limit-Trading/gnu-cross/releases/tags/v0.0.1 "$release"
    local url
    url="$(python3 - "$release" "$asset" <<'PY'
import json, sys
with open(sys.argv[1]) as stream:
    release = json.load(stream)
print(next(asset['browser_download_url'] for asset in release['assets'] if asset['name'] == sys.argv[2]))
PY
)"
    fetch "$url" "$cache_root/$asset"
    fetch "$url.sha256" "$cache_root/$asset.sha256"
    local expected
    expected="$(awk '{print $1}' "$cache_root/$asset.sha256")"
    printf '%s  %s\n' "$expected" "$cache_root/$asset" | sha256sum --check
    local unpack
    unpack="$(mktemp -d "$cache_root/compiler.XXXXXX")"
    tar -xJf "$cache_root/$asset" -C "$unpack"
    local executable
    executable="$(find "$unpack" -type f -path '*/bin/x86_64-unknown-linux-gnu-g++' -print -quit)"
    if [[ -z "$executable" ]]; then
        echo 'Error: compiler archive has no expected GCC executable.' >&2
        return 1
    fi
    mkdir -p "$compiler_root"
    cp -a "${executable%/bin/*}/." "$compiler_root/"
    "$compiler" --version | head -n 1
}

build_archive() {
    local name="$1" url="$2" marker="$3"
    shift 3
    if [[ -f "$prefix/$marker" ]]; then
        printf '[INFO] %s already installed\n' "$name"
        return
    fi
    local source="$cache_root/$name"
    mkdir -p "$source"
    fetch "$url" "$cache_root/$name.archive"
    tar -xf "$cache_root/$name.archive" --strip-components=1 -C "$source"
    cmake -S "$source" -B "$source/build" -G Ninja \
        -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_SYSROOT=/ \
        '-DCMAKE_CXX_FLAGS=-B/usr/lib/x86_64-linux-gnu -idirafter /usr/include/x86_64-linux-gnu' \
        '-DCMAKE_EXE_LINKER_FLAGS=-Wl,--sysroot=/ -L/usr/lib/x86_64-linux-gnu -Wl,-rpath-link,/usr/lib/x86_64-linux-gnu -Wl,--dynamic-linker=/lib64/ld-linux-x86-64.so.2' \
        "$@"
    cmake --build "$source/build" --parallel "$jobs"
    cmake --install "$source/build"
}

install_fory() {
    local source="$cache_root/fory-$fory_commit"
    if [[ ! -f "$prefix/lib/cmake/fory/ForyConfig.cmake" ||
          ! -x "$HOME/.local/share/fory-venv/bin/foryc" ]]; then
        if [[ ! -d "$source/.git" ]]; then
            git init "$source"
            git -C "$source" remote add origin https://github.com/apache/fory.git
        fi
        git -C "$source" fetch --depth 1 origin "$fory_commit"
        git -C "$source" checkout --detach FETCH_HEAD
    fi
    if [[ ! -f "$prefix/lib/cmake/fory/ForyConfig.cmake" ]]; then
        cmake -S "$source/cpp" -B "$source/build-grpcxx" -G Ninja \
            -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_SYSROOT=/ \
            '-DCMAKE_CXX_FLAGS=-B/usr/lib/x86_64-linux-gnu -idirafter /usr/include/x86_64-linux-gnu' \
            -DFORY_BUILD_TESTS=OFF -DFORY_BUILD_SHARED=OFF -DFORY_BUILD_STATIC=ON \
            -DFORY_USE_AVX2=OFF -DFORY_WARNINGS_AS_ERRORS=OFF
        cmake --build "$source/build-grpcxx" --parallel "$jobs"
        cmake --install "$source/build-grpcxx"
    fi
    local venv="$HOME/.local/share/fory-venv"
    if [[ ! -x "$venv/bin/foryc" ]]; then
        uv venv --python 3.12 "$venv"
        uv pip install --python "$venv/bin/python3" "$source/compiler"
    fi
    ln -sfn "$venv/bin/foryc" "$HOME/.local/bin/foryc"
    ln -sfn "$venv/bin/python3" "$HOME/.local/bin/python3"
}

install_compiler
build_archive fmt-11.0.2 \
    https://github.com/fmtlib/fmt/archive/refs/tags/11.0.2.tar.gz \
    lib/cmake/fmt/fmt-config.cmake -DFMT_TEST=OFF -DFMT_DOC=OFF
build_archive googletest-1.15.2 \
    https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz \
    lib/cmake/GTest/GTestConfig.cmake -Dgtest_build_tests=OFF
build_archive benchmark-1.9.1 \
    https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz \
    lib/cmake/benchmark/benchmarkConfig.cmake \
    -DBENCHMARK_ENABLE_TESTING=OFF -DBENCHMARK_ENABLE_WERROR=OFF
build_archive nghttp2-1.64.0 \
    https://github.com/nghttp2/nghttp2/releases/download/v1.64.0/nghttp2-1.64.0.tar.xz \
    lib/libnghttp2.a -DENABLE_LIB_ONLY=ON -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF
install_fory
