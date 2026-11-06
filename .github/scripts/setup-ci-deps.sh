#!/usr/bin/env bash
# Install the reflection compiler and its matching native C++ dependencies.
set -euo pipefail

toolchain="${GRPCXX_TOOLCHAIN:-gcc-glibc}"
case "$toolchain" in
    gcc-glibc) compiler_name=x86_64-unknown-linux-gnu-g++; compiler_repo=gnu-cross ;;
    clang-glibc) compiler_name=clang++; compiler_repo=clang-cross ;;
    *) echo "Unsupported native dependency toolchain: $toolchain" >&2; exit 2 ;;
esac
prefix="${GRPCXX_DEPS_PREFIX:-$HOME/.local/grpcxx-deps/$toolchain}"
dependency_profile="${GRPCXX_DEPENDENCY_PROFILE:-native}"
if [[ "$dependency_profile" == asan-ubsan && -z "${GRPCXX_DEPS_PREFIX:-}" ]]; then prefix="$prefix/asan"; fi
compiler_root="$HOME/.local/toolchain/$toolchain"
compiler="$compiler_root/bin/$compiler_name"
sysroot=/
c_compiler=/usr/bin/cc
c_flags='-B/usr/lib/x86_64-linux-gnu -idirafter /usr/include/x86_64-linux-gnu -idirafter /usr/include'
cxx_flags="$c_flags"
link_flags='-Wl,--sysroot=/ -L/usr/lib/x86_64-linux-gnu -Wl,-rpath-link,/usr/lib/x86_64-linux-gnu -Wl,--dynamic-linker=/lib64/ld-linux-x86-64.so.2'
if [[ "$toolchain" == clang-glibc ]]; then
    sysroot="$compiler_root/x86_64-unknown-linux-gnu/sysroot"
    c_compiler="$compiler_root/bin/clang"
    cxx_flags="-stdlib=libc++ $c_flags"
    link_flags="$link_flags --rtlib=compiler-rt --unwindlib=libunwind -Wl,-rpath,$compiler_root/x86_64-unknown-linux-gnu/sysroot/usr/lib"
fi
uarch_commit="${GRPCXX_UARCH_COMMIT:-2638d2befb165ead3d47e09f9a760814aded4287}"
liburing_commit="${GRPCXX_LIBURING_COMMIT:-28050792434b45b6b870fdfdd1092f67cdebb9e5}"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/grpcxx-deps/$toolchain"
dependency_cmake_flags=()
case "$dependency_profile" in
    native) ;;
    asan-ubsan)
        cache_root="$cache_root/asan"
        sanitizer_flags='-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer'
        c_flags="$c_flags $sanitizer_flags"
        cxx_flags="$cxx_flags $sanitizer_flags"
        link_flags="$link_flags $sanitizer_flags"
        dependency_cmake_flags=('-DCMAKE_CXX_FLAGS_RELEASE=-O1 -DNDEBUG' '-DCMAKE_C_FLAGS_RELEASE=-O1 -DNDEBUG')
        ;;
    *) echo "Unsupported dependency profile: $dependency_profile" >&2; exit 2 ;;
esac
jobs="${GRPCXX_SETUP_JOBS:-2}"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fory_commit=99fd6b3a4d23ca78f99070d3fe8555bb8fa48afa
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$prefix" "$cache_root"

fetch() {
    curl --fail --location --retry 3 --connect-timeout 30 --output "$2" "$1"
}

configure_clang_driver() {
    if [[ "$toolchain" != clang-glibc ]]; then return; fi
    chmod u+w "$compiler_root" "$compiler_root/bin" "$compiler_root/bin/clang.cfg" "$compiler_root/bin/clang++.cfg" 2>/dev/null || true
    printf '%s\n' "--sysroot=$sysroot" "--gcc-toolchain=$compiler_root" > "$compiler_root/bin/clang.cfg"
    printf '%s\n' "--sysroot=$sysroot" "--gcc-toolchain=$compiler_root" '-stdlib=libc++' \
        "-Wl,-rpath,$sysroot/usr/lib" "-Wl,-rpath,$compiler_root/lib/x86_64-unknown-linux-gnu" \
        > "$compiler_root/bin/clang++.cfg"
}

install_compiler() {
    if [[ -x "$compiler" ]]; then
        configure_clang_driver
        "$compiler" --version | head -n 1
        return
    fi
    local asset=x86_64-unknown-linux-gnu-ubuntu22.tar.xz
    local url="https://github.com/trunkszi/$compiler_repo/releases/download/v0.0.1/$asset"
    fetch "$url" "$cache_root/$asset"
    fetch "$url.sha256" "$cache_root/$asset.sha256"
    local expected
    expected="$(awk '{print $1}' "$cache_root/$asset.sha256")"
    printf '%s  %s\n' "$expected" "$cache_root/$asset" | sha256sum --check
    local unpack
    unpack="$(mktemp -d "$cache_root/compiler.XXXXXX")"
    tar -xJf "$cache_root/$asset" -C "$unpack"
    local executable
    executable="$(find "$unpack" -path "*/bin/$compiler_name" -print -quit)"
    if [[ -z "$executable" || ! -x "$executable" ]]; then
        echo "Error: compiler archive has no expected $compiler_name executable." >&2
        return 1
    fi
    mkdir -p "$compiler_root"
    cp -a "${executable%/bin/*}/." "$compiler_root/"
    chmod -R u+w "$compiler_root" "$unpack"
    rm -rf "$unpack"
    configure_clang_driver
    "$compiler" --version | head -n 1
}

build_archive() {
    local name="$1" url="$2" marker="$3"
    shift 3
    local patch="$project_root/.github/patches/$name.patch" patch_digest=""
    if [[ -f "$patch" ]]; then
        local patch_hash
        patch_hash="$(sha256sum "$patch" | awk '{print $1}')"
        patch_digest="$(printf '%s\n' "$patch_hash" "$c_flags" "$cxx_flags" "$link_flags" "$sysroot" \
            "${dependency_cmake_flags[@]}" "$@" |
            sha256sum | awk '{print $1}')"
    fi
    if [[ -f "$prefix/$marker" ]] &&
       [[ -z "$patch_digest" || "$(cat "$prefix/$marker")" == "$patch_digest" ]]; then
        printf '[INFO] %s already installed\n' "$name"
        return
    fi
    local source="$cache_root/$name"
    mkdir -p "$source"
    fetch "$url" "$cache_root/$name.archive"
    tar -xf "$cache_root/$name.archive" --strip-components=1 -C "$source"
    chmod -R u+w "$source"
    if [[ -n "$patch_digest" ]]; then git -C "$source" apply "$patch"; fi
    local policy_flags=()
    # Cap'n Proto 1.0.2 requests a compatibility level removed by CMake 4.
    if [[ "$name" == capnp-1.0.2 ]] &&
       (( $(cmake --version | awk 'NR==1 {split($3,v,"."); print v[1]}') >= 4 )); then
        policy_flags=(-DCMAKE_POLICY_VERSION_MINIMUM=3.10)
    fi
    cmake -S "$source" -B "$source/build" -G Ninja \
        -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_C_COMPILER="$c_compiler" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_SYSROOT="$sysroot" \
        "-DCMAKE_CXX_FLAGS=$cxx_flags" \
        "-DCMAKE_C_FLAGS=$c_flags" \
        "-DCMAKE_EXE_LINKER_FLAGS=$link_flags" \
        "${dependency_cmake_flags[@]}" "${policy_flags[@]}" "$@"
    cmake --build "$source/build" --parallel "$jobs"
    cmake --install "$source/build"
    if [[ -n "$patch_digest" ]]; then
        mkdir -p "$prefix/share/grpcxx-deps"
        printf '%s\n' "$patch_digest" > "$prefix/$marker"
    fi
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
            -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_C_COMPILER="$c_compiler" -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_SYSROOT="$sysroot" \
            "-DCMAKE_CXX_FLAGS=$cxx_flags" \
            "-DCMAKE_C_FLAGS=$c_flags" \
            "-DCMAKE_EXE_LINKER_FLAGS=$link_flags" \
            "${dependency_cmake_flags[@]}" \
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
}

install_native_package() {
    local name="$1" repository="$2" revision="$3"
    local source="$cache_root/$name-$revision"
    local stamp="$prefix/share/grpcxx-deps/$name.ref"
    if [[ -f "$stamp" && "$(cat "$stamp")" == "$revision" ]]; then return; fi
    if [[ ! "$revision" =~ ^[[:xdigit:]]{40}$ ]]; then
        echo "Expected an immutable commit for $name" >&2; return 1
    fi
    if [[ ! -d "$source/.git" ]]; then
        git init "$source"
        git -C "$source" remote add origin "$repository"
    fi
    git -C "$source" fetch --depth 1 origin "$revision"
    git -C "$source" checkout --detach FETCH_HEAD
    [[ "$(git -C "$source" rev-parse HEAD)" == "$revision" ]]
    cmake -S "$source" -B "$source/build" -G Ninja \
        -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_C_COMPILER="$c_compiler" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="$prefix" -DCMAKE_SYSROOT="$sysroot" \
        "-DCMAKE_CXX_FLAGS=$cxx_flags" -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        "-DCMAKE_C_FLAGS=$c_flags" \
        "-DCMAKE_EXE_LINKER_FLAGS=$link_flags" \
        "${dependency_cmake_flags[@]}" \
        -DUARCH_BUILD_TESTS=OFF -DUARCH_BUILD_BENCHES=OFF \
        -DLIBURING_BUILD_TESTS=OFF -DLIBURING_BUILD_BENCHES=OFF
    if [[ "$name" == uarch ]]; then
        cmake --build "$source/build" --target uarch_detect --parallel "$jobs"
    else
        cmake --build "$source/build" --parallel "$jobs"
    fi
    cmake --install "$source/build"
    mkdir -p "${stamp%/*}"
    printf '%s\n' "$revision" > "$stamp"
}

install_compiler
install_native_package uarch https://github.com/trunkszi/uarch.git "$uarch_commit"
install_native_package liburing-cpp https://github.com/trunkszi/liburing-cpp.git "$liburing_commit"
build_archive fmt-11.0.2 \
    https://github.com/fmtlib/fmt/archive/refs/tags/11.0.2.tar.gz \
    lib/cmake/fmt/fmt-config.cmake -DFMT_TEST=OFF -DFMT_DOC=OFF
build_archive googletest-1.15.2 \
    https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz \
    share/grpcxx-deps/googletest-character-cast.ref -Dgtest_build_tests=OFF
build_archive benchmark-1.9.1 \
    https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz \
    lib/cmake/benchmark/benchmarkConfig.cmake \
    -DBENCHMARK_ENABLE_TESTING=OFF -DBENCHMARK_ENABLE_WERROR=OFF
build_archive nghttp2-1.64.0 \
    https://github.com/nghttp2/nghttp2/releases/download/v1.64.0/nghttp2-1.64.0.tar.xz \
    lib/libnghttp2.a -DENABLE_LIB_ONLY=ON -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF
install_fory

# C++ libraries must use the same standard library as their consuming compiler.
if [[ "$toolchain" == clang-glibc ]]; then
    build_archive protobuf-21.12 \
        https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz \
        lib/libprotobuf.a -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_WITH_ZLIB=ON \
        -Dprotobuf_BUILD_SHARED_LIBS=OFF
    build_archive capnp-1.0.2 \
        https://github.com/capnproto/capnproto/archive/refs/tags/v1.0.2.tar.gz \
        share/grpcxx-deps/capnp-clang-casts.ref -DBUILD_TESTING=OFF -DWITH_FIBERS=OFF
fi

# libc++ container annotations must agree across archive and application boundaries.
if [[ "$toolchain" == clang-glibc && "$dependency_profile" == native ]]; then
    GRPCXX_DEPENDENCY_PROFILE=asan-ubsan GRPCXX_DEPS_PREFIX="$prefix/asan" bash "${BASH_SOURCE[0]}"
fi
