#!/usr/bin/env bash
# Reference CI toolchain + $HOME/.local/vcpkg missing-port provisioner.
# NEVER overwrites a non-empty $HOME/.local/vcpkg/<port> directory on developer hosts.
set -euo pipefail

detect_distro_tag() {
    local distro="ubuntu22"
    if [[ -r /etc/os-release ]]; then
        local id="" id_like=""
        id="$(awk -F= '$1=="ID"{gsub(/"/,"",$2); print $2}' /etc/os-release)"
        id_like="$(awk -F= '$1=="ID_LIKE"{gsub(/"/,"",$2); print $2}' /etc/os-release)"
        local identity=" ${id} ${id_like} "
        if [[ "$identity" == *" alinux "* ]]; then distro="alinux3"
        elif [[ "$identity" == *" amzn "* ]]; then distro="amazonlinux2"
        elif [[ "$identity" =~ [[:space:]](rhel|rocky|centos|almalinux|fedora)[[:space:]] ]]; then distro="centos7"
        fi
    fi
    printf '%s\n' "$distro"
}

fetch() {
    if [[ -s "$2" ]]; then return 0; fi
    curl --fail --location --retry 3 --connect-timeout 30 --output "$2" "$1"
}

install_one_toolchain() {
    local tc="$1"
    local root="${TOOLCHAIN_ROOT:-$HOME/.local/toolchain}/$tc"
    local cache_dir="${XDG_CACHE_HOME:-$HOME/.cache}/toolchains"
    local compiler_name="" compiler_repo="" target_triple=""
    case "$tc" in
        gcc-glibc) compiler_name=x86_64-unknown-linux-gnu-g++; compiler_repo=gcc-cross; target_triple=x86_64-unknown-linux-gnu ;;
        clang-glibc) compiler_name=clang++; compiler_repo=clang-cross; target_triple=x86_64-unknown-linux-gnu ;;
        gcc-musl) compiler_name=x86_64-unknown-linux-musl-g++; compiler_repo=gcc-cross; target_triple=x86_64-unknown-linux-musl ;;
        clang-musl) compiler_name=x86_64-unknown-linux-musl-clang++; compiler_repo=clang-cross; target_triple=x86_64-unknown-linux-musl ;;
        *) echo "Unsupported toolchain: $tc" >&2; return 2 ;;
    esac
    if [[ ! -x "$root/bin/$compiler_name" ]]; then
        mkdir -p "$cache_dir"
        local distro_tag asset url unpack executable
        distro_tag="${TOOLCHAIN_DISTRO:-$(detect_distro_tag)}"
        asset="${target_triple}-${distro_tag}.tar.xz"
        url="https://github.com/trunkszi/${compiler_repo}/releases/download/v0.0.1/${asset}"
        fetch "$url" "$cache_dir/${tc}-${asset}"
        fetch "${url}.sha256" "$cache_dir/${tc}-${asset}.sha256"
        printf '%s  %s\n' "$(awk '{print $1}' "$cache_dir/${tc}-${asset}.sha256")" "$cache_dir/${tc}-${asset}" | sha256sum --check
        unpack="$(mktemp -d "$cache_dir/unpack.XXXXXX")"
        tar -xJf "$cache_dir/${tc}-${asset}" -C "$unpack"
        chmod -R u+w "$unpack"
        executable="$(find "$unpack" -path "*/bin/$compiler_name" -print -quit)"
        mkdir -p "$root"
        cp -a "${executable%/bin/*}/." "$root/"
        chmod -R u+w "$root"
        rm -rf "$unpack"
    fi
    if [[ "$tc" == clang-glibc ]]; then
        local sysroot="$root/x86_64-unknown-linux-gnu/sysroot"
        chmod u+w "$root" "$root/bin" "$root/bin/clang.cfg" "$root/bin/clang++.cfg" 2>/dev/null || true
        printf '%s\n' "--sysroot=$sysroot" "--gcc-toolchain=$root" > "$root/bin/clang.cfg"
        printf '%s\n' "--sysroot=$sysroot" "--gcc-toolchain=$root" '-stdlib=libc++' \
            "-Wl,-rpath,$sysroot/usr/lib" "-Wl,-rpath,$root/lib/x86_64-unknown-linux-gnu" \
            > "$root/bin/clang++.cfg"
    fi
    "$root/bin/$compiler_name" --version | head -n 1
}

ensure_uarch_port() {
    local overlay="$1" sha="${UARCH_COMMIT:-9bb51fa7ef3ce69745f076595e3ccc1eb31e50d3}"
    [[ -n "$(ls -A "$overlay/uarch" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/uarch"
    cat > "$overlay/uarch/vcpkg.json" <<'EOF'
{
  "name": "uarch",
  "version": "0.1.0",
  "description": "Runtime-free C++26 microarchitectural primitives and compile-time TMA layout contracts",
  "homepage": "https://github.com/trunkszi/uarch",
  "license": "MIT",
  "supports": "linux & x64",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true }
  ]
}
EOF
    cat > "$overlay/uarch/portfile.cmake" <<EOF
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/trunkszi/uarch.git" REF "${sha}" FETCH_REF "main" HEAD_REF "main")
file(REMOVE "\${SOURCE_PATH}/vcpkg.json" "\${SOURCE_PATH}/vcpkg-configuration.json")
vcpkg_cmake_configure(SOURCE_PATH "\${SOURCE_PATH}" OPTIONS -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_INSTALL_LIBDIR=lib -DUARCH_AUTO_DOWNLOAD_TOOLCHAIN=OFF -DUARCH_BUILD_TESTS=OFF -DUARCH_BUILD_EXAMPLES=OFF -DUARCH_BUILD_BENCHES=OFF -DUARCH_BUILD_TOOLS=OFF -DUARCH_ENABLE_SANITIZERS=OFF -DUARCH_INSTALL_PACKAGE=ON)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME uarch CONFIG_PATH lib/cmake/uarch)
file(REMOVE_RECURSE "\${CURRENT_PACKAGES_DIR}/debug")
vcpkg_install_copyright(FILE_LIST "\${SOURCE_PATH}/LICENSE")
EOF
}

ensure_uring_port() {
    local overlay="$1" sha="${URING_COMMIT:-a8790889d71716a369d07e302b4e1a3f2934ea32}"
    [[ -n "$(ls -A "$overlay/uring" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/uring"
    cat > "$overlay/uring/vcpkg.json" <<'EOF'
{
  "name": "uring",
  "version-semver": "0.1.0",
  "description": "C++26 runtime-free io_uring and futex2 proactor framework",
  "homepage": "https://github.com/trunkszi/uring",
  "license": "MIT",
  "supports": "linux & x64",
  "dependencies": [
    "uarch",
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true }
  ]
}
EOF
    cat > "$overlay/uring/portfile.cmake" <<EOF
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/trunkszi/uring.git" REF "${sha}" FETCH_REF "main" HEAD_REF "main")
file(REMOVE "\${SOURCE_PATH}/vcpkg.json" "\${SOURCE_PATH}/vcpkg-configuration.json")
vcpkg_cmake_configure(SOURCE_PATH "\${SOURCE_PATH}" OPTIONS -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_INSTALL_LIBDIR=lib -DURING_AUTO_DOWNLOAD_TOOLCHAIN=OFF -DURING_BUILD_TESTS=OFF -DURING_BUILD_EXAMPLES=OFF -DURING_BUILD_BENCHES=OFF -DURING_BUILD_TOOLS=OFF -DURING_ENABLE_SANITIZERS=OFF -DURING_INSTALL_PACKAGE=ON)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME uring CONFIG_PATH lib/cmake/uring)
file(REMOVE_RECURSE "\${CURRENT_PACKAGES_DIR}/debug")
vcpkg_install_copyright(FILE_LIST "\${SOURCE_PATH}/LICENSE")
EOF
}

ensure_c_ares_single_thread_port() {
    local overlay="$1"
    [[ -n "$(ls -A "$overlay/c-ares" 2>/dev/null)" ]] && return 0
    local vcpkg_root="${VCPKG_ROOT:-$HOME/.vcpkg}"
    if [[ -d "$vcpkg_root/ports/c-ares" ]]; then
        mkdir -p "$overlay/c-ares"
        cp -a "$vcpkg_root/ports/c-ares/." "$overlay/c-ares/"
        python3 - "$overlay/c-ares/portfile.cmake" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
t = p.read_text()
if "-DCARES_THREADS=" not in t:
    t = t.replace("-DCARES_BUILD_TESTS=OFF", "-DCARES_THREADS=OFF\n        -DCARES_BUILD_TESTS=OFF", 1)
    p.write_text(t)
PY
    fi
}

ensure_cares_port() {
    local overlay="$1" sha="${CARES_COMMIT:-b46378d1977aa73dabf6c52535f61419d0b48063}"
    [[ -n "$(ls -A "$overlay/cares" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/cares"
    cat > "$overlay/cares/vcpkg.json" <<'EOF'
{
  "name": "cares",
  "version-semver": "0.1.0",
  "description": "Runtime-free C++26 asynchronous DNS resolver for Linux io_uring Proactor loops",
  "homepage": "https://github.com/trunkszi/cares",
  "license": "MIT",
  "supports": "linux & x64",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true },
    { "name": "c-ares", "default-features": false },
    "uring",
    "uarch"
  ]
}
EOF
    cat > "$overlay/cares/portfile.cmake" <<EOF
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/trunkszi/cares.git" REF "${sha}" FETCH_REF "main" HEAD_REF "main")
file(REMOVE "\${SOURCE_PATH}/vcpkg.json" "\${SOURCE_PATH}/vcpkg-configuration.json")
vcpkg_cmake_configure(SOURCE_PATH "\${SOURCE_PATH}" OPTIONS -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_INSTALL_LIBDIR=lib -DCARES_BUILD_TESTS=OFF -DCARES_BUILD_EXAMPLES=OFF -DCARES_BUILD_BENCHES=OFF -DCARES_BUILD_TOOLS=OFF -DCARES_ENABLE_SANITIZERS=OFF -DCARES_INSTALL_PACKAGE=ON)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME cares CONFIG_PATH lib/cmake/cares)
file(REMOVE_RECURSE "\${CURRENT_PACKAGES_DIR}/debug" "\${CURRENT_PACKAGES_DIR}/lib")
vcpkg_install_copyright(FILE_LIST "\${SOURCE_PATH}/LICENSE")
EOF
}

ensure_aws_lc_port() {
    local overlay="$1"
    [[ -n "$(ls -A "$overlay/aws-lc" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/aws-lc"
    cat > "$overlay/aws-lc/vcpkg.json" <<'EOF'
{
  "name": "aws-lc",
  "version": "5.12.0",
  "description": "AWS-LC cryptographic and TLS library providing OpenSSL-compatible SSL and crypto APIs",
  "homepage": "https://github.com/aws/aws-lc",
  "license": "Apache-2.0",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true }
  ]
}
EOF
    cat > "$overlay/aws-lc/portfile.cmake" <<'EOF'
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO aws/aws-lc
  REF cd481c207e38f06aeb5766f4a1cdfce10a4c0af8
  SHA512 d5dae38d6dd76eeacd5d1a5c6f1f5686f8e7f58e01ef5e3911ef4beefd6158c329a519ec4b7cad257009f3aa0202137b00d7d435cf5c68551f1b91e60bf94d6c
)
vcpkg_replace_string("${SOURCE_PATH}/CMakeLists.txt"
  "if(BORINGSSL_ALLOW_CXX_RUNTIME)"
  "if(NOT BORINGSSL_ALLOW_CXX_RUNTIME)\n  add_compile_definitions(_GLIBCXX_NO_ASSERTIONS)\nendif()\nif(BORINGSSL_ALLOW_CXX_RUNTIME)")
vcpkg_replace_string("${SOURCE_PATH}/CMakeLists.txt" "-Werror -Wformat=2" "-Wformat=2")
vcpkg_cmake_configure(
  SOURCE_PATH "${SOURCE_PATH}"
  OPTIONS -DBUILD_TESTING=OFF -DBUILD_TOOL=OFF -DDISABLE_PERL=ON -DDISABLE_GO=ON -DBUILD_LIBSSL=ON
)
vcpkg_cmake_install()
vcpkg_fixup_pkgconfig()
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include" "${CURRENT_PACKAGES_DIR}/debug/share")
file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/share/openssl")
file(WRITE "${CURRENT_PACKAGES_DIR}/share/openssl/vcpkg-cmake-wrapper.cmake" [=[
get_filename_component(_AWS_LC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
set(OPENSSL_ROOT_DIR "${_AWS_LC_ROOT}")
set(OPENSSL_INCLUDE_DIR "${_AWS_LC_ROOT}/include")
set(OPENSSL_CRYPTO_LIBRARY "${_AWS_LC_ROOT}/lib/libcrypto.a")
set(OPENSSL_SSL_LIBRARY "${_AWS_LC_ROOT}/lib/libssl.a")
_find_package(${ARGS})
find_package(Threads QUIET)
if(TARGET OpenSSL::Crypto AND TARGET Threads::Threads)
  set_property(TARGET OpenSSL::Crypto APPEND PROPERTY INTERFACE_LINK_LIBRARIES Threads::Threads dl)
endif()
]=])
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
EOF
}

ensure_nexora_port() {
    local overlay="$1" sha="${NEXORA_COMMIT:-f11700d5c9e8401bd5370871ea0a46c9146994b5}"
    [[ -n "$(ls -A "$overlay/nexora" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/nexora"
    cat > "$overlay/nexora/vcpkg.json" <<'EOF'
{
  "name": "nexora",
  "version-semver": "0.1.0",
  "description": "C++26 Sans-IO protocols and Proactor transport",
  "homepage": "https://github.com/trunkszi/nexora",
  "license": "MIT",
  "supports": "linux & x64",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true },
    "uarch",
    "uring",
    "cares",
    "aws-lc"
  ]
}
EOF
    cat > "$overlay/nexora/portfile.cmake" <<EOF
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/trunkszi/nexora.git" REF "${sha}" FETCH_REF "main" HEAD_REF "main")
file(REMOVE "\${SOURCE_PATH}/vcpkg.json" "\${SOURCE_PATH}/vcpkg-configuration.json")
vcpkg_cmake_configure(SOURCE_PATH "\${SOURCE_PATH}" OPTIONS -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_INSTALL_LIBDIR=lib -DNEXORA_BUILD_TESTS=OFF -DNEXORA_BUILD_EXAMPLES=OFF -DNEXORA_BUILD_BENCHES=OFF -DNEXORA_BUILD_TOOLS=OFF -DNEXORA_ENABLE_SANITIZERS=OFF -DNEXORA_INSTALL_PACKAGE=ON)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME nexora CONFIG_PATH lib/cmake/nexora)
file(REMOVE_RECURSE "\${CURRENT_PACKAGES_DIR}/debug" "\${CURRENT_PACKAGES_DIR}/lib")
vcpkg_install_copyright(FILE_LIST "\${SOURCE_PATH}/LICENSE")
EOF
}

ensure_abseil_port() {
    local overlay="$1"
    [[ -n "$(ls -A "$overlay/abseil" 2>/dev/null)" ]] && return 0
    local vcpkg_root="${VCPKG_ROOT:-$HOME/.vcpkg}"
    if [[ -d "$vcpkg_root/ports/abseil" ]]; then
        mkdir -p "$overlay/abseil"
        cp -a "$vcpkg_root/ports/abseil/." "$overlay/abseil/"
        python3 - "$overlay/abseil/portfile.cmake" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
t = p.read_text()
needle = 'vcpkg_cmake_configure('
patch = '''vcpkg_replace_string("${SOURCE_PATH}/absl/container/internal/hash_policy_traits.h"
  "    return Policy::template get_hash_slot_fn<Hash, kIsDefault>() == nullptr"
  "#if defined(__GNUC__) && !defined(__clang__) && defined(__SANITIZE_ADDRESS__)\\n    if (__builtin_is_constant_evaluated()) {\\n      return &hash_slot_fn_non_type_erased<Hash, kIsDefault>;\\n    }\\n#endif\\n    return Policy::template get_hash_slot_fn<Hash, kIsDefault>() == nullptr"
  IGNORE_UNCHANGED)
'''
if 'hash_policy_traits.h' not in t and needle in t:
    t = t.replace(needle, patch + needle, 1)
    p.write_text(t)
PY
    fi
}

ensure_protobuf_port() {
    local overlay="$1"
    [[ -n "$(ls -A "$overlay/protobuf" 2>/dev/null)" ]] && return 0
    local vcpkg_root="${VCPKG_ROOT:-$HOME/.vcpkg}"
    if [[ -d "$vcpkg_root/ports/protobuf" ]] && ! grep -q '"libprotoc"' "$vcpkg_root/ports/protobuf/vcpkg.json" 2>/dev/null; then
        mkdir -p "$overlay/protobuf"
        cp -a "$vcpkg_root/ports/protobuf/." "$overlay/protobuf/"
        python3 - "$overlay/protobuf/vcpkg.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text())
features = data.setdefault("features", {})
features.setdefault("libprotoc", {
    "description": "Build the libprotoc compiler library for target builds.",
    "supports": "!uwp",
})
p.write_text(json.dumps(data, indent=2) + "\n")
PY
    fi
}

ensure_fory_port() {
    local overlay="$1"
    [[ -n "$(ls -A "$overlay/fory" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/fory"
    cat > "$overlay/fory/vcpkg.json" <<'EOF'
{
  "name": "fory",
  "version": "1.8.0",
  "description": "Apache Fory multi-language serialization framework (C++ runtime)",
  "homepage": "https://fory.apache.org",
  "license": "Apache-2.0",
  "supports": "x64 & linux",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true }
  ]
}
EOF
    cat > "$overlay/fory/portfile.cmake" <<'EOF'
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/apache/fory.git" REF "99fd6b3a4d23ca78f99070d3fe8555bb8fa48afa" FETCH_REF "main" HEAD_REF "main")
vcpkg_replace_string("${SOURCE_PATH}/cpp/fory/util/logging.cc" "#if !defined(_WIN32)" "#if !defined(_WIN32) && defined(__GLIBC__)")
vcpkg_cmake_configure(SOURCE_PATH "${SOURCE_PATH}/cpp" OPTIONS -DFORY_BUILD_TESTS=OFF -DFORY_BUILD_SHARED=OFF -DFORY_BUILD_STATIC=ON -DFORY_USE_AVX2=OFF -DFORY_WARNINGS_AS_ERRORS=OFF)
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME Fory CONFIG_PATH lib/cmake/fory)
vcpkg_fixup_pkgconfig()
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
EOF
}

ensure_grpcxx_port() {
    local overlay="$1" sha="${GRPCXX_COMMIT:-448c929afc9d44b1bf0eb7b00475de92695a1346}"
    [[ -n "$(ls -A "$overlay/grpcxx" 2>/dev/null)" ]] && return 0
    mkdir -p "$overlay/grpcxx"
    cat > "$overlay/grpcxx/vcpkg.json" <<'EOF'
{
  "name": "grpcxx",
  "version-semver": "0.8.0",
  "description": "Runtime-free C++26 gRPC client and server framework with P2996 compile-time reflection",
  "homepage": "https://github.com/trunkszi/grpcxx",
  "license": "MIT",
  "supports": "linux & x64",
  "dependencies": [
    { "name": "vcpkg-cmake", "host": true },
    { "name": "vcpkg-cmake-config", "host": true },
    "uarch",
    "uring",
    "cares",
    "nexora",
    "fmt",
    "zlib",
    "zstd",
    "lz4",
    "snappy",
    { "name": "protobuf", "features": ["libprotoc"] }
  ],
  "features": {
    "capnproto": { "description": "Enable Cap'n Proto codec and contract code generator", "dependencies": ["capnproto"] },
    "fory": { "description": "Enable Apache Fory codec", "dependencies": ["fory"] }
  }
}
EOF
    cat > "$overlay/grpcxx/portfile.cmake" <<EOF
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
set(VCPKG_BUILD_TYPE release)
vcpkg_from_git(OUT_SOURCE_PATH SOURCE_PATH URL "https://github.com/trunkszi/grpcxx.git" REF "${sha}" FETCH_REF "main" HEAD_REF "main")
file(REMOVE "\${SOURCE_PATH}/vcpkg.json" "\${SOURCE_PATH}/vcpkg-configuration.json")
vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS FEATURES capnproto GRPCXX_ENABLE_CAPNP fory GRPCXX_ENABLE_FORY)
vcpkg_cmake_configure(SOURCE_PATH "\${SOURCE_PATH}" OPTIONS -DVCPKG_MANIFEST_MODE=OFF -DCMAKE_INSTALL_LIBDIR=lib -DGRPCXX_BUILD_TESTING=OFF -DGRPCXX_BUILD_BENCHMARKS=OFF -DGRPCXX_BUILD_EXAMPLES=OFF -DGRPCXX_ENABLE_PROTOBUF=ON \${FEATURE_OPTIONS})
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME grpcxx CONFIG_PATH lib/cmake/grpcxx)
file(REMOVE_RECURSE "\${CURRENT_PACKAGES_DIR}/debug")
vcpkg_copy_tools(TOOL_NAMES protoc-gen-grpcxx AUTO_CLEAN)
if("capnproto" IN_LIST FEATURES)
  vcpkg_copy_tools(TOOL_NAMES grpcxx-codegen AUTO_CLEAN)
endif()
vcpkg_install_copyright(FILE_LIST "\${SOURCE_PATH}/LICENSE")
EOF
}

main() {
    mkdir -p "$HOME/.cache/vcpkg/archives" "$HOME/.cache/ccache"
    local toolchain="${GRPCXX_TOOLCHAIN:-${1:-gcc-glibc}}"
    case "$toolchain" in
        gcc-glibc|clang-glibc|gcc-musl|clang-musl) ;;
        *) echo "Unsupported toolchain: $toolchain" >&2; return 2 ;;
    esac
    install_one_toolchain "$toolchain"
    if [[ "$toolchain" != gcc-glibc ]]; then
        install_one_toolchain gcc-glibc
    fi
    local overlay="${VCPKG_OVERLAY_PORTS:-$HOME/.local/vcpkg}"
    mkdir -p "$overlay"
    ensure_uarch_port "$overlay"
    ensure_uring_port "$overlay"
    ensure_c_ares_single_thread_port "$overlay"
    ensure_cares_port "$overlay"
    ensure_aws_lc_port "$overlay"
    ensure_nexora_port "$overlay"
    ensure_abseil_port "$overlay"
    ensure_protobuf_port "$overlay"
    ensure_fory_port "$overlay"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
