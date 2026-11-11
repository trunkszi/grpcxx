#!/usr/bin/env bash
# Install the user-local tools needed by the grpcxx CLI across all four toolchains.
set -euo pipefail

info() { printf '[INFO] %s\n' "$*"; }
version_ge() { printf '%s\n%s\n' "$2" "$1" | sort -V -C; }
ensure_path_entry() {
    case ":$PATH:" in
        *":$1:"*) ;;
        *) export PATH="$1:$PATH" ;;
    esac
}

ensure_system_tools() {
    local toolchain="$1"
    local packages=(
        build-essential ninja-build pkg-config curl git unzip zip xz-utils ca-certificates
        python3 python3-venv python3-pip clang-format clang-format-18 llvm ccache lld mold
    )
    if [[ "$toolchain" == *-glibc ]]; then
        packages+=(capnproto libcapnp-dev zlib1g-dev libzstd-dev liblz4-dev libsnappy-dev)
    fi
    local missing=() package
    for package in "${packages[@]}"; do
        if [[ "$(dpkg-query -W -f='${Status}' "$package" 2>/dev/null || true)" != 'install ok installed' ]]; then
            missing+=("$package")
        fi
    done
    if ((${#missing[@]} == 0)); then return; fi
    local privilege=()
    if ((EUID != 0)); then privilege=(sudo); fi
    "${privilege[@]}" apt-get update
    "${privilege[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
}

ensure_uv() {
    local local_bin="${UV_INSTALL_DIR:-$HOME/.local/bin}"
    mkdir -p "$local_bin"
    ensure_path_entry "$local_bin"
    if command -v uv >/dev/null 2>&1; then
        info "uv available: $(uv --version)"
        return 0
    fi
    info "Installing uv (Astral) to ${local_bin}..."
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$local_bin" sh
    hash -r
    if ! command -v uv >/dev/null 2>&1; then
        echo 'Error: uv auto-installation failed.' >&2
        return 1
    fi
}

ensure_ninja() (
    local required_version="1.13.2" current_version=""
    local local_bin="$HOME/.local/bin"
    mkdir -p "$local_bin"
    ensure_path_entry "$local_bin"
    if command -v ninja >/dev/null 2>&1; then
        current_version="$(ninja --version)"
    fi
    if [[ -n "$current_version" ]] && version_ge "$current_version" "$required_version"; then
        info "Ninja ${current_version} available"
        return 0
    fi
    local tmpdir
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT
    curl -fsSL -o "$tmpdir/ninja.zip" \
        "https://github.com/ninja-build/ninja/releases/download/v${required_version}/ninja-linux.zip"
    unzip -q "$tmpdir/ninja.zip" -d "$tmpdir"
    install -m 0755 "$tmpdir/ninja" "$local_bin/ninja"
    info "Ninja installed: $("$local_bin/ninja" --version)"
)

ensure_cmake() (
    local required_version="4.4.5" local_prefix="$HOME/.local"
    local current_version=""
    ensure_path_entry "$local_prefix/bin"
    if command -v cmake >/dev/null 2>&1; then
        current_version="$(cmake --version | sed -n '1s/.*version //p')"
    fi
    if [[ -n "$current_version" ]] && version_ge "$current_version" "$required_version"; then
        info "CMake ${current_version} available"
        return 0
    fi
    local tmpdir archive="cmake-${required_version}-linux-x86_64"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT
    curl -fsSL -o "$tmpdir/cmake.tar.gz" \
        "https://github.com/Kitware/CMake/releases/download/v${required_version}/${archive}.tar.gz"
    tar -xzf "$tmpdir/cmake.tar.gz" -C "$tmpdir"
    mkdir -p "$local_prefix"
    cp -a "$tmpdir/$archive/." "$local_prefix/"
    info "CMake installed: $("$local_prefix/bin/cmake" --version | sed -n '1p')"
)

ensure_vcpkg() {
    local directory="${VCPKG_ROOT:-$HOME/.vcpkg}"
    if [[ ! -d "$directory" ]]; then
        git clone https://github.com/microsoft/vcpkg.git "$directory"
    fi
    if [[ ! -f "$directory/bootstrap-vcpkg.sh" ]]; then
        echo "Error: not a vcpkg checkout: $directory" >&2
        return 1
    fi
    if [[ ! -x "$directory/vcpkg" ]]; then
        "$directory/bootstrap-vcpkg.sh" -disableMetrics
    fi
    info "vcpkg available at ${directory}; existing checkouts are preserved"
}

ensure_fory_compiler() {
    local fory_commit="${FORY_COMMIT:-99fd6b3a4d23ca78f99070d3fe8555bb8fa48afa}"
    local venv="$HOME/.local/share/fory-venv"
    if [[ -x "$venv/bin/foryc" ]] && "$venv/bin/python3" -c 'import fory_compiler' >/dev/null 2>&1; then
        mkdir -p "$HOME/.local/bin"
        ln -sfn "$venv/bin/foryc" "$HOME/.local/bin/foryc"
        return 0
    fi
    local tmpdir
    tmpdir="$(mktemp -d)"
    # shellcheck disable=SC2064
    trap "rm -rf ${tmpdir@Q}" EXIT
    curl -fsSL "https://github.com/apache/fory/archive/${fory_commit}.tar.gz" -o "$tmpdir/fory.tar.gz"
    tar -xzf "$tmpdir/fory.tar.gz" -C "$tmpdir"
    uv venv --python 3.12 "$venv"
    uv pip install --python "$venv/bin/python" "$tmpdir/fory-${fory_commit}/compiler"
    "$venv/bin/python3" -c 'import fory_compiler'
    mkdir -p "$HOME/.local/bin"
    ln -sfn "$venv/bin/foryc" "$HOME/.local/bin/foryc"
}

main() {
    local selected_toolchain="${GRPCXX_TOOLCHAIN:-gcc-glibc}"
    while (($# > 0)); do
        case "$1" in
            --help|-h)
                echo 'Usage: scripts/install-dependencies.sh [--vcpkg] [--toolchain <gcc-glibc|clang-glibc|gcc-musl|clang-musl>]'
                echo 'Install CMake >= 4.4.5, vcpkg, the reflection compiler, and missing overlay ports.'
                return 0 ;;
            --vcpkg) shift ;;
            --toolchain)
                if (($# < 2)); then
                    echo 'Error: --toolchain requires a toolchain name' >&2
                    return 2
                fi
                selected_toolchain="$2"
                shift 2 ;;
            *) echo "Error: unknown option $1" >&2; return 2 ;;
        esac
    done
    case "$selected_toolchain" in
        gcc-glibc|clang-glibc|gcc-musl|clang-musl) ;;
        *) echo "Error: unsupported toolchain: $selected_toolchain" >&2; return 2 ;;
    esac
    if [[ "$(uname -s)/$(uname -m)" != 'Linux/x86_64' ]]; then
        echo 'Error: the tool installer currently supports Linux x86_64 only.' >&2
        return 1
    fi
    mkdir -p "$HOME/.local/bin"
    ensure_path_entry "$HOME/.local/bin"
    ensure_system_tools "$selected_toolchain"
    ensure_uv
    ensure_ninja
    ensure_cmake
    ensure_vcpkg
    uv python install 3.12
    ensure_fory_compiler
    GRPCXX_TOOLCHAIN="$selected_toolchain" \
        bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.github/scripts/setup-ci-deps.sh"
    info "Ready. ${selected_toolchain} dependencies installed."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
