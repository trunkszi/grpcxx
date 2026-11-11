#!/usr/bin/env bash
# Launch the uv-managed grpcxx CLI from any working directory.
set -euo pipefail

GRPCXX_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="${GRPCXX_ROOT}/scripts"
export PATH="$HOME/.local/bin:$PATH"

ensure_uv() {
    local local_bin="${UV_INSTALL_DIR:-$HOME/.local/bin}"
    mkdir -p "$local_bin"
    case ":$PATH:" in
        *":$local_bin:"*) ;;
        *) export PATH="$local_bin:$PATH" ;;
    esac
    if command -v uv >/dev/null 2>&1; then
        return 0
    fi
    echo "[INFO] uv not found; downloading uv (Astral) to ${local_bin}..." >&2
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$local_bin" sh
    hash -r
    if ! command -v uv >/dev/null 2>&1; then
        echo "Error: uv auto-installation failed." >&2
        exit 1
    fi
}
ensure_uv

if [[ "${1:-}" == setup ]]; then
    shift
    setup_args=()
    setup_preset=""
    while (($#)); do
        case "$1" in
            --preset)
                if (($# < 2)); then echo 'Error: --preset requires a value.' >&2; exit 2; fi
                setup_preset="$2"; shift 2 ;;
            --preset=*) setup_preset="${1#*=}"; shift ;;
            *) setup_args+=("$1"); shift ;;
        esac
    done
    if [[ -n "$setup_preset" ]]; then
        case "$setup_preset" in
            gcc-glibc|clang-glibc) export GRPCXX_TOOLCHAIN="$setup_preset" ;;
            gcc-asan) export GRPCXX_TOOLCHAIN=gcc-glibc GRPCXX_DEPENDENCY_PROFILE=asan-ubsan ;;
            clang-asan) export GRPCXX_TOOLCHAIN=clang-glibc GRPCXX_DEPENDENCY_PROFILE=asan-ubsan ;;
            *) echo "Error: setup requires a native glibc preset: $setup_preset" >&2; exit 2 ;;
        esac
        set -- --preset "$setup_preset" setup "${setup_args[@]}"
    else
        set -- setup "${setup_args[@]}"
    fi
fi

unset RUST_LOG
export PYTHONPATH="${SCRIPTS_DIR}${PYTHONPATH:+:$PYTHONPATH}"
exec uv run --directory "${SCRIPTS_DIR}/cli" python -m cli "$@"
