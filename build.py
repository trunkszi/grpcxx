#!/usr/bin/env bash
# Launch the uv-managed grpcxx CLI from any working directory.
set -euo pipefail

GRPCXX_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="${GRPCXX_ROOT}/scripts"
export PATH="$HOME/.local/bin:$PATH"

cached_cmake="/root/.vcpkg/downloads/tools/cmake-3.31.10-linux/cmake-3.31.10-linux-x86_64/bin"
use_cached_cmake=true
if command -v cmake >/dev/null 2>&1; then
    cmake_version="$(cmake --version 2>/dev/null || true)"
    if [[ "$cmake_version" =~ cmake\ version\ ([0-9]+)\.([0-9]+) ]]; then
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[2]}"
        if (( major > 3 || (major == 3 && minor >= 30) )); then
            use_cached_cmake=false
        fi
    fi
fi
if [[ "$use_cached_cmake" == true && -x "$cached_cmake/cmake" ]]; then
    export PATH="$cached_cmake:$PATH"
fi

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

unset RUST_LOG
export PYTHONPATH="${SCRIPTS_DIR}${PYTHONPATH:+:$PYTHONPATH}"
exec uv run --directory "${SCRIPTS_DIR}/cli" python -m cli "$@"
