"""Repository paths and the public compiler/benchmark registries."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PRESET = "gcc-glibc"
DEFAULT_JOBS = 8
COMPILERS = {
    "gcc": "gcc-glibc",
    "clang": "clang-glibc",
    "musl-gcc": "gcc-musl",
    "musl-clang": "clang-musl",
}
BENCHMARK_TARGETS = {
    "bench_aligned_framing": "bench_aligned_framing",
    "bench_service_router": "bench_service_router",
    "bench_codecs": "bench_multi_codec",
    "bench_compression": "bench_compression",
    "bench_order_pipeline": "bench_order_pipeline",
}
CPP_EXTENSIONS = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}


def get_build_directory(preset: str) -> Path:
    """Return grpcxx's preset build directory; callers validate the preset first."""
    return PROJECT_ROOT / "build" / preset
