"""List benchmark entry points or build and execute a selected binary."""

import typer

from .cmd_build import build_project, ensure_configured
from .config import BENCHMARK_TARGETS, get_build_directory
from .output import Output
from .utils import run_action, selection


def bench_cmd(
    ctx: typer.Context,
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    target: str = typer.Option("bench_aligned_framing", "-t", "--target"),
    jobs: int | None = typer.Option(None, "-j", "--jobs", min=1),
    list_benchmarks: bool = typer.Option(False, "--list"),
    no_build: bool = typer.Option(False, "--no-build"),
    bench_args: list[str] | None = typer.Argument(None, help="Extra benchmark arguments after --"),
) -> None:
    def action(out: Output) -> int:
        name, count = selection(ctx, preset, compiler, jobs)
        if list_benchmarks:
            out.table(
                f"Benchmark targets ({name})",
                ["CLI target", "CMake target"],
                [[key, value] for key, value in BENCHMARK_TARGETS.items()],
            )
            return 0
        actual = BENCHMARK_TARGETS.get(target, target)
        if actual not in BENCHMARK_TARGETS.values():
            raise ValueError(f"Unknown benchmark {target!r}; use bench --list")
        if not no_build and (code := ensure_configured(name, out) or build_project(name, out, count, actual)):
            return code
        binary = get_build_directory(name) / "bin" / actual
        if not binary.is_file():
            raise ValueError(f"Benchmark binary not found: {binary}")
        extra = list(bench_args or [])
        if not any(arg.startswith("--benchmark_min_time") for arg in extra):
            extra.insert(0, "--benchmark_min_time=0.01s")
        return out.run_command([str(binary), *extra], label=f"bench:{target}").return_code

    run_action(ctx, action)
