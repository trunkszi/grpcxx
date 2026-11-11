"""Configure and build using the same CMake presets as direct CMake invocations."""

import typer

from .config import get_build_directory
from .output import Output
from .utils import clean_project, run_action, selection


def ensure_preset_tmp(preset: str) -> None:
    """Presets export TMPDIR here before cmake starts; clang needs it for assembly temps."""
    (get_build_directory(preset) / "tmp").mkdir(parents=True, exist_ok=True)


def ensure_configured(preset: str, out: Output) -> int:
    ensure_preset_tmp(preset)
    return out.run_command(["cmake", "--preset", preset], label="cmake configure").return_code


def build_project(preset: str, out: Output, jobs: int, target: str | None = None) -> int:
    ensure_preset_tmp(preset)
    command = ["cmake", "--build", "--preset", preset, "--parallel", str(jobs)]
    if out.verbose:
        command.append("--verbose")
    if target:
        command.extend(["--target", target])
    return out.run_command(command, label="cmake build").return_code


def build_cmd(
    ctx: typer.Context,
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    target: str | None = typer.Option(None, "-t", "--target"),
    jobs: int | None = typer.Option(None, "-j", "--jobs", min=1),
    clean: bool = typer.Option(False, "--clean", help="Remove this preset's build tree first"),
) -> None:
    """Configure and build all targets, or only --target."""

    def action(out: Output) -> int:
        name, count = selection(ctx, preset, compiler, jobs)
        if clean:
            clean_project(name, out)
        return ensure_configured(name, out) or build_project(name, out, count, target)

    run_action(ctx, action)
