"""Shared execution, context selection, and bounded repository file operations."""

import os
import re
import shlex
import shutil
from collections.abc import Callable
from pathlib import Path

import typer
from invoke import Context, Result

from .config import CPP_EXTENSIONS, DEFAULT_JOBS, PROJECT_ROOT, get_build_directory
from .output import Output
from .preset import resolve_preset


def run_command(cmd: list[str], *, cwd: Path | None = None, hide: bool = False) -> Result:
    """Quote every argv element before Invoke passes the command to the shell."""
    command = shlex.join(str(part) for part in cmd)
    command = f"cd {shlex.quote(str(cwd or PROJECT_ROOT))} && {command}"
    return Context().run(command, hide="both" if hide else False, warn=True, pty=False, in_stream=False)


def selection(ctx: typer.Context, preset: str | None, compiler: str | None, jobs: int | None = None) -> tuple[str, int]:
    """Combine global and command-local options, validating conflicting choices."""
    state = ctx.obj
    selected = resolve_preset(preset or state.preset, compiler or state.compiler)
    return selected, jobs or state.jobs or DEFAULT_JOBS


def run_action(ctx: typer.Context, action: Callable[[Output], int]) -> None:
    """Always flush one JSON outcome for operational errors and child failures."""
    out = ctx.obj.output
    try:
        code = action(out)
    except (OSError, ValueError, re.error) as error:
        out.error(str(error))
        code = 1
    raise typer.Exit(out.finish(code))


def clean_project(preset: str, out: Output) -> int:
    """Remove only a validated direct child of the repository's build directory."""
    resolve_preset(preset)
    directory = get_build_directory(preset)
    if directory.is_symlink() or directory.resolve().parent != (PROJECT_ROOT / "build"):
        raise ValueError(f"Refusing to remove a redirected build directory: {directory}")
    if directory.exists():
        shutil.rmtree(directory)
    out.info(f"Cleaned {directory}")
    return 0


def find_clang_tool(name: str) -> str:
    """Prefer versioned tools, reporting missing installations explicitly."""
    candidates = (
        ("clang-format-18", name) if name == "clang-format" else (f"{name}-21", name, f"{name}-20", f"{name}-19")
    )
    for candidate in candidates:
        if executable := shutil.which(candidate):
            return executable
    raise ValueError(f"{name} is not installed; add it to PATH")


def collect_cpp_files(paths: list[str] | None) -> list[Path]:
    """Collect source files without traversing generated, dependency, or hidden trees."""
    result: set[Path] = set()
    for value in paths or ["src", "examples", "tests", "benches"]:
        path = (PROJECT_ROOT / value).resolve()
        if not path.is_relative_to(PROJECT_ROOT) or not path.exists():
            raise ValueError(f"Not a repository path: {value}")
        candidates = [path] if path.is_file() else []
        if path.is_dir():
            for directory, folders, files in os.walk(path, followlinks=False):
                folders[:] = [name for name in folders if name != "build" and not name.startswith(".")]
                candidates.extend(Path(directory) / name for name in files)
        for candidate in candidates:
            if candidate.suffix not in CPP_EXTENSIONS or not candidate.is_file():
                continue
            relative = candidate.relative_to(PROJECT_ROOT)
            if not any(
                part == "build" or part.startswith(".") for part in relative.parts
            ) and candidate.resolve().is_relative_to(PROJECT_ROOT):
                result.add(candidate)
    return sorted(result)
