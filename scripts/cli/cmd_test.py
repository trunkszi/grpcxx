"""CTest execution with filters that also recognize discovered executable targets."""

import json
import re
from pathlib import Path

import typer

from .cmd_build import build_project, ensure_configured
from .config import get_build_directory
from .output import Output
from .utils import run_action, selection


def discover_tests(out: Output, directory: Path, label: str | None = None) -> list[dict]:
    """Read CTest's executable/name mapping instead of guessing GoogleTest names."""
    command = ["ctest", "--test-dir", str(directory), "--show-only=json-v1"]
    if label:
        command.extend(["-L", label])
    result = out.run_command(command, label="ctest discovery", capture=True)
    if result.failed:
        raise ValueError(f"CTest discovery failed (exit {result.return_code})")
    return json.loads(result.stdout)["tests"]


def select_tests(tests: list[dict], target: str | None, pattern: str | None) -> list[str]:
    """Intersect target and regex; a regex can match CTest names or binary stems."""
    regex = re.compile(pattern) if pattern else None
    names = []
    for entry in tests:
        command = entry.get("command", [])
        binary = Path(command[0]).stem if command else ""
        name = entry["name"]
        if target and binary != target:
            continue
        if regex and not (regex.search(name) or regex.search(binary)):
            continue
        names.append(name)
    if not names:
        raise ValueError("No CTest cases match the requested target/filter")
    return names


def test_cmd(
    ctx: typer.Context,
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    target: str | None = typer.Option(None, "-t", "--target"),
    filter_pattern: str | None = typer.Option(None, "--filter", "-R"),
    label: str | None = typer.Option(None, "--label", "-L"),
    parallel: int | None = typer.Option(None, "-j", "--parallel", "--jobs", min=1),
    no_build: bool = typer.Option(False, "--no-build"),
    list_tests: bool = typer.Option(False, "--list"),
) -> None:
    def action(out: Output) -> int:
        name, count = selection(ctx, preset, compiler, parallel)
        directory = get_build_directory(name)
        if not no_build:
            if code := ensure_configured(name, out):
                return code
            if not list_tests and (code := build_project(name, out, count, target)):
                return code
        elif not (directory / "CMakeCache.txt").is_file():
            raise ValueError(f"No configured build directory: {directory}")
        tests = discover_tests(out, directory, label) if list_tests or target or filter_pattern else []
        selected = select_tests(tests, target, filter_pattern) if target or filter_pattern else None
        if list_tests:
            out.table(
                f"CTest cases ({name})",
                ["Test", "Executable"],
                [
                    [entry["name"], Path((entry.get("command") or [""])[0]).name]
                    for entry in tests
                    if selected is None or entry["name"] in selected
                ],
            )
            return 0
        command = ["ctest", "--test-dir", str(directory), "--output-on-failure", "--no-tests=error", "-j", str(count)]
        if selected is not None:
            command.extend(["-R", "^(" + "|".join(re.escape(item) for item in selected) + ")$"])
        if label:
            command.extend(["-L", label])
        if out.verbose:
            command.append("-V")
        return out.run_command(command, label="ctest").return_code

    run_action(ctx, action)
