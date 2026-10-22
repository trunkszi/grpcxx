"""Run clang-tidy with the selected preset's compilation database."""

import json
from pathlib import Path

import typer

from .cmd_build import ensure_configured
from .config import get_build_directory
from .output import Output
from .utils import collect_cpp_files, find_clang_tool, run_action, selection


def tidy_cmd(
    ctx: typer.Context,
    paths: list[str] | None = typer.Argument(None),
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    fix: bool = typer.Option(False, "--fix"),
) -> None:
    def action(out: Output) -> int:
        name, _ = selection(ctx, preset, compiler)
        tool = find_clang_tool("clang-tidy")
        directory = get_build_directory(name)
        database = directory / "compile_commands.json"
        if not database.exists() and (code := ensure_configured(name, out)):
            return code
        entries = json.loads(database.read_text())
        compiled = {(Path(entry["directory"]) / entry["file"]).resolve() for entry in entries}
        files = [path for path in collect_cpp_files(paths) if path.resolve() in compiled]
        if not files:
            raise ValueError("No selected sources are present in compile_commands.json")
        for path in files:
            command = [tool, "-p", str(directory), str(path)]
            if fix:
                command.append("--fix")
            if code := out.run_command(command, label="clang-tidy").return_code:
                return code
        return 0

    run_action(ctx, action)
