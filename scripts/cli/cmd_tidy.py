"""Run clang-tidy with the selected preset's compilation database."""

import json
import shlex
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from .cmd_build import ensure_configured
from .config import get_build_directory
from .output import Output
from .utils import collect_cpp_files, find_clang_tool, run_action, selection


def clang_database(entries: list[dict], tool: str) -> list[dict]:
    """Translate GCC driver flags without discarding required reflection semantics."""
    translated = []
    seen = set()
    for entry in entries:
        source = (Path(entry["directory"]) / entry["file"]).resolve()
        if source in seen:
            continue
        seen.add(source)
        arguments = list(entry.get("arguments") or shlex.split(entry.get("command", "")))
        if not arguments:
            raise ValueError(f"Missing compiler command for {source}")
        if "-freflection" in arguments:
            roots = (
                Path(tool).resolve().parent.parent,
                Path("/opt/clang-p2996"),
                Path.home() / ".local/toolchain/clang-glibc",
            )
            for root in roots:
                sysroot = root / "x86_64-unknown-linux-gnu/sysroot"
                if (root / "bin/clang++").is_file() and (sysroot / "usr/include/c++/v1/meta").is_file():
                    arguments = [str(root / "bin/clang++"), *arguments[1:]]
                    arguments = [arg for arg in arguments if not arg.startswith("--sysroot=")]
                    arguments.extend([f"--sysroot={sysroot}", "-stdlib=libc++"])
                    break
                if (root / "bin/clang++").is_file() and (root / "include/c++/v1/meta").is_file():
                    arguments[0] = str(root / "bin/clang++")
                    arguments.extend(["-stdlib=libc++", "-stdlib++-isystem", str(root / "include/c++/v1")])
                    break
            else:
                raise ValueError("GCC reflection analysis needs a clang-p2996 SDK with libc++ <meta>")
            arguments = ["-freflection-latest" if arg == "-freflection" else arg for arg in arguments]
        # GCC diagnostic rendering has no effect on source analysis.
        arguments = [
            arg
            for arg in arguments
            if not arg.startswith(("-fconcepts-diagnostics-depth=", "-fdiagnostics-show-template-tree"))
        ]
        translated.append({"directory": entry["directory"], "file": str(source), "arguments": arguments})
    return translated


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
        selected = set(files)
        entries = [entry for entry in entries if (Path(entry["directory"]) / entry["file"]).resolve() in selected]
        with TemporaryDirectory(prefix="tidy-", dir=directory) as temporary:
            (Path(temporary) / "compile_commands.json").write_text(json.dumps(clang_database(entries, tool)))
            for path in files:
                command = [
                    tool,
                    "-p",
                    temporary,
                    str(path),
                    "--checks=clang-analyzer-*,bugprone-use-after-move",
                    "--warnings-as-errors=*",
                ]
                if fix:
                    command.append("--fix")
                result = out.run_command(command, label="clang-tidy")
                if result.return_code:
                    if "unknown argument: '-freflection-latest'" in result.stdout + result.stderr:
                        out.error(f"{tool} lacks P2996 reflection support; set CLANG_TIDY to a clang-p2996 build")
                    return result.return_code
        return 0

    run_action(ctx, action)
