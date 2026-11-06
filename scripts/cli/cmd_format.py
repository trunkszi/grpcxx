"""Format repository C++ files using clang-format."""

import typer

from .output import Output
from .utils import collect_cpp_files, find_clang_tool, run_action


def format_cmd(
    ctx: typer.Context, paths: list[str] | None = typer.Argument(None), check: bool = typer.Option(False, "--check")
) -> None:
    def action(out: Output) -> int:
        tool = find_clang_tool("clang-format")
        files = collect_cpp_files(paths)
        if not files:
            raise ValueError("No C++ files selected")
        options = ["--dry-run", "--Werror"] if check else ["-i"]
        for offset in range(0, len(files), 64):
            command = [tool, *options, *[str(path) for path in files[offset : offset + 64]]]
            if code := out.run_command(command, label="clang-format").return_code:
                return code
        return 0

    run_action(ctx, action)
