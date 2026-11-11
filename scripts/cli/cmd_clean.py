"""Remove one preset build directory."""

import typer

from .output import Output
from .utils import clean_project, run_action, selection


def clean_cmd(
    ctx: typer.Context,
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
) -> None:
    def action(out: Output) -> int:
        name, _ = selection(ctx, preset, compiler)
        return clean_project(name, out)

    run_action(ctx, action)
