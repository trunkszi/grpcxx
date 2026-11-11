"""Global Typer options and modular command registration."""

from dataclasses import dataclass, field

import typer

from . import __version__
from .cmd_bench import bench_cmd
from .cmd_build import build_cmd
from .cmd_clean import clean_cmd
from .cmd_format import format_cmd
from .cmd_setup import setup_cmd
from .cmd_test import test_cmd
from .cmd_tidy import tidy_cmd
from .output import Output, OutputFormat

app = typer.Typer(name="build.py", help="Configure, build, test, and benchmark grpcxx.", add_completion=False)


@dataclass
class State:
    """Global defaults overridden by command-local options."""

    output: Output = field(default_factory=Output)
    preset: str | None = None
    compiler: str | None = None
    jobs: int | None = None
    verbose: bool = False


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    fmt: OutputFormat = typer.Option(OutputFormat.text, "--format", "-f"),
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    jobs: int | None = typer.Option(None, "-j", "--jobs", min=1),
    verbose: bool = typer.Option(False, "-v", "--verbose"),
    version: bool = typer.Option(False, "--version", "-V", is_eager=True),
) -> None:
    if version:
        typer.echo(f"build.py {__version__}")
        raise typer.Exit()
    ctx.obj = State(Output(fmt), preset, compiler, jobs, verbose)
    ctx.obj.output.verbose = verbose


for name, command in (
    ("build", build_cmd),
    ("test", test_cmd),
    ("bench", bench_cmd),
    ("clean", clean_cmd),
    ("format", format_cmd),
    ("tidy", tidy_cmd),
    ("setup", setup_cmd),
):
    app.command(name=name)(command)
