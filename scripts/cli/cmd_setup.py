"""Bootstrap the developer tools using the repository installer."""

import typer

from .config import PROJECT_ROOT
from .utils import run_action


def setup_cmd(ctx: typer.Context, vcpkg: bool = typer.Option(False, "--vcpkg")) -> None:
    command = [str(PROJECT_ROOT / "scripts" / "install-dependencies.sh")]
    if vcpkg:
        command.append("--vcpkg")
    run_action(ctx, lambda out: out.run_command(command, label="setup").return_code)
