"""Rich text output or a single structured JSON document, including child output."""

import json
import shlex
import time
from enum import Enum
from pathlib import Path
from typing import Any

from invoke import Result
from rich.console import Console
from rich.table import Table


class OutputFormat(str, Enum):
    """Public output selectors shared by every command."""

    text = "text"
    json = "json"


class Output:
    """Keep subprocess streams out of stdout when emitting structured JSON."""

    def __init__(self, fmt: OutputFormat = OutputFormat.text) -> None:
        self.fmt = fmt
        self.events: list[dict[str, Any]] = []
        self.started = time.monotonic()
        self.finished = False
        self.verbose = False

    @property
    def is_json(self) -> bool:
        return self.fmt == OutputFormat.json

    def emit(self, level: str, message: str, **data: Any) -> None:
        if self.is_json:
            self.events.append({"level": level, "message": message, "data": data})
        else:
            console = Console(stderr=level == "error")
            console.print(f"[{level.upper()}] {message}", markup=False)
            if data:
                console.print(data)

    def info(self, message: str, **data: Any) -> None:
        self.emit("info", message, **data)

    def error(self, message: str, **data: Any) -> None:
        self.emit("error", message, **data)

    def table(self, title: str, columns: list[str], rows: list[list[str]]) -> None:
        if self.is_json:
            self.emit("table", title, columns=columns, rows=rows)
        else:
            table = Table(title=title)
            for column in columns:
                table.add_column(column)
            for row in rows:
                table.add_row(*row)
            Console().print(table)

    def run_command(self, cmd: list[str], *, cwd: Path | None = None, label: str = "", capture: bool = False) -> Result:
        from .utils import run_command

        command = shlex.join(str(part) for part in cmd)
        if not self.is_json:
            self.info(f"RUN {command}")
        result = run_command(cmd, cwd=cwd, hide=self.is_json or capture)
        if self.is_json:
            self.emit(
                "command",
                label or command,
                cmd=command,
                exit_code=result.return_code,
                stdout=result.stdout[-50_000:],
                stderr=result.stderr[-10_000:],
            )
        return result

    def finish(self, exit_code: int = 0) -> int:
        if self.is_json and not self.finished:
            print(
                json.dumps(
                    {
                        "success": exit_code == 0,
                        "exit_code": exit_code,
                        "elapsed_seconds": round(time.monotonic() - self.started, 3),
                        "events": self.events,
                    },
                    indent=2,
                )
            )
        self.finished = True
        return exit_code
