"""Launch from the repository root and insert the default build subcommand."""

import os
import sys

from .config import PROJECT_ROOT

KNOWN_COMMANDS = {"build", "test", "bench", "clean", "format", "tidy", "setup"}
GLOBAL_TERMINAL_FLAGS = {"--version", "-V", "--help", "-h"}
GLOBAL_VALUE_OPTIONS = {"--format", "-f", "--preset", "--compiler", "-c", "--jobs", "-j"}


def _with_default_command(argv: list[str]) -> list[str]:
    """Keep global options before the inserted command and build options after it."""
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in GLOBAL_TERMINAL_FLAGS:
            return argv
        if token in GLOBAL_VALUE_OPTIONS:
            index += 2
        elif any(token.startswith(option + "=") for option in GLOBAL_VALUE_OPTIONS) or token in {"-v", "--verbose"}:
            index += 1
        else:
            break
    if index < len(argv) and argv[index] in KNOWN_COMMANDS:
        return argv
    return [*argv[:index], "build", *argv[index:]]


def main() -> None:
    """Resolve the CLI package independently of uv's project-root working directory."""
    from .app import app

    os.chdir(PROJECT_ROOT)
    sys.argv = [sys.argv[0], *_with_default_command(sys.argv[1:])]
    app(prog_name="build.py")


if __name__ == "__main__":
    main()
