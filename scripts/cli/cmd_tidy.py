"""Run clang-tidy with the selected preset's compilation database."""

import json
import re
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from . import utils
from .cmd_build import ensure_configured
from .config import get_build_directory
from .output import Output
from .utils import collect_cpp_files, find_clang_tidy, find_clang_tool, run_action, selection

DIAGNOSTIC_CHECK_RE = re.compile(r"(?:warning|error):\s+.*\[([^\],\s]+)(?:,[^\]]*)?\]\s*$")
LOCATION_DIAG_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+(warning|error):\s+(.*)\[([^\],\s]+)(?:,[^\]]*)?\]\s*$")
BARE_LOCATION_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+(warning|error):\s+(.*)$")
NOTE_LOCATION_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+note:\s+(.*)$")
LOCATION_PREFIX_RE = re.compile(r"^[^:\n]+:\d+:\d+:\s+(?:warning|error|note):\s+")

REFLECTION_DIAGNOSTIC_MARKERS = (
    "^^",
    "[[=",
    "[:",
    "std::meta",
    "no member named 'meta' in namespace 'std'",
    "define_static_",
    "template for",
    "blocks support disabled",
    "'meta' file not found",
    "reflection is only",
    "meta/meta",
    "expansion statements",
    "iterating expansion statements are not yet supported",
)

_REFLECTION_SEED_MARKERS = (
    *REFLECTION_DIAGNOSTIC_MARKERS,
    "uarch::",
    "ZeroRegisterCapability",
    "RegisterPassable",
    "NoPadding",
    "tiers::as_str",
    "Subsystem(token)",
    "Slot(token)",
    "State(h)",
    "ParkedRegistry",
    "ServiceContract",
    "ExplicitContract",
    "ServiceName",
    "ContractMethods",
    "HasFourShapes",
)

_UNDECLARED_ANCHOR_WINDOW = 20

_DRIVER_ERROR_MARKERS = (
    "unknown argument:",
    "unsupported option",
    "invalid value",
    "no such file or directory",
    "unable to find compilation database",
    "Error while trying to load a compilation database",
    "unable to handle compilation",
    "no input files",
)

_REFLECTION_PROBE = "consteval bool Probe() { return ^^int == ^^int; }\nstatic_assert(Probe());\n"


@dataclass
class _DiagnosticBlock:
    file: str
    line: int
    message: str
    header: str
    lines: list[str] = field(default_factory=list)
    notes: set[str] = field(default_factory=set)
    note_locations: set[tuple[str, int]] = field(default_factory=set)

    def body(self) -> str:
        return "\n".join(LOCATION_PREFIX_RE.sub("", line) for line in self.lines)


def _has_direct_marker(text: str) -> bool:
    return any(marker in text for marker in REFLECTION_DIAGNOSTIC_MARKERS)


def _is_reflection_seed(text: str) -> bool:
    return any(marker in text for marker in _REFLECTION_SEED_MARKERS)


def _has_reflection_note(
    block: _DiagnosticBlock,
    reflection_files: set[str] | None = None,
    reflection_locations: set[tuple[str, int]] | None = None,
) -> bool:
    if reflection_files and (block.notes & reflection_files):
        return True
    if reflection_locations and (block.note_locations & reflection_locations):
        return True
    for line in block.lines[1:]:
        if ": note:" in line and (_has_direct_marker(line) or _is_reflection_seed(line)):
            return True
    return False


def _is_direct_reflection_block(block: _DiagnosticBlock) -> bool:
    stripped = block.message.strip()
    return _has_direct_marker(block.body()) or stripped.startswith("expanding ") or _has_reflection_note(block)


def _parse_diagnostic_blocks(output: str) -> tuple[list[_DiagnosticBlock], list[str]]:
    blocks: list[_DiagnosticBlock] = []
    unlocated: list[str] = []
    current: _DiagnosticBlock | None = None
    for line in output.splitlines():
        if any(marker in line for marker in _DRIVER_ERROR_MARKERS):
            if current is not None:
                blocks.append(current)
                current = None
            unlocated.append(line)
            continue
        located = LOCATION_DIAG_RE.match(line)
        if located:
            if current is not None:
                blocks.append(current)
            check_name = located.group(6)
            if check_name.startswith("clang-diagnostic-"):
                current = _DiagnosticBlock(
                    file=located.group(1),
                    line=int(located.group(2)),
                    message=located.group(5),
                    header=line,
                    lines=[line],
                )
            else:
                current = None
            continue
        bare = BARE_LOCATION_RE.match(line)
        if bare:
            if current is not None:
                blocks.append(current)
            current = _DiagnosticBlock(
                file=bare.group(1),
                line=int(bare.group(2)),
                message=bare.group(5),
                header=line,
                lines=[line],
            )
            continue
        check = DIAGNOSTIC_CHECK_RE.search(line)
        if check:
            if current is not None:
                blocks.append(current)
                current = None
            if check.group(1).startswith("clang-diagnostic-"):
                unlocated.append(line)
            continue
        if current is not None:
            current.lines.append(line)
            note = NOTE_LOCATION_RE.match(line)
            if note:
                current.notes.add(note.group(1))
                current.note_locations.add((note.group(1), int(note.group(2))))
    if current is not None:
        blocks.append(current)
    return blocks, unlocated


def _is_silent_alias_cascade(block: _DiagnosticBlock) -> bool:
    match = re.search(r"undeclared identifier '(\w+)'", block.message)
    if not match:
        return False
    source = Path(block.file)
    if not source.is_file():
        return False
    text = source.read_text(errors="ignore")
    if "uring::" not in text and "uarch::" not in text:
        return False
    lines = text.splitlines()
    window = "\n".join(lines[max(0, block.line - 4) : max(0, block.line - 1)])
    return f"using {match.group(1)} = decltype(" in window


def _classify_clang_diagnostics(output: str) -> tuple[bool, list[str]]:
    if any(marker in output for marker in _DRIVER_ERROR_MARKERS):
        return False, [line for line in output.splitlines() if line.strip()]
    blocks, unlocated = _parse_diagnostic_blocks(output)
    if unlocated or not blocks:
        return False, unlocated
    by_file: dict[str, list[_DiagnosticBlock]] = {}
    direct_lines: dict[str, list[int]] = {}
    reflection_locations: set[tuple[str, int]] = set()
    for block in blocks:
        by_file.setdefault(block.file, []).append(block)
        if _is_direct_reflection_block(block):
            direct_lines.setdefault(block.file, []).append(block.line)
            reflection_locations.add((block.file, block.line))
            reflection_locations.update(block.note_locations)
    if not direct_lines:
        return False, [block.header for block in blocks]
    reflection_files: set[str] = set(direct_lines.keys())
    for file_path, file_blocks in by_file.items():
        if _is_reflection_seed(file_blocks[0].body()):
            reflection_files.add(file_path)
        for block in file_blocks:
            if _is_silent_alias_cascade(block):
                reflection_files.add(file_path)
                reflection_locations.add((block.file, block.line))
    changed = True
    while changed:
        changed = False
        for file_path, file_blocks in by_file.items():
            if file_path not in reflection_files and any(
                _has_reflection_note(block, reflection_files, reflection_locations) for block in file_blocks
            ):
                reflection_files.add(file_path)
                changed = True
    unsuppressed: list[str] = []
    for block in blocks:
        if (
            block.file not in reflection_files
            and (block.file, block.line) not in reflection_locations
            and not _has_reflection_note(block, reflection_files, reflection_locations)
        ):
            unsuppressed.append(block.header)
            continue
        if "undeclared identifier" in block.message:
            anchors = direct_lines.get(block.file, [])
            if not (
                (block.file, block.line) in reflection_locations
                or any(0 <= block.line - anchor <= _UNDECLARED_ANCHOR_WINDOW for anchor in anchors)
            ):
                unsuppressed.append(block.header)
    return bool(blocks and not unsuppressed), unsuppressed


def _supports_p2996_reflection(clang_tidy: str) -> bool:
    """Return True when clang-tidy natively parses C++26 P2996 reflection syntax."""
    try:
        with TemporaryDirectory(prefix="grpcxx-tidy-probe-") as temporary:
            probe = Path(temporary) / "probe.cc"
            probe.write_text(_REFLECTION_PROBE)
            for flag in ("-freflection", "-freflection-latest"):
                result = subprocess.run(
                    [str(clang_tidy), str(probe), "--", "-std=c++26", flag],
                    cwd=temporary,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=15,
                )
                if result.returncode == 0:
                    return True
    except (OSError, subprocess.SubprocessError):
        return False
    return False


def _extract_non_clang_diagnostics(output: str) -> list[str]:
    return [
        line
        for line in output.splitlines()
        if (match := DIAGNOSTIC_CHECK_RE.search(line)) and not match[1].startswith("clang-diagnostic-")
    ]


def _unique_lines(lines: list[str]) -> list[str]:
    return list(dict.fromkeys(line for line in lines if line))


def _actionable_fatal_lines(output: str) -> list[str]:
    return [line for line in output.splitlines() if "fatal error:" in line and "'meta' file not found" not in line]


def _tidy_failure(output: str, status: int | None = 1, capable: bool = False) -> tuple[bool, str]:
    if capable:
        return (status is not None and status != 0) or "fatal error:" in output, output
    filtered = _unique_lines(_extract_non_clang_diagnostics(output))
    fatals = _unique_lines(_actionable_fatal_lines(output))
    reflection_blocks, raw_unsuppressed = _classify_clang_diagnostics(output)
    unsuppressed = _unique_lines(raw_unsuppressed)
    has_reflection = reflection_blocks or ("'meta' file not found" in output and not fatals and not unsuppressed)
    negative = status is not None and status < 0
    positive = status is not None and status > 0
    if negative or filtered or fatals or unsuppressed or (positive and not has_reflection):
        shown = _unique_lines([*filtered, *fatals, *unsuppressed])
        return True, "\n".join(shown) if shown else output
    return False, ""


def clang_database(entries: list[dict], tool: str, *, capable: bool = False) -> list[dict]:
    """Translate GCC driver flags and strip reflection flags unsupported by clang-tidy."""
    translated = []
    seen = set()
    stripped = {"-freflection-latest"} if capable else {"-freflection-latest", "-freflection"}
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
                if capable:
                    raise ValueError("GCC reflection analysis needs a clang-p2996 SDK with libc++ <meta>")
            arguments = ["-freflection-latest" if arg == "-freflection" else arg for arg in arguments]
        arguments = [
            arg
            for arg in arguments
            if arg not in stripped
            and not arg.startswith(("-fconcepts-diagnostics-depth=", "-fdiagnostics-show-template-tree"))
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
        tool = find_clang_tool("clang-tidy") if find_clang_tool is not utils.find_clang_tool else find_clang_tidy()
        capable = _supports_p2996_reflection(tool)
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
        extra_args = (
            ["--extra-arg=-freflection", "--warnings-as-errors=*"]
            if capable
            else ["--extra-arg=-D__is_consteval_only(...)=false", "--extra-arg=-ferror-limit=0"]
        )
        with TemporaryDirectory(prefix="grpcxx-tidy-", dir=directory) as temporary:
            (Path(temporary) / "compile_commands.json").write_text(
                json.dumps(clang_database(entries, tool, capable=capable))
            )
            for path in files:
                command = [
                    tool,
                    "-p",
                    temporary,
                    str(path),
                    "--checks=clang-analyzer-*,bugprone-use-after-move",
                    *extra_args,
                ]
                if fix:
                    command.append("--fix")
                result = out.run_command(command, label="clang-tidy", capture=not capable)
                combined = f"{result.stdout or ''}{result.stderr or ''}"
                failed, display = _tidy_failure(combined, result.return_code, capable=capable)
                if failed:
                    if display and not capable:
                        out.error(display)
                    return result.return_code or 1
        return 0

    run_action(ctx, action)
