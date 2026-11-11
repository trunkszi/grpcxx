"""Resolve compiler aliases against the repository's CMake preset declarations."""

import json
import re

from .config import COMPILERS, DEFAULT_PRESET, PROJECT_ROOT


def load_available_presets() -> set[str]:
    """Read visible configure/build preset pairs without guessing toolchain names."""
    text = (PROJECT_ROOT / "CMakePresets.json").read_text(encoding="utf-8")
    pattern = r'("(?:[^"\\]|\\.)*")|//[^\n]*|/\*[\s\S]*?\*/'
    text = re.sub(pattern, lambda match: match.group(1) or "", text)
    data = json.loads(text)
    configure = {entry["name"] for entry in data["configurePresets"] if not entry.get("hidden")}
    build = {entry["name"] for entry in data["buildPresets"] if not entry.get("hidden")}
    return configure & build


def resolve_preset(preset: str | None = None, compiler: str | None = None) -> str:
    """Reject unknown or conflicting selections before starting any build command."""
    available = load_available_presets()
    selected = COMPILERS.get(compiler, compiler) if compiler else None
    if selected is not None and selected not in available:
        raise ValueError(f"Unknown compiler {compiler!r}; choose from {sorted(set(COMPILERS) | available)}")
    if preset and selected and preset != selected:
        raise ValueError(f"--preset {preset} conflicts with --compiler {compiler}")
    result = preset or selected or DEFAULT_PRESET
    if result not in available:
        raise ValueError(f"Unknown preset {result!r}; choose from {sorted(available)}")
    return result
