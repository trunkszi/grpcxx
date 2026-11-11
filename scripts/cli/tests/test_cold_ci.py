"""Cold-runner TMPDIR creation and partial vcpkg overlay preservation."""

import json
import os
import subprocess

import pytest
from invoke import Result
from typer.testing import CliRunner

from cli import config, preset, utils
from cli.__main__ import _with_default_command
from cli.app import app
from cli.output import Output

REPOSITORY = config.PROJECT_ROOT
runner = CliRunner()


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "CMakePresets.json").write_text((REPOSITORY / "CMakePresets.json").read_text())
    for module in (config, preset, utils):
        monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    return tmp_path


def invoke(argv):
    return runner.invoke(app, _with_default_command(argv))


def test_preset_tmp_exists_before_cmake_and_ctest(project, monkeypatch):
    seen = []

    def fake_run(self, cmd, **kwargs):
        if cmd[:2] == ["cmake", "--preset"]:
            seen.append(("cmake", (project / "build" / cmd[2] / "tmp").is_dir()))
        elif cmd and cmd[0] == "ctest" and "--show-only=json-v1" not in cmd:
            seen.append(("ctest", (project / "build/clang-glibc/tmp").is_dir()))
        stdout = json.dumps({"tests": []}) if "--show-only=json-v1" in cmd else ""
        return Result(exited=0, stdout=stdout, stderr="")

    monkeypatch.setattr(Output, "run_command", fake_run)
    result = invoke(["--preset", "clang-glibc", "-j", "1", "test"])
    assert result.exit_code == 0, result.output
    assert seen == [("cmake", True), ("ctest", True)]
    assert (project / "build/clang-glibc/tmp").is_dir()


def test_no_build_ctest_still_creates_tmp(project, monkeypatch):
    cache = project / "build/gcc-musl"
    cache.mkdir(parents=True)
    (cache / "CMakeCache.txt").write_text("# configured\n")
    seen = []

    def fake_run(self, cmd, **kwargs):
        if cmd and cmd[0] == "ctest":
            seen.append((project / "build/gcc-musl/tmp").is_dir())
        return Result(exited=0, stdout="", stderr="")

    monkeypatch.setattr(Output, "run_command", fake_run)
    result = invoke(["--preset", "gcc-musl", "test", "--no-build"])
    assert result.exit_code == 0, result.output
    assert seen == [True]


def test_nonempty_partial_overlay_is_preserved(tmp_path):
    overlay = tmp_path / "overlay"
    upstream = tmp_path / "vcpkg-root/ports"
    for port, body in (
        ("abseil", "UPSTREAM-ABSEIL\n"),
        ("protobuf", "UPSTREAM-PROTOBUF\n"),
        ("c-ares", "UPSTREAM-CARES\n"),
    ):
        directory = upstream / port
        directory.mkdir(parents=True)
        (directory / "portfile.cmake").write_text(body)
        (directory / "vcpkg.json").write_text("{}\n")
    (upstream / "protobuf/vcpkg.json").write_text('{"name":"protobuf"}\n')
    for port in ("abseil", "protobuf", "c-ares", "fory", "nexora"):
        directory = overlay / port
        directory.mkdir(parents=True)
        (directory / "vcpkg.json").write_text(f"PARTIAL-{port}\n")
    script = REPOSITORY / ".github/scripts/setup-ci-deps.sh"
    env = {**os.environ, "VCPKG_ROOT": str(tmp_path / "vcpkg-root")}
    command = f"""
        set -euo pipefail
        source {json.dumps(str(script))}
        ensure_abseil_port {json.dumps(str(overlay))}
        ensure_protobuf_port {json.dumps(str(overlay))}
        ensure_c_ares_single_thread_port {json.dumps(str(overlay))}
        ensure_fory_port {json.dumps(str(overlay))}
        ensure_nexora_port {json.dumps(str(overlay))}
        ensure_fory_port {json.dumps(str(overlay / "fresh"))}
    """
    result = subprocess.run(["bash", "--noprofile", "--norc", "-c", command], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    for port in ("abseil", "protobuf", "c-ares", "fory", "nexora"):
        assert (overlay / port / "vcpkg.json").read_text() == f"PARTIAL-{port}\n"
        assert not (overlay / port / "portfile.cmake").exists()
        assert not (overlay / port / port).exists()
    fresh = overlay / "fresh/fory"
    assert (fresh / "portfile.cmake").is_file()
    assert '"name": "fory"' in (fresh / "vcpkg.json").read_text()
    assert not (fresh / "fory").exists()


def test_empty_overlay_directory_is_copied_without_nesting(tmp_path):
    overlay = tmp_path / "overlay"
    upstream = tmp_path / "vcpkg-root/ports/abseil"
    upstream.mkdir(parents=True)
    (upstream / "portfile.cmake").write_text("PORTFILE\n")
    (upstream / "vcpkg.json").write_text("JSON\n")
    (overlay / "abseil").mkdir(parents=True)
    script = REPOSITORY / ".github/scripts/setup-ci-deps.sh"
    env = {**os.environ, "VCPKG_ROOT": str(tmp_path / "vcpkg-root")}
    command = f"""
        set -euo pipefail
        source {json.dumps(str(script))}
        ensure_abseil_port {json.dumps(str(overlay))}
    """
    result = subprocess.run(["bash", "--noprofile", "--norc", "-c", command], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (overlay / "abseil/portfile.cmake").read_text() == "PORTFILE\n"
    assert (overlay / "abseil/vcpkg.json").read_text() == "JSON\n"
    assert not (overlay / "abseil/abseil").exists()


def test_existing_fory_venv_is_relinked_without_download(tmp_path):
    home = tmp_path / "home"
    venv = home / ".local/share/fory-venv/bin"
    venv.mkdir(parents=True)
    python = venv / "python3"
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)
    foryc = venv / "foryc"
    foryc.write_text("#!/bin/sh\nexit 0\n")
    foryc.chmod(0o755)
    script = REPOSITORY / "scripts/install-dependencies.sh"
    env = {**os.environ, "HOME": str(home), "PATH": "/usr/bin:/bin"}
    command = f"""
        set -euo pipefail
        source {json.dumps(str(script))}
        ensure_fory_compiler
        test -L "$HOME/.local/bin/foryc"
        test "$(readlink "$HOME/.local/bin/foryc")" = {json.dumps(str(foryc))}
    """
    result = subprocess.run(["bash", "--noprofile", "--norc", "-c", command], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (home / ".local/bin/foryc").is_symlink()
