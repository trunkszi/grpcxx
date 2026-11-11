"""Discriminating CLI, preset, process-output, and launcher bootstrap tests."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from invoke import Result
from typer.testing import CliRunner

from cli import cmd_format, cmd_tidy, config, preset, utils
from cli.__main__ import _with_default_command
from cli.app import app
from cli.output import Output, OutputFormat

REPOSITORY = config.PROJECT_ROOT
runner = CliRunner()


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Keep command and cleanup tests independent of the actual checkout/build tree."""
    (tmp_path / "CMakePresets.json").write_text((REPOSITORY / "CMakePresets.json").read_text())
    for module in (config, preset, utils):
        monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    return tmp_path


@pytest.fixture
def commands(project, monkeypatch):
    """Record child argv; model CTest discovery without requiring a C++ build."""
    calls = []
    tests = [
        {"name": "Types.Ready", "command": [str(project / "bin/test_core_types")]},
        {"name": "Types.Pending", "command": [str(project / "bin/test_core_types")]},
        {"name": "Router.Dispatch", "command": [str(project / "bin/test_router")]},
    ]

    def fake_run(self, cmd, **kwargs):
        calls.append(cmd)
        stdout = json.dumps({"tests": tests}) if "--show-only=json-v1" in cmd else ""
        return Result(exited=0, stdout=stdout, stderr="")

    monkeypatch.setattr(Output, "run_command", fake_run)
    return calls


def invoke(argv):
    return runner.invoke(app, _with_default_command(argv))


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        ([], ["build"]),
        (["-t", "example"], ["build", "-t", "example"]),
        (["--preset", "gcc-musl"], ["--preset", "gcc-musl", "build"]),
        (["--format", "json", "test"], ["--format", "json", "test"]),
        (["--format=json", "--clean"], ["--format=json", "build", "--clean"]),
        (["-j", "2", "test"], ["-j", "2", "test"]),
        (["--help"], ["--help"]),
        (["--version"], ["--version"]),
    ],
)
def test_default_injection(argv, expected):
    assert _with_default_command(argv) == expected


def test_preset_resolution(project):
    assert preset.resolve_preset() == "gcc-glibc"
    for name in ("gcc-glibc", "gcc-musl", "clang-glibc", "clang-musl", "gcc-asan", "clang-asan"):
        assert preset.resolve_preset(name) == name
        assert preset.resolve_preset(compiler=name) == name
    assert preset.resolve_preset(compiler="musl-clang") == "clang-musl"
    with pytest.raises(ValueError, match="conflicts"):
        preset.resolve_preset("gcc-glibc", "clang")
    with pytest.raises(ValueError, match="Unknown preset"):
        preset.resolve_preset("../../outside")
    with pytest.raises(ValueError, match="Unknown preset"):
        preset.resolve_preset("gcc-glibc-vcpkg")


@pytest.mark.parametrize("command", ["build", "test", "bench", "clean", "format", "tidy", "setup"])
def test_command_help(command):
    result = invoke([command, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output


def test_root_help_and_version():
    assert invoke(["--help"]).exit_code == 0
    result = invoke(["--version"])
    assert result.exit_code == 0, result.output
    assert "build.py 1.0.0" in result.output


def test_build_preserves_global_options(commands):
    result = invoke(["-c", "musl-gcc", "-j", "2", "build", "-t", "example"])
    assert result.exit_code == 0, result.output
    assert commands == [
        ["cmake", "--preset", "gcc-musl"],
        ["cmake", "--build", "--preset", "gcc-musl", "--parallel", "2", "--target", "example"],
    ]


def test_test_listing_configures_without_build(commands):
    result = invoke(["--format", "json", "test", "--list"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["success"]
    assert data["events"][-1]["data"]["rows"][0] == ["Types.Ready", "test_core_types"]
    assert commands[0] == ["cmake", "--preset", "gcc-glibc"]
    assert not any("--build" in command for command in commands)


def test_target_filters_use_executable_mapping(project, commands):
    directory = project / "build/gcc-glibc"
    directory.mkdir(parents=True)
    (directory / "CMakeCache.txt").touch()
    result = invoke(["--format", "json", "test", "--no-build", "--filter", "test_core_types", "-L", "unit"])
    assert result.exit_code == 0, result.output
    assert not any(command[0] == "cmake" for command in commands)
    command = commands[-1]
    assert command[command.index("-R") + 1] == r"^(Types\.Ready|Types\.Pending)$"
    assert command[-2:] == ["-L", "unit"]
    assert "--no-tests=error" in command


def test_empty_selection_is_failure(commands):
    result = invoke(["--format", "json", "test", "--filter", "missing", "--list"])
    assert result.exit_code == 1
    assert not json.loads(result.output)["success"]


def test_no_build_requires_configured_tree(commands):
    result = invoke(["--format", "json", "test", "--no-build"])
    assert result.exit_code == 1
    assert "No configured build directory" in result.output
    assert commands == []


def test_bench_listing_never_builds(commands):
    result = invoke(["--format", "json", "bench", "--list"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)["events"][-1]["data"]["rows"]
    assert ["bench_codecs", "bench_multi_codec"] in rows
    assert len(rows) == 5
    assert commands == []


@pytest.mark.parametrize(
    ("args", "target"),
    [
        ([], "bench_aligned_framing"),
        (["-t", "bench_codecs"], "bench_multi_codec"),
    ],
)
def test_bench_default_and_alias(project, commands, args, target):
    directory = project / "build/gcc-glibc/bin"
    directory.mkdir(parents=True)
    (directory / target).touch()
    result = invoke(["bench", *args])
    assert result.exit_code == 0, result.output
    assert commands[1][-2:] == ["--target", target]
    assert commands[2] == [str(directory / target), "--benchmark_min_time=0.01s"]


def test_configure_failure_stops_build(project, monkeypatch):
    calls = []

    def fail(self, cmd, **kwargs):
        calls.append(cmd)
        return Result(exited=7, stdout="", stderr="configure failed")

    monkeypatch.setattr(Output, "run_command", fail)
    result = invoke(["--format", "json", "build"])
    assert result.exit_code == 7
    assert json.loads(result.output)["exit_code"] == 7
    assert len(calls) == 1


def test_invalid_preset_is_structured(commands):
    result = invoke(["--format", "json", "build", "--preset", "missing"])
    assert result.exit_code == 1
    assert not json.loads(result.output)["success"]
    assert commands == []


def test_clean_rejects_symlinks(project, commands, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "keep"
    marker.touch()
    directory = project / "build"
    directory.mkdir()
    (directory / "gcc-glibc").symlink_to(outside, target_is_directory=True)
    assert invoke(["clean"]).exit_code == 1
    assert marker.exists()


def test_format_check_selects_sources(project, commands, monkeypatch):
    source = project / "src/example.cc"
    source.parent.mkdir()
    source.write_text("int x;\n")
    monkeypatch.setattr(cmd_format, "find_clang_tool", lambda name: name)
    result = invoke(["format", "--check", "src"])
    assert result.exit_code == 0, result.output
    assert commands == [["clang-format", "--dry-run", "--Werror", str(source)]]


def test_tidy_uses_compilation_database(project, commands, monkeypatch):
    source = project / "src/example.cc"
    source.parent.mkdir()
    source.touch()
    directory = project / "build/gcc-glibc"
    directory.mkdir(parents=True)
    sdk = project / "sdk"
    (sdk / "bin").mkdir(parents=True)
    (sdk / "bin/clang++").touch()
    meta = sdk / "x86_64-unknown-linux-gnu/sysroot/usr/include/c++/v1/meta"
    meta.parent.mkdir(parents=True)
    meta.touch()
    tool = str(sdk / "bin/clang-tidy")
    entry = {
        "directory": str(project),
        "file": str(source),
        "arguments": [
            "g++",
            "--sysroot=/",
            "-freflection",
            "-fconcepts-diagnostics-depth=3",
            "-DKEEP=1",
            "-c",
            str(source),
        ],
    }
    original = json.dumps([entry, entry])
    (directory / "compile_commands.json").write_text(original)
    monkeypatch.setattr(cmd_tidy, "find_clang_tool", lambda name: tool)
    monkeypatch.setattr(cmd_tidy, "_supports_p2996_reflection", lambda candidate: True)

    def check_database(self, command, **kwargs):
        commands.append(command)
        entries = json.loads((Path(command[2]) / "compile_commands.json").read_text())
        assert len(entries) == 1
        arguments = entries[0]["arguments"]
        assert arguments[0] == str(sdk / "bin/clang++")
        assert "-freflection-latest" not in arguments and "-freflection" not in arguments
        assert "-fconcepts-diagnostics-depth=3" not in arguments
        assert "-DKEEP=1" in arguments
        assert f"--sysroot={sdk / 'x86_64-unknown-linux-gnu/sysroot'}" in arguments
        assert any(arg.startswith("--checks=") for arg in command)
        assert "--extra-arg=-freflection" in command
        assert "--warnings-as-errors=*" in command
        return Result(exited=0)

    monkeypatch.setattr(Output, "run_command", check_database)
    result = invoke(["tidy", "--fix", "src"])
    assert result.exit_code == 0, result.output
    assert commands[0][0] == tool and commands[0][-1] == "--fix"
    assert commands[0][3] == str(source)
    assert not Path(commands[0][2]).exists()
    assert (directory / "compile_commands.json").read_text() == original
    monkeypatch.setattr(Output, "run_command", lambda self, command, **kwargs: Result(exited=7))
    failed = invoke(["tidy", "src"])
    assert failed.exit_code == 7
    assert not list(directory.glob("grpcxx-tidy-*"))

    monkeypatch.setattr(cmd_tidy, "_supports_p2996_reflection", lambda candidate: False)
    fallback_commands = []

    def check_fallback(self, command, **kwargs):
        fallback_commands.append(command)
        entries = json.loads((Path(command[2]) / "compile_commands.json").read_text())
        arguments = entries[0]["arguments"]
        assert "-freflection-latest" not in arguments and "-freflection" not in arguments
        assert "--extra-arg=-D__is_consteval_only(...)=false" in command
        assert "--extra-arg=-ferror-limit=0" in command
        return Result(
            exited=1,
            stdout=f"{source}:10:15: error: iterating expansion statements are not yet supported [clang-diagnostic-error]\n",
            stderr="",
        )

    monkeypatch.setattr(Output, "run_command", check_fallback)
    fallback_ok = invoke(["tidy", "src"])
    assert fallback_ok.exit_code == 0, fallback_ok.output
    assert len(fallback_commands) == 1

    monkeypatch.delenv("GRPCXX_CLANG_TIDY", raising=False)
    monkeypatch.setenv("CLANG_TIDY", "/custom/clang-tidy")
    monkeypatch.setattr(utils.shutil, "which", lambda name, **kwargs: name if name == "/custom/clang-tidy" else None)
    assert utils.find_clang_tool("clang-tidy") == "/custom/clang-tidy"
    assert utils.find_clang_tidy() == "/custom/clang-tidy"
    monkeypatch.setenv("GRPCXX_CLANG_TIDY", "/override/clang-tidy-23")
    monkeypatch.setattr(
        utils.shutil, "which", lambda name, **kwargs: name if name == "/override/clang-tidy-23" else None
    )
    assert utils.find_clang_tidy() == "/override/clang-tidy-23"
    monkeypatch.setenv("GRPCXX_CLANG_TIDY", "/missing/grpcxx-clang-tidy")
    with pytest.raises(ValueError, match="GRPCXX_CLANG_TIDY is not executable"):
        utils.find_clang_tidy()
    monkeypatch.delenv("GRPCXX_CLANG_TIDY", raising=False)
    monkeypatch.setenv("CLANG_TIDY", "/missing/clang-tidy")
    with pytest.raises(ValueError, match="CLANG_TIDY is not executable"):
        utils.find_clang_tool("clang-tidy")
    monkeypatch.delenv("CLANG_TIDY")
    monkeypatch.setattr(
        utils.shutil,
        "which",
        lambda name, **kwargs: (
            "/opt/clang-p2996/bin/clang-tidy" if kwargs.get("path") == "/opt/clang-p2996/bin" else None
        ),
    )
    assert utils.find_clang_tool("clang-tidy") == "/opt/clang-p2996/bin/clang-tidy"
    monkeypatch.setattr(
        utils.shutil,
        "which",
        lambda name, **kwargs: name if not kwargs.get("path") and name in ("clang-tidy", "clang-tidy-23") else None,
    )
    assert utils.find_clang_tool("clang-tidy") == "clang-tidy-23"
    monkeypatch.setattr(
        utils.shutil, "which", lambda name, **kwargs: name if not kwargs.get("path") and name == "clang-tidy" else None
    )
    assert utils.find_clang_tool("clang-tidy") == "clang-tidy"
    monkeypatch.setattr(
        utils.shutil,
        "which",
        lambda name, **kwargs: (
            name if not kwargs.get("path") and name == "/opt/clang-tidy-23.1.6/clang-tidy-23" else None
        ),
    )
    assert utils.find_clang_tidy() == "/opt/clang-tidy-23.1.6/clang-tidy-23"


def test_setup_delegates_to_ci_script_with_supported_presets():
    installer = (REPOSITORY / "scripts/install-dependencies.sh").read_text()
    script = (REPOSITORY / ".github/scripts/setup-ci-deps.sh").read_text()
    assert "setup-ci-deps.sh" in installer
    assert "ensure_vcpkg" in installer
    assert 'required_version="4.4.5"' in installer
    assert "ensure_uarch_port" in script
    assert "ensure_uring_port" in script
    assert "ensure_cares_port" in script
    assert "ensure_nexora_port" in script
    assert "ensure_fory_port" in script
    assert 'install_one_toolchain gcc-glibc' in script


def test_classify_clang_diagnostics_and_tidy_failure(tmp_path):
    assert "iterating expansion statements are not yet supported" in cmd_tidy.REFLECTION_DIAGNOSTIC_MARKERS

    tidy_23_0_0 = (
        "src/router.h:12:15: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
        "src/router.h:14:30: error: blocks support disabled - compile with -fblocks or pick a deployment target "
        "that supports them [clang-diagnostic-error]\n"
    )
    ok_230, unsup_230 = cmd_tidy._classify_clang_diagnostics(tidy_23_0_0)
    assert ok_230 and unsup_230 == []
    failed_230, display_230 = cmd_tidy._tidy_failure(tidy_23_0_0, status=1, capable=False)
    assert not failed_230 and display_230 == ""

    tidy_23_1_6 = (
        "src/client_stub.h:72:40: error: iterating expansion statements are not yet supported "
        "[clang-diagnostic-error]\n"
        "src/client_stub.h:75:12: error: expanding expression statement [clang-diagnostic-error]\n"
    )
    ok_2316, unsup_2316 = cmd_tidy._classify_clang_diagnostics(tidy_23_1_6)
    assert ok_2316 and unsup_2316 == []
    failed_2316, display_2316 = cmd_tidy._tidy_failure(tidy_23_1_6, status=1, capable=False)
    assert not failed_2316 and display_2316 == ""

    cascade_note = (
        "src/annotations.h:28:15: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
        "tests/router_test.cc:86:3: error: constraints not satisfied for class template 'ServiceRouter' "
        "[clang-diagnostic-error]\n"
        "src/annotations.h:256:27: note: because '!reflection_detail::ServiceName<EchoService>().empty()' "
        "evaluated to false\n"
    )
    blocks, unlocated = cmd_tidy._parse_diagnostic_blocks(cascade_note)
    assert len(blocks) == 2 and not unlocated
    assert cmd_tidy._has_reflection_note(blocks[1], {"src/annotations.h"})
    ok_cascade, unsup_cascade = cmd_tidy._classify_clang_diagnostics(cascade_note)
    assert ok_cascade and unsup_cascade == []
    failed_cascade, display_cascade = cmd_tidy._tidy_failure(cascade_note, status=1, capable=False)
    assert not failed_cascade and display_cascade == ""

    use_after_move = (
        f"{tidy_23_0_0}tests/codec_test.cc:441:3: warning: 'source' used after it was moved [bugprone-use-after-move]\n"
    )
    failed_uam, display_uam = cmd_tidy._tidy_failure(use_after_move, status=1, capable=False)
    assert failed_uam
    assert "[bugprone-use-after-move]" in display_uam

    analyzer_warn = (
        f"{tidy_23_1_6}"
        "benches/framing.cc:48:8: warning: Value stored to '_' during its initialization is never read "
        "[clang-analyzer-deadcode.DeadStores]\n"
    )
    failed_ana, display_ana = cmd_tidy._tidy_failure(analyzer_warn, status=1, capable=False)
    assert failed_ana
    assert "[clang-analyzer-deadcode.DeadStores]" in display_ana

    uring_splice = (
        "payload_codec.hpp:76:19: error: expected a type [clang-diagnostic-error]\n"
        "   76 |     using Field = [:std::meta::type_of(field):];\n"
        "payload_codec.hpp:77:12: error: use of undeclared identifier 'Field' [clang-diagnostic-error]\n"
        "operation_slab.hpp:55:40: error: iterating expansion statements are not yet supported "
        "[clang-diagnostic-error]\n"
        "operation_slab.hpp:61:24: error: use of undeclared identifier 'Index' [clang-diagnostic-error]\n"
    )
    ok_splice, unsup_splice = cmd_tidy._classify_clang_diagnostics(uring_splice)
    assert ok_splice and unsup_splice == []
    failed_splice, display_splice = cmd_tidy._tidy_failure(uring_splice, status=1, capable=False)
    assert not failed_splice and display_splice == ""

    bogus_same_file = (
        "src/router.h:12:15: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
        "src/router.h:45:5: error: use of undeclared identifier 'bogus_symbol' [clang-diagnostic-error]\n"
    )
    ok_bogus, unsup_bogus = cmd_tidy._classify_clang_diagnostics(bogus_same_file)
    assert not ok_bogus and any("bogus_symbol" in line for line in unsup_bogus)
    failed_bogus, display_bogus = cmd_tidy._tidy_failure(bogus_same_file, status=1, capable=False)
    assert failed_bogus and "bogus_symbol" in display_bogus

    invalid_std = "error: invalid value 'c++99' in '-std=c++99' [clang-diagnostic-error]\n"
    ok_std, unsup_std = cmd_tidy._classify_clang_diagnostics(invalid_std)
    assert not ok_std and unsup_std
    failed_std, display_std = cmd_tidy._tidy_failure(invalid_std, status=1, capable=False)
    assert failed_std and "invalid value" in display_std

    failed_sig, _ = cmd_tidy._tidy_failure(tidy_23_0_0, status=-9, capable=False)
    assert failed_sig
    failed_empty, _ = cmd_tidy._tidy_failure("", status=1, capable=False)
    assert failed_empty

    routes_hdr = tmp_path / "completion_routes.hpp"
    routes_hdr.write_text(
        "void f(const uring::Delivery* delivery) {\n"
        "  using Ret = decltype(delivery->completion);\n"
        "  if constexpr (std::is_void_v<Ret>) {}\n"
        "}\n"
    )
    nexora_cascade = (
        f"{tidy_23_0_0}"
        "parked_registry.hpp:114:41: error: variable does not have a constant initializer [clang-diagnostic-error]\n"
        "  114 |   constinit thread_local ParkedRegistry registry{};\n"
        f"{routes_hdr}:3:32: error: use of undeclared identifier 'Ret' [clang-diagnostic-error]\n"
    )
    assert cmd_tidy._classify_clang_diagnostics(nexora_cascade) == (True, [])


def test_setup_routes_to_installer(commands):
    result = invoke(["setup", "--vcpkg"])
    assert result.exit_code == 0, result.output
    assert Path(commands[0][0]).name == "install-dependencies.sh"
    assert commands[0][-1] == "--vcpkg"


def test_json_captures_child_failure(capsys, tmp_path):
    out = Output(OutputFormat.json)
    result = out.run_command(
        [sys.executable, "-c", "import sys; print('child-out'); print('child-error', file=sys.stderr); sys.exit(7)"],
        cwd=tmp_path,
    )
    assert out.finish(result.return_code) == 7
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert not data["success"]
    assert data["events"][0]["data"]["stdout"] == "child-out\n"
    assert data["events"][0]["data"]["stderr"] == "child-error\n"
    assert captured.err == ""


def test_invoke_escapes_argv_and_directory(tmp_path):
    directory = tmp_path / "dir with spaces '$()"
    directory.mkdir()
    text = "$(touch forbidden); a 'literal' argument"
    result = utils.run_command([sys.executable, "-c", "import sys; print(sys.argv[1])", text], cwd=directory, hide=True)
    assert result.ok
    assert result.stdout.strip() == text
    assert not (directory / "forbidden").exists()


@pytest.mark.parametrize("mode", ["missing", "download-fails", "already-installed", "installer-incomplete"])
def test_launcher_bootstraps_missing_uv(tmp_path, mode):
    home = tmp_path / "home"
    home.mkdir()
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ("dirname", "mkdir", "env", "sh", "cat", "chmod"):
        (tools / name).symlink_to(shutil.which(name))
    (tools / "cmake").write_text("#!/bin/sh\necho 'cmake version 4.4.5'\n")
    (tools / "cmake").chmod(0o755)
    installer = """mkdir -p "$UV_INSTALL_DIR"
cat > "$UV_INSTALL_DIR/uv" <<'UV'
#!/bin/sh
printf 'uv-args:'
printf ' <%s>' "$@"
printf '\\nRUST_LOG:%s\\n' "${RUST_LOG-unset}"
printf 'PYTHONPATH:%s\\n' "$PYTHONPATH"
UV
chmod +x "$UV_INSTALL_DIR/uv"
"""
    script = "#!/bin/sh\ncat <<'INSTALL'\n" + installer + "INSTALL\n"
    if mode == "download-fails":
        script = "#!/bin/sh\nexit 22\n"
    elif mode == "installer-incomplete":
        script = "#!/bin/sh\nprintf ':'\n"
    elif mode == "already-installed":
        (tools / "uv").write_text("#!/bin/sh\necho already-installed-uv\n")
        (tools / "uv").chmod(0o755)
    (tools / "curl").write_text(script)
    (tools / "curl").chmod(0o755)
    launcher = tmp_path / "build.py"
    launcher.write_text((REPOSITORY / "build.py").read_text())
    install_dir = tmp_path / "custom install"
    environment = {
        **os.environ,
        "HOME": str(home),
        "PATH": str(tools),
        "UV_INSTALL_DIR": str(install_dir),
        "PYTHONPATH": "existing",
        "RUST_LOG": "debug",
    }
    result = subprocess.run(
        ["/bin/bash", str(launcher), "--help"], env=environment, capture_output=True, text=True, check=False
    )
    if mode == "already-installed":
        assert result.returncode == 0, result.stderr
        assert "already-installed-uv" in result.stdout
        assert not (install_dir / "uv").exists()
        assert "downloading uv" not in result.stderr
        return
    if mode in {"download-fails", "installer-incomplete"}:
        assert result.returncode != 0
        assert not (install_dir / "uv").exists()
    else:
        assert result.returncode == 0, result.stderr
        assert (install_dir / "uv").is_file()
        assert "<run> <--directory>" in result.stdout
        assert "<python> <-m> <cli> <--help>" in result.stdout
        assert "RUST_LOG:unset" in result.stdout
        assert f"PYTHONPATH:{tmp_path}/scripts:existing" in result.stdout
    assert "uv not found; downloading uv" in result.stderr


def test_launcher_and_installer_contract():
    launcher = (REPOSITORY / "build.py").read_text()
    assert launcher.startswith("#!/usr/bin/env bash\n")
    assert "ensure_uv()" in launcher
    assert "https://astral.sh/uv/install.sh" in launcher
    assert "set -euo pipefail" in launcher
    installer = (REPOSITORY / "scripts/install-dependencies.sh").read_text()
    for function in ("ensure_uv", "ensure_ninja", "ensure_cmake"):
        assert function + "()" in installer


def test_list_filter_applies_label(commands):
    result = invoke(["test", "--list", "-L", "unit"])
    assert result.exit_code == 0, result.output
    assert commands[-1][-2:] == ["-L", "unit"]


def test_verbose_reaches_build(commands):
    result = invoke(["--verbose", "build"])
    assert result.exit_code == 0, result.output
    assert "--verbose" in commands[-1]


def test_bad_regex_is_structured(commands):
    result = invoke(["--format", "json", "test", "--list", "--filter", "["])
    assert result.exit_code == 1
    assert not json.loads(result.output)["success"]


def test_clean_removes_only_selected_tree(project, commands):
    selected = project / "build/gcc-glibc"
    other = project / "build/clang-glibc"
    selected.mkdir(parents=True)
    other.mkdir()
    (selected / "artifact").touch()
    assert invoke(["clean"]).exit_code == 0
    assert not selected.exists()
    assert other.exists()


@pytest.mark.parametrize("argv", [["build", "-j", "0"], ["test", "--parallel", "-1"], ["-j", "0", "build"]])
def test_invalid_job_counts_never_run(argv, commands):
    assert invoke(argv).exit_code == 2
    assert commands == []
