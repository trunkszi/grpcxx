"""Exercise native driver tool precedence without depending on installed generators."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    driver = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="grpcxx tool selection ") as temporary:
        root = Path(temporary)
        home = root / "home"
        local = home / ".local/bin"
        fallback = root / "path tools"
        explicit = root / "overrides"
        for directory in (local, fallback, explicit):
            directory.mkdir(parents=True)
        log = root / "calls.jsonl"
        environment = {key: value for key, value in os.environ.items()
                       if key not in {"PROTOC", "FORYC", "FORY_PYTHON", "GRPCXX_CAPNP_PLUGIN"}}
        environment.update(HOME=str(home), PATH=str(fallback), GRPCXX_TOOL_LOG=str(log))

        def fake(directory, name):
            path = directory / name
            path.write_text(
                "#!/usr/bin/python3\nimport json, os, sys\n"
                "with open(os.environ['GRPCXX_TOOL_LOG'], 'a') as stream:\n"
                "    stream.write(json.dumps(sys.argv) + '\\n')\n"
            )
            path.chmod(0o755)
            return str(path)

        for name in ("protoc", "foryc", "python3", "capnp"):
            fake(fallback, name)

        def run(extension, options=(), env=None):
            log.unlink(missing_ok=True)
            result = subprocess.run(
                [str(driver), "--out", str(root / "output"), *options,
                 str(root / f"input with spaces.{extension}")],
                env=env or environment, capture_output=True, text=True, check=False,
            )
            assert result.returncode == 0, result.stderr
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            assert all(any(arg.endswith(f"input with spaces.{extension}") for arg in call[1:]) for call in calls)
            return [call[0] for call in calls]

        assert run("proto") == [str(fallback / "protoc")]
        assert run("fdl") == [str(fallback / "foryc"), str(fallback / "python3")]
        assert run("capnp") == [str(fallback / "capnp")]
        for name in ("protoc", "foryc", "python3", "capnp"):
            fake(local, name)
        assert run("proto") == [str(local / "protoc")]
        assert run("fdl") == [str(local / "foryc"), str(local / "python3")]
        assert run("capnp") == [str(local / "capnp")]
        overrides = dict(environment)
        for variable, name in (("PROTOC", "protoc"), ("FORYC", "foryc"), ("FORY_PYTHON", "python3")):
            overrides[variable] = fake(explicit, name)
        assert run("proto", env=overrides) == [str(explicit / "protoc")]
        assert run("fdl", env=overrides) == [str(explicit / "foryc"), str(explicit / "python3")]
        assert run("proto", ("--protoc", str(local / "protoc")), overrides) == [str(local / "protoc")]
        (local / "python3").unlink()
        (local / "python3").symlink_to(explicit / "python3")
        assert run("fdl") == [str(local / "foryc"), str(explicit / "python3")]
        empty_overrides = dict(environment, PROTOC="", FORYC="", FORY_PYTHON="")
        assert run("proto", env=empty_overrides) == [str(local / "protoc")]
    print("Tool overrides, HOME defaults, PATH fallback, and spaced argv passed")


if __name__ == "__main__":
    main()
