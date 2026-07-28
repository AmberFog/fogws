import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap


SMOKE_SCRIPT = textwrap.dedent(
    """
    from importlib.metadata import version

    import fogws


    distribution_version = version("fogws")
    assert distribution_version == fogws.__version__
    assert fogws.__all__ == (
        "DEFAULT_CLOSE_TIMEOUT",
        "DEFAULT_MAX_BUFFERED_BYTES",
        "DEFAULT_MAX_MESSAGE_SIZE",
        "DEFAULT_MAX_QUEUE",
        "ConcurrencyError",
        "Connection",
        "ConnectionClosed",
        "ConnectionClosedError",
        "ConnectionClosedOK",
        "ConnectionFailedError",
        "FogWSError",
        "InvalidURIError",
        "LoopAffinityError",
        "ResourceLimitError",
        "RuntimeContextError",
        "__version__",
        "connect",
    )
    assert all(hasattr(fogws, name) for name in fogws.__all__)
    """,
)


def main() -> int:
    args = parse_args()
    wheel_path = find_wheel(args.dist_dir)

    with tempfile.TemporaryDirectory() as tmp_dir:
        smoke_dir = Path(tmp_dir)
        target_dir = smoke_dir / "site-packages"
        target_dir.mkdir()

        run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-deps",
                "--target",
                str(target_dir),
                str(wheel_path),
            ],
        )
        run([sys.executable, "-c", SMOKE_SCRIPT], cwd=smoke_dir, python_path=target_dir)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install a built FogWS wheel and run a smoke test.")
    parser.add_argument("--dist-dir", type=Path, required=True)
    return parser.parse_args()


def find_wheel(dist_dir: Path) -> Path:
    wheel_paths = sorted(dist_dir.glob("*.whl"))
    if len(wheel_paths) != 1:
        msg = f"expected exactly one wheel in {dist_dir}, found {len(wheel_paths)}"
        raise SystemExit(msg)
    return wheel_paths[0].resolve()


def run(command: list[str], *, cwd: Path | None = None, python_path: Path | None = None) -> None:
    env = os.environ.copy()
    env.pop("PYTHONHOME", None)
    if python_path is None:
        env.pop("PYTHONPATH", None)
    else:
        env["PYTHONPATH"] = str(python_path)
    subprocess.run(command, check=True, cwd=cwd, env=env)  # noqa: S603


if __name__ == "__main__":
    raise SystemExit(main())
