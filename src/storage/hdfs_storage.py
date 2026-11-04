"""Small, explicit HDFS CLI wrapper for the single-node WSL deployment."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from config.settings import SETTINGS, hdfs_uri


class HdfsError(RuntimeError):
    pass


class HdfsStorage:
    def __init__(self, timeout_seconds: int = 120, executable: str = "hdfs") -> None:
        self.timeout_seconds = timeout_seconds
        self.executable = executable

    def _run(self, args: list[str], timeout: int | None = None) -> str:
        command = [self.executable, "dfs", *args]
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout or self.timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HdfsError(f"HDFS command could not complete: {exc}") from exc
        if result.returncode:
            detail = (result.stderr or result.stdout).strip()[-2000:]
            raise HdfsError(f"HDFS command failed ({result.returncode}): {detail}")
        return result.stdout.strip()

    def mkdir(self, path: str) -> None:
        self._run(["-mkdir", "-p", path])

    def exists(self, path: str) -> bool:
        result = subprocess.run(
            [self.executable, "dfs", "-test", "-e", path],
            check=False,
            capture_output=True,
            timeout=self.timeout_seconds,
        )
        if result.returncode not in (0, 1):
            detail = result.stderr.decode(errors="replace")[-1000:]
            raise HdfsError(f"Could not check HDFS path {path}: {detail}")
        return result.returncode == 0

    def put_file(self, local_path: str | Path, hdfs_path: str) -> str:
        source = Path(local_path).resolve()
        if not source.is_file():
            raise HdfsError(f"Source file does not exist: {source}")
        if self.exists(hdfs_path):
            raise HdfsError(f"Refusing to overwrite existing Bronze path: {hdfs_path}")
        parent = hdfs_path.rsplit("/", 1)[0]
        self.mkdir(parent)
        self._run(["-put", str(source), hdfs_path], timeout=max(self.timeout_seconds, 600))
        return hdfs_uri(hdfs_path)

    def list(self, path: str) -> list[str]:
        output = self._run(["-ls", path])
        return [line.split()[-1] for line in output.splitlines() if line.strip() and not line.startswith("Found ")]

    def read_text(self, path: str, max_bytes: int = 1_000_000) -> str:
        size = int(self._run(["-du", "-s", path]).split()[0])
        if size > max_bytes:
            raise HdfsError(f"Refusing to read {size} bytes into the driver (limit {max_bytes}).")
        return self._run(["-cat", path])

    def read_json(self, path: str) -> dict[str, Any]:
        return json.loads(self.read_text(path))

    def size_bytes(self, path: str) -> int:
        output = self._run(["-du", "-s", path])
        return int(output.split()[0])


def configured_storage() -> HdfsStorage:
    return HdfsStorage(timeout_seconds=SETTINGS.command_timeout_seconds)
