"""Cross-process ownership lock for the embedded Derby Hive metastore."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from config.settings import RUNTIME_ROOT


@contextmanager
def metastore_writer() -> Iterator[None]:
    """Serialize processes that open the shared embedded Derby database.

    The persistent lock file is only an OS flock rendezvous point. Kernel
    ownership is released automatically when a process exits; it is never
    removed or used as a substitute for Derby's own lock.
    """
    lock_path = Path(RUNTIME_ROOT) / "metastore" / "writer.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
