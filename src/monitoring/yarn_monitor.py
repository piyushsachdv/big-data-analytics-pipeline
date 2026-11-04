"""Read actual application and node states from the YARN CLI."""

from __future__ import annotations

import re
import subprocess
from typing import Any

from config.settings import SETTINGS


class YarnMonitorError(RuntimeError):
    pass


def application_status(application_id: str, timeout_seconds: int = 15) -> dict[str, Any]:
    result = subprocess.run(
        ["yarn", "application", "-status", application_id],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if result.returncode:
        raise YarnMonitorError((result.stderr or result.stdout).strip()[-2000:])
    status: dict[str, Any] = {"application_id": application_id}
    fields = {
        "State": "state",
        "Final-State": "final_state",
        "Progress": "progress",
        "Tracking-URL": "tracking_url",
        "Diagnostics": "diagnostics",
        "Queue": "queue",
    }
    for line in result.stdout.splitlines():
        for source, target in fields.items():
            if line.strip().startswith(source + " :"):
                status[target] = line.split(":", 1)[1].strip()
    if "state" not in status:
        raise YarnMonitorError("YARN returned an application report without a state.")
    return status


def list_applications(limit: int = 30) -> list[dict[str, str]]:
    result = subprocess.run(
        ["yarn", "application", "-list", "-appStates", "ALL"],
        capture_output=True,
        text=True,
        timeout=SETTINGS.command_timeout_seconds,
        check=False,
    )
    if result.returncode:
        raise YarnMonitorError((result.stderr or result.stdout).strip()[-2000:])
    applications = []
    for line in result.stdout.splitlines():
        if re.match(r"application_\d+_\d+\s", line.strip()):
            parts = line.split()
            if len(parts) >= 7:
                applications.append(
                    {
                        "application_id": parts[0],
                        "name": parts[1],
                        "type": parts[2],
                        "user": parts[3],
                        "queue": parts[4],
                        "state": parts[5],
                        "final_state": parts[6],
                    }
                )
    return applications[-limit:]
