"""Append-only JSONL event log and current-run metadata."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PIPELINE_LOG_ROOT


def run_directory(run_id: str) -> Path:
    directory = PIPELINE_LOG_ROOT / run_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def append_event(run_id: str, stage: str, state: str, message: str, **details: Any) -> dict[str, Any]:
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "stage": stage,
        "state": state,
        "message": message,
        "details": details,
    }
    with (run_directory(run_id) / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, default=str) + "\n")
    return event


def save_run_result(run_id: str, result: dict[str, Any], current: bool = True) -> None:
    path = run_directory(run_id) / "result.json"
    path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    if current:
        latest = PIPELINE_LOG_ROOT / "current.json"
        temporary = latest.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        temporary.replace(latest)


def load_current_result() -> dict[str, Any] | None:
    path = PIPELINE_LOG_ROOT / "current.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_events(run_id: str) -> list[dict[str, Any]]:
    path = run_directory(run_id) / "events.jsonl"
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
