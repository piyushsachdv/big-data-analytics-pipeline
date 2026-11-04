from __future__ import annotations

import re
from datetime import datetime, timezone
from uuid import uuid4


def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"


def safe_filename(value: str) -> str:
    name = value.replace("\\", "/").split("/")[-1]
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "transactions.csv"


def format_inr(value: float | int | None) -> str:
    if value is None:
        return "₹0.00"
    return f"₹{float(value):,.2f}"
