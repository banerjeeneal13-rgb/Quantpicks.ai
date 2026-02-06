"""Storage helpers for append-only CSV with dedupe."""
from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Iterable

IDEMPOTENCY_FIELDS = [
    "timestamp_minute",
    "sportsbook",
    "event_id",
    "market_key",
    "player_id",
    "outcome",
    "line",
    "price_decimal",
]


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _timestamp_minute(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return dt.isoformat()


def _row_key(row: dict) -> tuple:
    return tuple(str(row.get(k, "")) for k in IDEMPOTENCY_FIELDS)


def load_existing_keys(path: str) -> set[tuple]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {_row_key(row) for row in reader}


def append_rows(path: str, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    ensure_parent(path)
    file_exists = os.path.exists(path)

    for row in rows:
        row["timestamp_minute"] = _timestamp_minute(row["timestamp_utc"])

    existing_keys = load_existing_keys(path)
    new_rows = [row for row in rows if _row_key(row) not in existing_keys]

    fieldnames = list(rows[0].keys()) + ["timestamp_minute"]
    if file_exists:
        with open(path, "r", newline="", encoding="utf-8") as f:
            header = f.readline().strip().split(",")
        if header != fieldnames:
            raise RuntimeError("CSV header mismatch. Use a new output file.")

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        count = 0
        for row in new_rows:
            writer.writerow(row)
            count += 1
        return count
