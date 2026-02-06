"""Utility helpers."""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from typing import Any

_SUFFIXES = (" jr", " sr", " ii", " iii", " iv", " v")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_player_name(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    s = s.replace("’", "'")
    s = s.replace("'", "")
    s = s.replace(".", "")
    for suf in _SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def decimal_from_american(american: float) -> float:
    if american is None or american == 0:
        return math.nan
    if american > 0:
        return 1 + american / 100.0
    return 1 + 100.0 / abs(american)


def american_from_decimal(decimal: float) -> int | None:
    if decimal is None or decimal <= 1:
        return None
    if decimal >= 2:
        return int(round((decimal - 1) * 100))
    return int(round(-100 / (decimal - 1)))


def implied_prob_from_decimal(decimal: float) -> float | None:
    if decimal is None or decimal <= 1:
        return None
    return 1.0 / decimal


def normalize_overround(probs: dict[str, float]) -> dict[str, float]:
    total = sum(probs.values())
    if total <= 0:
        return probs
    return {k: v / total for k, v in probs.items()}


def stringify_extra(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def group_key(*parts: Any) -> str:
    return "|".join([str(p or "") for p in parts])


def parse_iso_datetime(value: str | None) -> str | None:
    if not value:
        return None
    s = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def sanitize_book(book: str | None) -> str | None:
    if not book:
        return None
    return str(book).strip().lower()
