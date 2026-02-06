"""Market discovery and mapping for SportsGameOdds."""
from __future__ import annotations

import json
from typing import Any

from .config import default_market_rules


def discover_markets(markets_payload: Any) -> dict[str, Any]:
    return {"raw": markets_payload}


def map_market(odd_id: str, market_name: str) -> tuple[str, str]:
    rules = default_market_rules()
    haystack = f"{odd_id} {market_name}".strip()
    for pattern, mapped in rules:
        if pattern.search(haystack):
            return mapped
    return "unknown", "unknown"


def write_markets_json(path: str, markets_payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(markets_payload, f, ensure_ascii=False, indent=2)
