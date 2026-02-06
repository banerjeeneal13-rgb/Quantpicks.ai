"""Normalization helpers for SportsGameOdds."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from .market_discovery import map_market


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def implied_prob(decimal_odds: float) -> float | None:
    if not decimal_odds or decimal_odds <= 1:
        return None
    return 1 / decimal_odds


def normalize_overround(probs: dict[str, float]) -> dict[str, float]:
    total = sum(probs.values())
    if total <= 0:
        return probs
    return {k: v / total for k, v in probs.items()}


def _stringify(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def normalize_event_odds(
    event: dict[str, Any],
    odds_payload: dict[str, Any],
    fetch_run_id: str,
    timestamp_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    event_id = str(event.get("eventID") or event.get("id") or "")
    event_start = str(event.get("startTimeUTC") or event.get("start_time_utc") or "")
    home_team = str(event.get("homeTeam") or event.get("home_team") or "")
    away_team = str(event.get("awayTeam") or event.get("away_team") or "")

    # Expected structure: event.odds.<oddID>.byBookmaker.<bookmakerID>
    odds_map = odds_payload or {}

    # Group for overround adjustment.
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for odd_id, odd_obj in odds_map.items():
        market_name = str(odd_obj.get("name") or odd_obj.get("marketName") or odd_id)
        market_key, stat_key = map_market(odd_id, market_name)

        by_book = odd_obj.get("byBookmaker") or {}
        for book_id, book_obj in by_book.items():
            last_updated = book_obj.get("lastUpdatedAt") or book_obj.get("last_update")
            outcomes = book_obj.get("outcomes") or book_obj.get("odds") or []

            for outcome in outcomes:
                player_name = str(outcome.get("playerName") or outcome.get("player") or "")
                player_id = str(outcome.get("playerID") or outcome.get("playerId") or "")
                line = outcome.get("line")
                price_decimal = outcome.get("priceDecimal") or outcome.get("oddsDecimal")
                price_american = outcome.get("priceAmerican") or outcome.get("oddsAmerican")
                side = str(outcome.get("outcome") or outcome.get("side") or outcome.get("team") or "")

                if not price_decimal:
                    continue

                row = {
                    "timestamp_utc": timestamp_utc,
                    "league": "NBA",
                    "event_id": event_id,
                    "event_start_utc": event_start,
                    "home_team": home_team,
                    "away_team": away_team,
                    "sportsbook": str(book_id),
                    "market_key": market_key,
                    "stat_key": stat_key,
                    "player_name": player_name,
                    "player_id": player_id,
                    "outcome": side,
                    "line": line if line is not None else "",
                    "price_american": price_american or "",
                    "price_decimal": float(price_decimal),
                    "implied_prob": None,
                    "adj_implied_prob": None,
                    "source": "sportsgameodds",
                    "fetch_run_id": fetch_run_id,
                    "last_updated_at": last_updated or "",
                    "extra_json": _stringify({"odd_id": odd_id, "market_name": market_name, "raw": outcome}),
                }

                group_key = "|".join([
                    timestamp_utc[:16],
                    str(book_id),
                    event_id,
                    market_key,
                    player_id or player_name,
                ])

                groups[group_key].append(row)

    # Apply implied prob and overround adjustments within groups.
    for _, group_rows in groups.items():
        probs = {}
        for r in group_rows:
            p = implied_prob(float(r["price_decimal"]))
            if p is None:
                continue
            probs[r["outcome"]] = p
        adj = normalize_overround(probs) if probs else {}

        for r in group_rows:
            p = implied_prob(float(r["price_decimal"]))
            r["implied_prob"] = p
            r["adj_implied_prob"] = adj.get(r["outcome"])
            rows.append(r)

    return rows
