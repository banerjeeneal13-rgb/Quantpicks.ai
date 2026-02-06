"""Normalization helpers and market mapping."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable

from .providers.base import PropOutcome
from .utils import (
    implied_prob_from_decimal,
    normalize_player_name,
    normalize_overround,
    stringify_extra,
    utc_now_iso,
    sanitize_book,
    group_key,
)

MARKET_CANONICAL = {
    "player_points": ("points", "points"),
    "player_rebounds": ("rebounds", "rebounds"),
    "player_assists": ("assists", "assists"),
    "player_threes": ("threes", "threes"),
    "player_points_rebounds_assists": ("pra", "pra"),
    "player_points_assists": ("pa", "pa"),
    "player_points_rebounds": ("pr", "pr"),
    "player_rebounds_assists": ("ra", "ra"),
    "player_blocks_steals": ("stocks", "stocks"),
}


def canonical_market(market_key: str) -> tuple[str, str] | None:
    return MARKET_CANONICAL.get(market_key)


def normalize_outcomes(
    outcomes: Iterable[PropOutcome],
    source_name: str,
    fetch_run_id: str,
    timestamp_utc: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize PropOutcome objects into CSV-ready rows with overround adjustment."""
    ts = timestamp_utc or utc_now_iso()

    # Group by event+book+market+player+line to normalize over/under.
    groups: dict[str, list[PropOutcome]] = defaultdict(list)
    for o in outcomes:
        key = group_key(o.event.event_id, o.sportsbook, o.market_key, o.player_name, o.line)
        groups[key].append(o)

    rows: list[dict[str, Any]] = []

    for _, items in groups.items():
        implied_map: dict[str, float] = {}
        for o in items:
            implied = implied_prob_from_decimal(o.price_decimal)
            if implied is None:
                continue
            implied_map[o.outcome.lower()] = implied
        adjusted = normalize_overround(implied_map) if implied_map else {}

        for o in items:
            canonical = canonical_market(o.market_key)
            market_key = canonical[0] if canonical else o.market_key
            stat_key = canonical[1] if canonical else o.market_key
            raw_implied = implied_map.get(o.outcome.lower())
            adj_implied = adjusted.get(o.outcome.lower())

            rows.append(
                {
                    "timestamp_utc": ts,
                    "league": o.event.league,
                    "event_id": o.event.event_id,
                    "event_start_utc": o.event.event_start_utc,
                    "home_team": o.event.home_team,
                    "away_team": o.event.away_team,
                    "sportsbook": sanitize_book(o.sportsbook),
                    "market_key": market_key,
                    "stat_key": stat_key,
                    "player_name": o.player_name,
                    "player_name_norm": normalize_player_name(o.player_name),
                    "outcome": o.outcome.lower(),
                    "line": o.line,
                    "price_decimal": o.price_decimal,
                    "price_american": o.price_american,
                    "raw_implied_prob": raw_implied,
                    "adj_implied_prob": adj_implied,
                    "source": source_name,
                    "fetch_run_id": fetch_run_id,
                    "extra_json": stringify_extra(o.raw),
                }
            )

    return rows
