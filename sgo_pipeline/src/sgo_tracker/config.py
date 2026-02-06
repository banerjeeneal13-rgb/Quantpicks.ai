"""Configuration for sgo_tracker."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Pattern
import re


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_key: str | None
    request_timeout_s: int
    max_retries: int
    backoff_base_s: float
    league_id: str


def load_settings() -> Settings:
    return Settings(
        base_url=os.getenv("SGO_BASE_URL", "https://api.sportsgameodds.com/v2").rstrip("/"),
        api_key=os.getenv("SPORTS_GAME_ODDS_API_KEY"),
        request_timeout_s=int(os.getenv("REQUEST_TIMEOUT_S", "20")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        backoff_base_s=float(os.getenv("BACKOFF_BASE_S", "0.5")),
        league_id=os.getenv("LEAGUE_ID", "NBA"),
    )


def default_market_rules() -> list[tuple[Pattern[str], tuple[str, str]]]:
    """Regex rules to map oddID or market names to canonical market_key/stat_key."""
    rules = {
        r"moneyline|ml": ("moneyline", "points"),
        r"spread|handicap": ("spread", "points"),
        r"total|over/under": ("total", "points"),
        r"points\s*\+\s*rebounds\s*\+\s*assists|pra": ("pra", "pra"),
        r"points\s*\+\s*assists|pa": ("pa", "pa"),
        r"points\s*\+\s*rebounds|pr": ("pr", "pr"),
        r"rebounds\s*\+\s*assists|ra": ("ra", "ra"),
        r"blocks\s*\+\s*steals|stocks": ("stocks", "stocks"),
        r"player\s*points|points\s*player": ("player_points", "points"),
        r"player\s*rebounds|rebounds\s*player": ("player_rebounds", "rebounds"),
        r"player\s*assists|assists\s*player": ("player_assists", "assists"),
        r"three\s*pointers\s*made|3pt|3pt\s*made|threes": ("player_threes", "threes"),
    }
    return [(re.compile(k, re.IGNORECASE), v) for k, v in rules.items()]
