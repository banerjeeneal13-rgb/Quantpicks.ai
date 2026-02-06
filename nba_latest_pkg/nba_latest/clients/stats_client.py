from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

from ..cache import cache_path, read_cache, write_cache
from ..http import DEFAULT_HEADERS, RateLimiter, request_json_with_backoff


STATS_SCOREBOARD_URL = "https://stats.nba.com/stats/scoreboardv2"


@dataclass
class FetchResult:
    data: dict[str, Any]
    stale: bool


@dataclass
class StatsClient:
    cache_dir: Path
    ttl_seconds: int
    timeout_s: float
    max_retries: int
    backoff_base_s: float
    rate_limiter: RateLimiter

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        # stats.nba.com is picky; keep additional headers similar to browser.
        self.session.headers.update(
            {
                "Host": "stats.nba.com",
                "Connection": "keep-alive",
                "x-nba-stats-token": "true",
                "x-nba-stats-origin": "stats",
            }
        )

    def fetch_scoreboard(self, game_date: date) -> FetchResult:
        params = {
            "GameDate": game_date.strftime("%m/%d/%Y"),
            "LeagueID": "00",
            "DayOffset": 0,
        }
        cache_file = cache_path(self.cache_dir, STATS_SCOREBOARD_URL, params)
        cached, fresh = read_cache(cache_file, self.ttl_seconds)
        if cached and fresh:
            return FetchResult(data=cached, stale=False)

        try:
            data = request_json_with_backoff(
                self.session,
                url=STATS_SCOREBOARD_URL,
                params=params,
                timeout_s=self.timeout_s,
                max_retries=self.max_retries,
                backoff_base_s=self.backoff_base_s,
                rate_limiter=self.rate_limiter,
            )
            write_cache(cache_file, data)
            return FetchResult(data=data, stale=False)
        except Exception:
            if cached:
                return FetchResult(data=cached, stale=True)
            raise
