from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

from ..cache import cache_path, read_cache, write_cache
from ..http import DEFAULT_HEADERS, RateLimiter, request_json_with_backoff


@dataclass
class NBA_CDNClient:
    cache_dir: Path
    ttl_seconds: int
    timeout_s: float
    max_retries: int
    backoff_base_s: float
    rate_limiter: RateLimiter

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def fetch_boxscore(self, game_id: str) -> dict[str, Any]:
        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        params: dict[str, Any] = {}
        cache_file = cache_path(self.cache_dir, url, params)
        cached, fresh = read_cache(cache_file, self.ttl_seconds)
        if cached and fresh:
            return cached

        data = request_json_with_backoff(
            self.session,
            url=url,
            params=params,
            timeout_s=self.timeout_s,
            max_retries=self.max_retries,
            backoff_base_s=self.backoff_base_s,
            rate_limiter=self.rate_limiter,
        )
        write_cache(cache_file, data)
        return data

    def fetch_scoreboard(self, game_date: date) -> dict[str, Any]:
        ymd = game_date.strftime("%Y%m%d")
        candidates = [
            f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json",
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard.json",
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
        ]
        last_exc: Exception | None = None
        for url in candidates:
            params: dict[str, Any] = {}
            cache_file = cache_path(self.cache_dir, url, params)
            cached, fresh = read_cache(cache_file, self.ttl_seconds)
            if cached and fresh:
                return cached
            try:
                data = request_json_with_backoff(
                    self.session,
                    url=url,
                    params=params,
                    timeout_s=self.timeout_s,
                    max_retries=self.max_retries,
                    backoff_base_s=self.backoff_base_s,
                    rate_limiter=self.rate_limiter,
                )
                write_cache(cache_file, data)
                return data
            except Exception as exc:
                last_exc = exc
                continue
        raise RuntimeError(f"Scoreboard fetch failed: {last_exc}")

    def fetch_schedule(self) -> dict[str, Any]:
        url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
        params: dict[str, Any] = {}
        cache_file = cache_path(self.cache_dir, url, params)
        cached, fresh = read_cache(cache_file, self.ttl_seconds)
        if cached and fresh:
            return cached
        data = request_json_with_backoff(
            self.session,
            url=url,
            params=params,
            timeout_s=self.timeout_s,
            max_retries=self.max_retries,
            backoff_base_s=self.backoff_base_s,
            rate_limiter=self.rate_limiter,
        )
        write_cache(cache_file, data)
        return data
