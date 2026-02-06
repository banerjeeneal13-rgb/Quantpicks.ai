from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import requests

from ..cache import cache_path, read_cache, write_cache
from ..http import DEFAULT_HEADERS, RateLimiter, request_json_with_backoff


@dataclass
class FetchResult:
    data: dict[str, Any]
    stale: bool
    url: str


@dataclass
class ScoreboardClient:
    cache_dir: Path
    ttl_seconds: int
    timeout_s: float
    max_retries: int
    backoff_base_s: float
    rate_limiter: RateLimiter

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _try_fetch(self, url: str, params: dict[str, Any]) -> FetchResult:
        cache_file = cache_path(self.cache_dir, url, params)
        cached, fresh = read_cache(cache_file, self.ttl_seconds)
        if cached and fresh:
            return FetchResult(data=cached, stale=False, url=url)

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
            return FetchResult(data=data, stale=False, url=url)
        except Exception:
            if cached:
                return FetchResult(data=cached, stale=True, url=url)
            raise

    def fetch_scoreboard(self, game_date: date) -> FetchResult:
        ymd = game_date.strftime("%Y%m%d")
        candidates = [
            f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json",
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard.json",
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
        ]
        last_err: Optional[Exception] = None
        for url in candidates:
            try:
                return self._try_fetch(url, params={})
            except Exception as exc:
                last_err = exc
                continue
        raise RuntimeError(f"Primary scoreboard endpoint failed: {last_err}")
