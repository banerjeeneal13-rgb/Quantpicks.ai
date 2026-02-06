from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

import requests


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class RateLimiter:
    min_interval_s: float = 1.0
    _last_time: float = 0.0

    def wait(self) -> None:
        now = time.time()
        elapsed = now - self._last_time
        if elapsed < self.min_interval_s:
            time.sleep(self.min_interval_s - elapsed)
        self._last_time = time.time()


def request_json_with_backoff(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout_s: float,
    max_retries: int,
    backoff_base_s: float,
    rate_limiter: Optional[RateLimiter] = None,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            if rate_limiter:
                rate_limiter.wait()
            resp = session.get(url, params=params, timeout=timeout_s)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            sleep_s = backoff_base_s * (2 ** attempt)
            time.sleep(sleep_s)
    raise RuntimeError(f"Request failed for {url}: {last_exc}")
