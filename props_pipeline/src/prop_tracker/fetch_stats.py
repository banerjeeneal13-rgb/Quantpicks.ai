from __future__ import annotations

from typing import Any

from .config import Settings
from .stats import NBAAPIStatsProvider, APISportsStatsProvider
from .stats.base import StatsProvider


class CSVStatsProvider:
    name = "csv"

    def fetch_stats(self, props_csv: str, out_csv: str) -> str:
        # Pass-through: assumes file already exists.
        return out_csv


def get_stats_provider(settings: Settings) -> StatsProvider:
    if settings.stats_provider == "csv":
        return CSVStatsProvider()
    if settings.stats_provider == "nba_api":
        return NBAAPIStatsProvider(
            timeout_s=settings.request_timeout_s,
            max_retries=settings.max_retries,
            backoff_base_s=settings.backoff_base_s,
        )
    if settings.stats_provider == "api_sports":
        if not settings.api_sports_key:
            raise RuntimeError("API_SPORTS_KEY missing; cannot fetch stats.")
        return APISportsStatsProvider(
            api_key=settings.api_sports_key,
            timeout_s=settings.request_timeout_s,
            max_retries=settings.max_retries,
            backoff_base_s=settings.backoff_base_s,
        )
    raise RuntimeError(f"Unsupported STATS_PROVIDER: {settings.stats_provider}")
