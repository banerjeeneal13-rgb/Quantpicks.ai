from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from ..cache import cache_path, read_cache, write_cache
from ..http import DEFAULT_HEADERS, RateLimiter, request_json_with_backoff


LEAGUE_GAMELOG_URL = "https://stats.nba.com/stats/leaguegamelog"
BOXSCORE_TRAD_URL = "https://stats.nba.com/stats/boxscoretraditionalv2"
BOXSCORE_ADV_URL = "https://stats.nba.com/stats/boxscoreadvancedv2"
PBP_URL = "https://stats.nba.com/stats/playbyplayv2"


@dataclass
class StatsNBAClient:
    cache_dir: Path
    ttl_seconds: int
    timeout_s: float
    max_retries: int
    backoff_base_s: float
    rate_limiter: RateLimiter

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.headers.update(
            {
                "Host": "stats.nba.com",
                "Connection": "keep-alive",
                "x-nba-stats-token": "true",
                "x-nba-stats-origin": "stats",
            }
        )

    def _fetch(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
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

    def fetch_league_gamelog(self, season: str, season_type: str) -> dict[str, Any]:
        params = {
            "Season": season,
            "SeasonType": season_type,
            "PlayerOrTeam": "Team",
            "LeagueID": "00",
            "SortOrder": "ASC",
            "Sorter": "DATE",
        }
        return self._fetch(LEAGUE_GAMELOG_URL, params)

    def fetch_boxscore_traditional(self, game_id: str) -> dict[str, Any]:
        params = {
            "GameID": game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 0,
            "StartRange": 0,
            "EndRange": 0,
        }
        return self._fetch(BOXSCORE_TRAD_URL, params)

    def fetch_boxscore_advanced(self, game_id: str) -> dict[str, Any]:
        params = {
            "GameID": game_id,
            "StartPeriod": 0,
            "EndPeriod": 10,
            "RangeType": 0,
            "StartRange": 0,
            "EndRange": 0,
        }
        return self._fetch(BOXSCORE_ADV_URL, params)

    def fetch_play_by_play(self, game_id: str) -> dict[str, Any]:
        params = {"GameID": game_id, "StartPeriod": 0, "EndPeriod": 10}
        return self._fetch(PBP_URL, params)
