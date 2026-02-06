"""SportsGameOdds v2 client."""
from __future__ import annotations

from typing import Any

from .config import Settings
from .http_client import get_json


class SGOClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        if not settings.api_key:
            raise RuntimeError("Missing SPORTS_GAME_ODDS_API_KEY")

    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self.settings.api_key}

    def events(self, limit: int, bookmaker_ids: list[str] | None, event_id: str | None, include_alt_lines: bool) -> Any:
        params = {
            "leagueID": self.settings.league_id,
            "oddsAvailable": "true",
            "limit": limit,
        }
        if bookmaker_ids:
            params["bookmakerID"] = ",".join(bookmaker_ids)
        if event_id:
            params["eventID"] = event_id
        if include_alt_lines:
            params["includeAltLines"] = "true"

        url = f"{self.settings.base_url}/events/"
        return get_json(
            url,
            headers=self._headers(),
            params=params,
            timeout_s=self.settings.request_timeout_s,
            max_retries=self.settings.max_retries,
            backoff_base_s=self.settings.backoff_base_s,
        )

    def markets(self) -> Any:
        url = f"{self.settings.base_url}/markets/"
        params = {"leagueID": self.settings.league_id, "limit": 10000}
        return get_json(
            url,
            headers=self._headers(),
            params=params,
            timeout_s=self.settings.request_timeout_s,
            max_retries=self.settings.max_retries,
            backoff_base_s=self.settings.backoff_base_s,
        )
