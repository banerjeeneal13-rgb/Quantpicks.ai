"""The Odds API provider implementation."""
from __future__ import annotations

import logging

import httpx

from typing import Iterable, List

from .base import Event, PropOutcome
from ..config import Settings
from ..http import get_json_with_backoff
from ..utils import american_from_decimal, coerce_float, parse_iso_datetime


class TheOddsAPIProvider:
    name = "theoddsapi"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        if not settings.odds_api_key:
            raise RuntimeError("Missing ODDS_API_KEY for The Odds API")

    def fetch_events(self, sport: str) -> Iterable[Event]:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
        params = {"apiKey": self.settings.odds_api_key}
        data = get_json_with_backoff(
            url,
            params=params,
            timeout_s=self.settings.request_timeout_s,
            max_retries=self.settings.max_retries,
            backoff_base_s=self.settings.backoff_base_s,
        )

        events: List[Event] = []
        for item in data or []:
            start_time = parse_iso_datetime(item.get("commence_time"))
            if not start_time:
                continue
            events.append(
                Event(
                    event_id=str(item.get("id")),
                    sport=sport,
                    league=str(item.get("sport_title") or "NBA").upper(),
                    event_start_utc=start_time,
                    home_team=str(item.get("home_team") or ""),
                    away_team=str(item.get("away_team") or ""),
                )
            )
        return events

    def fetch_props(
        self,
        event: Event,
        markets: list[str],
        regions: list[str],
        bookmakers: list[str] | None,
    ) -> Iterable[PropOutcome]:
        url = f"https://api.the-odds-api.com/v4/sports/{event.sport}/events/{event.event_id}/odds"
        params = {
            "apiKey": self.settings.odds_api_key,
            "regions": ",".join(regions),
            "markets": ",".join(markets),
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        if bookmakers:
            params["bookmakers"] = ",".join(bookmakers)

        try:
            data = get_json_with_backoff(
                url,
                params=params,
                timeout_s=self.settings.request_timeout_s,
                max_retries=self.settings.max_retries,
                backoff_base_s=self.settings.backoff_base_s,
            )
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response else "unknown"
            logging.warning("Odds API request failed for event %s (HTTP %s)", event.event_id, status)
            return []

        outcomes: list[PropOutcome] = []
        for book in (data.get("bookmakers") or []) if isinstance(data, dict) else []:
            book_key = str(book.get("key") or "").strip().lower()
            if not book_key:
                continue
            for market in book.get("markets", []) or []:
                market_key = str(market.get("key") or "")
                if not market_key:
                    continue
                for outcome in market.get("outcomes", []) or []:
                    side = str(outcome.get("name") or "").strip().lower()
                    player = str(outcome.get("description") or "").strip()
                    line = coerce_float(outcome.get("point"))
                    price_decimal = coerce_float(outcome.get("price"))
                    if not player or side not in {"over", "under"}:
                        continue
                    if line is None or price_decimal is None:
                        continue

                    outcomes.append(
                        PropOutcome(
                            event=event,
                            sportsbook=book_key,
                            market_key=market_key,
                            player_name=player,
                            outcome=side,
                            line=float(line),
                            price_decimal=float(price_decimal),
                            price_american=american_from_decimal(float(price_decimal)),
                            raw={
                                "book_title": book.get("title"),
                                "market_last_update": market.get("last_update"),
                            },
                        )
                    )

        return outcomes


def get_provider(settings: Settings):
    if settings.odds_provider == "theoddsapi":
        return TheOddsAPIProvider(settings)
    raise RuntimeError(f"Unsupported ODDS_PROVIDER: {settings.odds_provider}")
