from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Protocol


@dataclass(frozen=True)
class Event:
    event_id: str
    sport: str
    league: str
    start_time_utc: str
    home_team: str
    away_team: str


@dataclass(frozen=True)
class PropOutcome:
    event: Event
    sportsbook: str
    market_key: str
    player_name: str
    outcome: str  # over/under
    line: float
    price_decimal: float
    price_american: int | None
    extra: Dict[str, Any]


class OddsProvider(Protocol):
    name: str

    def fetch_events(self, sport: str) -> Iterable[Event]:
        ...

    def fetch_props(
        self,
        event: Event,
        markets: list[str],
        regions: list[str],
        bookmakers: list[str] | None,
    ) -> Iterable[PropOutcome]:
        ...


class StatsProvider(Protocol):
    name: str

    def fetch_stats(self, sport: str, out_csv: str) -> str:
        ...
