from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TeamScore:
    tricode: str
    name: Optional[str]
    score: int


@dataclass(frozen=True)
class GameResult:
    game_id: str
    game_date_iso: str  # ISO 8601 in requested timezone
    status: str  # FINAL
    home_team: TeamScore
    away_team: TeamScore
    source: str
