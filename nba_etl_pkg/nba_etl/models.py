from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class GameRow:
    game_id: str
    season: str
    season_type: str
    date_utc: str
    home_team_id: str
    away_team_id: str
    final_score_home: int
    final_score_away: int
    status: str
    source: str


@dataclass(frozen=True)
class PlayerBoxRow:
    game_id: str
    season: str
    season_type: str
    player_id: str
    player_name: str
    team_id: str
    team_tricode: Optional[str]
    minutes: float
    pts: int
    reb: int
    ast: int
    stl: int
    blk: int
    tov: int
    fga: int
    fgm: int
    fg3a: int
    fg3m: int
    fta: int
    ftm: int
    plus_minus: int
    starter: bool
    source: str


@dataclass(frozen=True)
class TeamBoxRow:
    game_id: str
    season: str
    season_type: str
    team_id: str
    team_tricode: Optional[str]
    minutes: float
    pts: int
    reb: int
    ast: int
    stl: int
    blk: int
    tov: int
    fga: int
    fgm: int
    fg3a: int
    fg3m: int
    fta: int
    ftm: int
    plus_minus: int
    source: str


@dataclass(frozen=True)
class PlayerAdvancedRow:
    game_id: str
    season: str
    season_type: str
    player_id: str
    team_id: str
    ts_pct: Optional[float]
    efg_pct: Optional[float]
    usg_pct: Optional[float]
    ast_pct: Optional[float]
    tov_pct: Optional[float]
    reb_pct: Optional[float]
    possessions: Optional[float]
    pace: Optional[float]
    metric_source: str  # "sourced" or "computed"
