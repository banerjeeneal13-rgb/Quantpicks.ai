"""NBA Stats API provider (unofficial)."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from .base import StatsProvider
from ..http import request_with_backoff
from ..utils import normalize_player_name, parse_iso_datetime

NBA_SCOREBOARD_URL = "https://stats.nba.com/stats/scoreboardv2"
NBA_BOXSCORE_URL = "https://stats.nba.com/stats/boxscoretraditionalv2"


@dataclass(frozen=True)
class EventKey:
    event_id: str
    event_start_utc: str
    home_team: str
    away_team: str


def _nba_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nba.com",
        "Referer": "https://www.nba.com/",
        "Connection": "keep-alive",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _get_json(url: str, params: dict[str, Any], timeout_s: int, max_retries: int, backoff_base_s: float) -> Any:
    def do_request() -> httpx.Response:
        return httpx.get(url, params=params, timeout=timeout_s, headers=_nba_headers())

    resp = request_with_backoff(do_request, max_retries=max_retries, backoff_base_s=backoff_base_s)
    resp.raise_for_status()
    return resp.json()


def _normalize_team(name: str) -> str:
    return normalize_player_name(name)


def _parse_props_events(props_csv: str) -> list[EventKey]:
    events: dict[str, EventKey] = {}
    with open(props_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_id = str(row.get("event_id") or "").strip()
            if not event_id:
                continue
            if event_id in events:
                continue
            event_start = parse_iso_datetime(str(row.get("event_start_utc") or "").strip())
            if not event_start:
                continue
            events[event_id] = EventKey(
                event_id=event_id,
                event_start_utc=event_start,
                home_team=str(row.get("home_team") or "").strip(),
                away_team=str(row.get("away_team") or "").strip(),
            )
    return list(events.values())


def _scoreboard_games(game_date: str, timeout_s: int, max_retries: int, backoff_base_s: float) -> list[dict[str, Any]]:
    params = {
        "GameDate": game_date,
        "LeagueID": "00",
        "DayOffset": 0,
    }
    data = _get_json(NBA_SCOREBOARD_URL, params, timeout_s, max_retries, backoff_base_s)
    results = data.get("resultSets", []) if isinstance(data, dict) else []
    for rs in results:
        if rs.get("name") == "GameHeader":
            headers = rs.get("headers", [])
            rows = rs.get("rowSet", [])
            return [dict(zip(headers, row)) for row in rows]
    return []


def _find_game_id(
    event: EventKey,
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
    scoreboard_cache: dict[str, list[dict[str, Any]]],
) -> str | None:
    if not event.event_start_utc:
        return None
    try:
        start_dt = datetime.fromisoformat(event.event_start_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    candidates = [start_dt.date(), (start_dt + timedelta(days=1)).date(), (start_dt - timedelta(days=1)).date()]

    home_norm = _normalize_team(event.home_team)
    away_norm = _normalize_team(event.away_team)

    for date in candidates:
        game_date = date.strftime("%m/%d/%Y")
        if game_date not in scoreboard_cache:
            scoreboard_cache[game_date] = _scoreboard_games(game_date, timeout_s, max_retries, backoff_base_s)
        games = scoreboard_cache[game_date]
        for game in games:
            home = _normalize_team(str(game.get("HOME_TEAM_NAME") or ""))
            away = _normalize_team(str(game.get("AWAY_TEAM_NAME") or ""))
            if home == home_norm and away == away_norm:
                return str(game.get("GAME_ID"))
    return None


def _boxscore_players(game_id: str, timeout_s: int, max_retries: int, backoff_base_s: float) -> list[dict[str, Any]]:
    params = {
        "GameID": game_id,
        "StartPeriod": 0,
        "EndPeriod": 10,
        "RangeType": 0,
        "StartRange": 0,
        "EndRange": 0,
    }
    data = _get_json(NBA_BOXSCORE_URL, params, timeout_s, max_retries, backoff_base_s)
    results = data.get("resultSets", []) if isinstance(data, dict) else []
    for rs in results:
        if rs.get("name") == "PlayerStats":
            headers = rs.get("headers", [])
            rows = rs.get("rowSet", [])
            return [dict(zip(headers, row)) for row in rows]
    return []


def _rows_for_player(event_id: str, player: dict[str, Any]) -> list[dict[str, Any]]:
    pts = float(player.get("PTS") or 0)
    reb = float(player.get("REB") or 0)
    ast = float(player.get("AST") or 0)
    fg3m = float(player.get("FG3M") or 0)
    blk = float(player.get("BLK") or 0)
    stl = float(player.get("STL") or 0)

    stocks = blk + stl
    pra = pts + reb + ast
    pr = pts + reb
    pa = pts + ast
    ra = reb + ast

    player_name = str(player.get("PLAYER_NAME") or "").strip()

    return [
        {"event_id": event_id, "player_name": player_name, "stat_key": "points", "actual_value": pts},
        {"event_id": event_id, "player_name": player_name, "stat_key": "rebounds", "actual_value": reb},
        {"event_id": event_id, "player_name": player_name, "stat_key": "assists", "actual_value": ast},
        {"event_id": event_id, "player_name": player_name, "stat_key": "threes", "actual_value": fg3m},
        {"event_id": event_id, "player_name": player_name, "stat_key": "stocks", "actual_value": stocks},
        {"event_id": event_id, "player_name": player_name, "stat_key": "pra", "actual_value": pra},
        {"event_id": event_id, "player_name": player_name, "stat_key": "pr", "actual_value": pr},
        {"event_id": event_id, "player_name": player_name, "stat_key": "pa", "actual_value": pa},
        {"event_id": event_id, "player_name": player_name, "stat_key": "ra", "actual_value": ra},
    ]


class NBAAPIStatsProvider(StatsProvider):
    name = "nba_api"

    def __init__(self, timeout_s: int, max_retries: int, backoff_base_s: float) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s

    def fetch_stats(self, props_csv: str, out_csv: str) -> str:
        events = _parse_props_events(props_csv)
        if not events:
            raise RuntimeError("No events found in props CSV.")

        rows: list[dict[str, Any]] = []
        missing: list[str] = []
        scoreboard_cache: dict[str, list[dict[str, Any]]] = {}

        for event in events:
            game_id = _find_game_id(
                event,
                self.timeout_s,
                self.max_retries,
                self.backoff_base_s,
                scoreboard_cache,
            )
            if not game_id:
                missing.append(event.event_id)
                continue
            players = _boxscore_players(game_id, self.timeout_s, self.max_retries, self.backoff_base_s)
            for player in players:
                rows.extend(_rows_for_player(event.event_id, player))

        if missing:
            print(f"NBA API missing {len(missing)} event mappings: {missing[:5]}")

        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["event_id", "player_name", "stat_key", "actual_value"])
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return out_csv
