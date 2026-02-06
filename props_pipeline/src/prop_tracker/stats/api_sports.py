"""API-Sports (nba.api-sports.io) stats provider."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from .base import StatsProvider
from ..http import request_with_backoff
from ..utils import normalize_player_name, parse_iso_datetime

BASE_URL = "https://v2.nba.api-sports.io"


@dataclass(frozen=True)
class EventKey:
    event_id: str
    event_start_utc: str
    home_team: str
    away_team: str


def _api_headers(api_key: str) -> dict[str, str]:
    return {"x-apisports-key": api_key}


def _get_json(
    path: str,
    params: dict[str, Any],
    api_key: str,
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
) -> Any:
    url = f"{BASE_URL}/{path}"

    def do_request() -> httpx.Response:
        return httpx.get(url, params=params, headers=_api_headers(api_key), timeout=timeout_s)

    resp = request_with_backoff(do_request, max_retries=max_retries, backoff_base_s=backoff_base_s)
    resp.raise_for_status()
    return resp.json()


def _parse_props_events(props_csv: str) -> list[EventKey]:
    events: dict[str, EventKey] = {}
    with open(props_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_id = str(row.get("event_id") or "").strip()
            if not event_id or event_id in events:
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


def _normalize_team(name: str) -> str:
    return normalize_player_name(name)


def _season_from_date(dt: datetime) -> int:
    # NBA season starts around Oct; Jan/Feb belong to previous season year.
    return dt.year if dt.month >= 10 else dt.year - 1


def _load_teams(api_key: str, timeout_s: int, max_retries: int, backoff_base_s: float) -> dict[str, int]:
    data = _get_json("teams", {}, api_key, timeout_s, max_retries, backoff_base_s)
    teams: dict[str, int] = {}
    for team in data.get("response", []):
        name = str(team.get("name") or "").strip()
        team_id = team.get("id")
        if name and team_id:
            teams[_normalize_team(name)] = int(team_id)
    return teams


def _load_games_for_team(
    season: int,
    team_id: int,
    api_key: str,
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
) -> list[dict[str, Any]]:
    params = {"season": season, "team": team_id, "league": "standard"}
    data = _get_json("games", params, api_key, timeout_s, max_retries, backoff_base_s)
    return data.get("response", []) if isinstance(data, dict) else []


def _parse_game_start(game: dict[str, Any]) -> datetime | None:
    start = game.get("date", {}).get("start") if isinstance(game.get("date"), dict) else None
    if not start:
        return None
    try:
        return datetime.fromisoformat(str(start).replace("Z", "+00:00"))
    except ValueError:
        return None


def _find_game_for_event(
    event: EventKey,
    teams_map: dict[str, int],
    games_cache: dict[tuple[int, int], list[dict[str, Any]]],
    api_key: str,
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
) -> int | None:
    try:
        start_dt = datetime.fromisoformat(event.event_start_utc.replace("Z", "+00:00"))
    except ValueError:
        return None

    season = _season_from_date(start_dt)
    home_norm = _normalize_team(event.home_team)
    away_norm = _normalize_team(event.away_team)

    for team_norm in (home_norm, away_norm):
        team_id = teams_map.get(team_norm)
        if not team_id:
            continue
        cache_key = (season, team_id)
        if cache_key not in games_cache:
            games_cache[cache_key] = _load_games_for_team(
                season,
                team_id,
                api_key,
                timeout_s,
                max_retries,
                backoff_base_s,
            )
        games = games_cache[cache_key]
        best_id: int | None = None
        best_delta: float | None = None
        for game in games:
            home = _normalize_team(str(game.get("teams", {}).get("home", {}).get("name") or ""))
            away = _normalize_team(str(game.get("teams", {}).get("away", {}).get("name") or ""))
            if {home, away} != {home_norm, away_norm}:
                continue
            game_start = _parse_game_start(game)
            if not game_start:
                continue
            delta = abs((game_start - start_dt).total_seconds())
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_id = int(game.get("id"))
        if best_id:
            return best_id
    return None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _rows_for_player(event_id: str, player: dict[str, Any]) -> list[dict[str, Any]]:
    pts = _to_float(player.get("points"))
    reb = _to_float(player.get("totReb"))
    ast = _to_float(player.get("assists"))
    fg3m = _to_float(player.get("tpm"))
    blk = _to_float(player.get("blocks"))
    stl = _to_float(player.get("steals"))

    stocks = blk + stl
    pra = pts + reb + ast
    pr = pts + reb
    pa = pts + ast
    ra = reb + ast

    firstname = str(player.get("player", {}).get("firstname") or "").strip()
    lastname = str(player.get("player", {}).get("lastname") or "").strip()
    player_name = (firstname + " " + lastname).strip()

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


class APISportsStatsProvider(StatsProvider):
    name = "api_sports"

    def __init__(self, api_key: str, timeout_s: int, max_retries: int, backoff_base_s: float) -> None:
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s

    def fetch_stats(self, props_csv: str, out_csv: str) -> str:
        events = _parse_props_events(props_csv)
        if not events:
            raise RuntimeError("No events found in props CSV.")

        teams_map = _load_teams(self.api_key, self.timeout_s, self.max_retries, self.backoff_base_s)
        games_cache: dict[tuple[int, int], list[dict[str, Any]]] = {}
        stats_cache: dict[int, list[dict[str, Any]]] = {}

        rows: list[dict[str, Any]] = []
        missing: list[str] = []

        for event in events:
            game_id = _find_game_for_event(
                event,
                teams_map,
                games_cache,
                self.api_key,
                self.timeout_s,
                self.max_retries,
                self.backoff_base_s,
            )
            if not game_id:
                missing.append(event.event_id)
                continue
            if game_id not in stats_cache:
                data = _get_json(
                    "players/statistics",
                    {"game": game_id},
                    self.api_key,
                    self.timeout_s,
                    self.max_retries,
                    self.backoff_base_s,
                )
                stats_cache[game_id] = data.get("response", []) if isinstance(data, dict) else []
            for player in stats_cache[game_id]:
                rows.extend(_rows_for_player(event.event_id, player))

        if missing:
            print(f"API-Sports missing {len(missing)} event mappings: {missing[:5]}")

        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["event_id", "player_name", "stat_key", "actual_value"])
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return out_csv
