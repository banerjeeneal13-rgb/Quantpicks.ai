from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from .models import GameResult, TeamScore


def _parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _team_from_primary(team: dict[str, Any]) -> TeamScore:
    return TeamScore(
        tricode=str(team.get("teamTricode") or team.get("triCode") or "").strip(),
        name=str(team.get("teamName") or team.get("name") or "").strip() or None,
        score=_to_int(team.get("score")),
    )


def normalize_primary(raw: dict[str, Any], timezone: str) -> list[GameResult]:
    scoreboard = raw.get("scoreboard")
    if not isinstance(scoreboard, dict):
        raise ValueError("Primary response missing scoreboard object")
    games = scoreboard.get("games")
    if not isinstance(games, list):
        raise ValueError("Primary response missing games list")

    tz = ZoneInfo(timezone)
    out: list[GameResult] = []
    for g in games:
        if not isinstance(g, dict):
            continue
        status_id = g.get("gameStatus")
        status_text = str(g.get("gameStatusText") or "").upper()
        if status_id != 3 and "FINAL" not in status_text:
            continue
        game_id = str(g.get("gameId") or "")
        game_time = _parse_datetime(str(g.get("gameTimeUTC") or g.get("gameDateTimeUTC") or ""))
        if not game_time:
            # fallback to date only
            date_str = str(g.get("gameDate") or "")
            if len(date_str) == 8:
                try:
                    game_time = datetime.strptime(date_str, "%Y%m%d")
                except Exception:
                    game_time = None
        if not game_time:
            continue
        local_time = game_time.astimezone(tz)
        out.append(
            GameResult(
                game_id=game_id,
                game_date_iso=local_time.isoformat(),
                status="FINAL",
                home_team=_team_from_primary(g.get("homeTeam") or {}),
                away_team=_team_from_primary(g.get("awayTeam") or {}),
                source="cdn",
            )
        )
    return out


def normalize_fallback(raw: dict[str, Any], timezone: str) -> list[GameResult]:
    result_sets = raw.get("resultSets")
    if not isinstance(result_sets, list):
        raise ValueError("Fallback response missing resultSets")

    header_rows: list[dict[str, Any]] = []
    line_rows: list[dict[str, Any]] = []
    for rs in result_sets:
        if not isinstance(rs, dict):
            continue
        if rs.get("name") == "GameHeader":
            headers = rs.get("headers", [])
            for row in rs.get("rowSet", []):
                header_rows.append(dict(zip(headers, row)))
        if rs.get("name") == "LineScore":
            headers = rs.get("headers", [])
            for row in rs.get("rowSet", []):
                line_rows.append(dict(zip(headers, row)))

    if not header_rows or not line_rows:
        raise ValueError("Fallback response missing GameHeader/LineScore data")

    line_by_game: dict[str, list[dict[str, Any]]] = {}
    for row in line_rows:
        game_id = str(row.get("GAME_ID") or "")
        line_by_game.setdefault(game_id, []).append(row)

    tz = ZoneInfo(timezone)
    out: list[GameResult] = []
    for gh in header_rows:
        status_id = gh.get("GAME_STATUS_ID")
        status_text = str(gh.get("GAME_STATUS_TEXT") or "").upper()
        if status_id != 3 and "FINAL" not in status_text:
            continue
        game_id = str(gh.get("GAME_ID") or "")
        date_str = str(gh.get("GAME_DATE_EST") or "")
        game_time = _parse_datetime(date_str)
        if not game_time:
            try:
                game_time = datetime.strptime(date_str, "%Y-%m-%d")
            except Exception:
                game_time = None
        if not game_time:
            continue

        lines = line_by_game.get(game_id, [])
        home_id = str(gh.get("HOME_TEAM_ID") or "")
        away_id = str(gh.get("VISITOR_TEAM_ID") or "")
        home_line = next((l for l in lines if str(l.get("TEAM_ID")) == home_id), None)
        away_line = next((l for l in lines if str(l.get("TEAM_ID")) == away_id), None)
        if not home_line or not away_line:
            continue

        local_time = game_time.astimezone(tz)
        out.append(
            GameResult(
                game_id=game_id,
                game_date_iso=local_time.isoformat(),
                status="FINAL",
                home_team=TeamScore(
                    tricode=str(home_line.get("TEAM_ABBREVIATION") or ""),
                    name=f"{home_line.get('TEAM_CITY_NAME','')} {home_line.get('TEAM_NAME','')}".strip() or None,
                    score=_to_int(home_line.get("PTS")),
                ),
                away_team=TeamScore(
                    tricode=str(away_line.get("TEAM_ABBREVIATION") or ""),
                    name=f"{away_line.get('TEAM_CITY_NAME','')} {away_line.get('TEAM_NAME','')}".strip() or None,
                    score=_to_int(away_line.get("PTS")),
                ),
                source="stats",
            )
        )
    return out


def most_recent_game(games: Iterable[GameResult]) -> Optional[GameResult]:
    games_list = list(games)
    if not games_list:
        return None
    return max(games_list, key=lambda g: g.game_date_iso)
