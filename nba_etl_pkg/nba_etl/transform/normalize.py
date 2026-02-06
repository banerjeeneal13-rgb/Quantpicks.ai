from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from ..models import GameRow, PlayerBoxRow, TeamBoxRow, PlayerAdvancedRow


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _parse_minutes(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value)
    if s.startswith("PT") and "M" in s:
        try:
            minutes = s.split("PT", 1)[1].split("M", 1)[0]
            return float(minutes)
        except Exception:
            return 0.0
    if ":" in s:
        try:
            mins, secs = s.split(":")
            return float(mins) + float(secs) / 60.0
        except Exception:
            return 0.0
    return _safe_float(value)


def normalize_league_gamelog(data: dict[str, Any], season: str, season_type: str) -> list[dict[str, Any]]:
    result_sets = data.get("resultSets", [])
    if not result_sets:
        raise ValueError("leaguegamelog response missing resultSets")
    rows = []
    for rs in result_sets:
        if rs.get("name") != "LeagueGameLog":
            continue
        headers = rs.get("headers", [])
        for row in rs.get("rowSet", []):
            rec = dict(zip(headers, row))
            rec["season"] = season
            rec["season_type"] = season_type
            rows.append(rec)
    if not rows:
        raise ValueError("leaguegamelog missing rows")
    return rows


def normalize_cdn_boxscore(
    data: dict[str, Any],
    season: str,
    season_type: str,
) -> tuple[GameRow, list[PlayerBoxRow], list[TeamBoxRow]]:
    game = data.get("game")
    if not isinstance(game, dict):
        raise ValueError("cdn boxscore missing game")

    game_id = str(game.get("gameId") or "")
    status = str(game.get("gameStatusText") or "").upper()
    game_time = str(game.get("gameTimeUTC") or "")
    home = game.get("homeTeam", {})
    away = game.get("awayTeam", {})

    game_row = GameRow(
        game_id=game_id,
        season=season,
        season_type=season_type,
        date_utc=game_time,
        home_team_id=str(home.get("teamId") or ""),
        away_team_id=str(away.get("teamId") or ""),
        final_score_home=_safe_int(home.get("score")),
        final_score_away=_safe_int(away.get("score")),
        status="FINAL" if "FINAL" in status or str(game.get("gameStatus")) == "3" else status,
        source="cdn",
    )

    player_rows: list[PlayerBoxRow] = []
    team_rows: list[TeamBoxRow] = []

    for team in (home, away):
        team_id = str(team.get("teamId") or "")
        team_tri = team.get("teamTricode")
        stats = team.get("statistics", {}) or {}
        team_rows.append(
            TeamBoxRow(
                game_id=game_id,
                season=season,
                season_type=season_type,
                team_id=team_id,
                team_tricode=str(team_tri) if team_tri else None,
                minutes=_parse_minutes(stats.get("minutes")),
                pts=_safe_int(stats.get("points")),
                reb=_safe_int(stats.get("reboundsTotal")),
                ast=_safe_int(stats.get("assists")),
                stl=_safe_int(stats.get("steals")),
                blk=_safe_int(stats.get("blocks")),
                tov=_safe_int(stats.get("turnovers")),
                fga=_safe_int(stats.get("fieldGoalsAttempted")),
                fgm=_safe_int(stats.get("fieldGoalsMade")),
                fg3a=_safe_int(stats.get("threePointersAttempted")),
                fg3m=_safe_int(stats.get("threePointersMade")),
                fta=_safe_int(stats.get("freeThrowsAttempted")),
                ftm=_safe_int(stats.get("freeThrowsMade")),
                plus_minus=_safe_int(stats.get("plusMinusPoints")),
                source="cdn",
            )
        )

        for player in team.get("players", []) or []:
            stats_p = player.get("statistics", {}) or {}
            player_rows.append(
                PlayerBoxRow(
                    game_id=game_id,
                    season=season,
                    season_type=season_type,
                    player_id=str(player.get("personId") or ""),
                    player_name=str(player.get("name") or ""),
                    team_id=team_id,
                    team_tricode=str(team_tri) if team_tri else None,
                    minutes=_parse_minutes(stats_p.get("minutes")),
                    pts=_safe_int(stats_p.get("points")),
                    reb=_safe_int(stats_p.get("reboundsTotal")),
                    ast=_safe_int(stats_p.get("assists")),
                    stl=_safe_int(stats_p.get("steals")),
                    blk=_safe_int(stats_p.get("blocks")),
                    tov=_safe_int(stats_p.get("turnovers")),
                    fga=_safe_int(stats_p.get("fieldGoalsAttempted")),
                    fgm=_safe_int(stats_p.get("fieldGoalsMade")),
                    fg3a=_safe_int(stats_p.get("threePointersAttempted")),
                    fg3m=_safe_int(stats_p.get("threePointersMade")),
                    fta=_safe_int(stats_p.get("freeThrowsAttempted")),
                    ftm=_safe_int(stats_p.get("freeThrowsMade")),
                    plus_minus=_safe_int(stats_p.get("plusMinusPoints")),
                    starter=str(player.get("starter") or "0") in {"1", "True", "true"},
                    source="cdn",
                )
            )

    return game_row, player_rows, team_rows


def normalize_stats_boxscore(
    data: dict[str, Any],
    season: str,
    season_type: str,
) -> tuple[GameRow, list[PlayerBoxRow], list[TeamBoxRow]]:
    result_sets = data.get("resultSets", [])
    if not result_sets:
        raise ValueError("stats boxscore missing resultSets")

    game_summary = None
    player_rows_raw: list[dict[str, Any]] = []
    team_rows_raw: list[dict[str, Any]] = []

    for rs in result_sets:
        if rs.get("name") == "GameSummary":
            headers = rs.get("headers", [])
            rowset = rs.get("rowSet", [])
            if rowset:
                game_summary = dict(zip(headers, rowset[0]))
        if rs.get("name") == "PlayerStats":
            headers = rs.get("headers", [])
            for row in rs.get("rowSet", []):
                player_rows_raw.append(dict(zip(headers, row)))
        if rs.get("name") == "TeamStats":
            headers = rs.get("headers", [])
            for row in rs.get("rowSet", []):
                team_rows_raw.append(dict(zip(headers, row)))

    if not game_summary:
        raise ValueError("stats boxscore missing GameSummary")

    game_id = str(game_summary.get("GAME_ID") or "")
    date_utc = str(game_summary.get("GAME_DATE_EST") or "")
    home_id = str(game_summary.get("HOME_TEAM_ID") or "")
    away_id = str(game_summary.get("VISITOR_TEAM_ID") or "")
    home_score = _safe_int(game_summary.get("HOME_TEAM_SCORE"))
    away_score = _safe_int(game_summary.get("VISITOR_TEAM_SCORE"))

    game_row = GameRow(
        game_id=game_id,
        season=season,
        season_type=season_type,
        date_utc=date_utc,
        home_team_id=home_id,
        away_team_id=away_id,
        final_score_home=home_score,
        final_score_away=away_score,
        status="FINAL",
        source="stats",
    )

    team_rows: list[TeamBoxRow] = []
    for row in team_rows_raw:
        team_rows.append(
            TeamBoxRow(
                game_id=game_id,
                season=season,
                season_type=season_type,
                team_id=str(row.get("TEAM_ID") or ""),
                team_tricode=str(row.get("TEAM_ABBREVIATION") or ""),
                minutes=_parse_minutes(row.get("MIN")),
                pts=_safe_int(row.get("PTS")),
                reb=_safe_int(row.get("REB")),
                ast=_safe_int(row.get("AST")),
                stl=_safe_int(row.get("STL")),
                blk=_safe_int(row.get("BLK")),
                tov=_safe_int(row.get("TO")),
                fga=_safe_int(row.get("FGA")),
                fgm=_safe_int(row.get("FGM")),
                fg3a=_safe_int(row.get("FG3A")),
                fg3m=_safe_int(row.get("FG3M")),
                fta=_safe_int(row.get("FTA")),
                ftm=_safe_int(row.get("FTM")),
                plus_minus=_safe_int(row.get("PLUS_MINUS")),
                source="stats",
            )
        )

    player_rows: list[PlayerBoxRow] = []
    for row in player_rows_raw:
        player_rows.append(
            PlayerBoxRow(
                game_id=game_id,
                season=season,
                season_type=season_type,
                player_id=str(row.get("PLAYER_ID") or ""),
                player_name=str(row.get("PLAYER_NAME") or ""),
                team_id=str(row.get("TEAM_ID") or ""),
                team_tricode=str(row.get("TEAM_ABBREVIATION") or ""),
                minutes=_parse_minutes(row.get("MIN")),
                pts=_safe_int(row.get("PTS")),
                reb=_safe_int(row.get("REB")),
                ast=_safe_int(row.get("AST")),
                stl=_safe_int(row.get("STL")),
                blk=_safe_int(row.get("BLK")),
                tov=_safe_int(row.get("TO")),
                fga=_safe_int(row.get("FGA")),
                fgm=_safe_int(row.get("FGM")),
                fg3a=_safe_int(row.get("FG3A")),
                fg3m=_safe_int(row.get("FG3M")),
                fta=_safe_int(row.get("FTA")),
                ftm=_safe_int(row.get("FTM")),
                plus_minus=_safe_int(row.get("PLUS_MINUS")),
                starter=str(row.get("START_POSITION") or "").strip() != "",
                source="stats",
            )
        )

    return game_row, player_rows, team_rows


def normalize_stats_advanced(
    data: dict[str, Any],
    season: str,
    season_type: str,
) -> list[PlayerAdvancedRow]:
    result_sets = data.get("resultSets", [])
    if not result_sets:
        raise ValueError("advanced boxscore missing resultSets")

    player_rows: list[dict[str, Any]] = []
    for rs in result_sets:
        if rs.get("name") == "PlayerStats":
            headers = rs.get("headers", [])
            for row in rs.get("rowSet", []):
                player_rows.append(dict(zip(headers, row)))

    out: list[PlayerAdvancedRow] = []
    for row in player_rows:
        out.append(
            PlayerAdvancedRow(
                game_id=str(row.get("GAME_ID") or ""),
                season=season,
                season_type=season_type,
                player_id=str(row.get("PLAYER_ID") or ""),
                team_id=str(row.get("TEAM_ID") or ""),
                ts_pct=_safe_float(row.get("TS_PCT")),
                efg_pct=_safe_float(row.get("EFG_PCT")),
                usg_pct=_safe_float(row.get("USG_PCT")),
                ast_pct=_safe_float(row.get("AST_PCT")),
                tov_pct=_safe_float(row.get("TOV_PCT")),
                reb_pct=_safe_float(row.get("REB_PCT")),
                possessions=_safe_float(row.get("POSS")),
                pace=_safe_float(row.get("PACE")),
                metric_source="sourced",
            )
        )
    return out


def compute_advanced(
    player_rows: Iterable[PlayerBoxRow],
    team_rows: Iterable[TeamBoxRow],
    season: str,
    season_type: str,
) -> list[PlayerAdvancedRow]:
    team_by_id = {t.team_id: t for t in team_rows}
    opp_by_team = {}
    team_ids = list(team_by_id.keys())
    if len(team_ids) == 2:
        opp_by_team[team_ids[0]] = team_by_id[team_ids[1]]
        opp_by_team[team_ids[1]] = team_by_id[team_ids[0]]

    out: list[PlayerAdvancedRow] = []
    for p in player_rows:
        team = team_by_id.get(p.team_id)
        opp = opp_by_team.get(p.team_id)
        if not team or not opp or p.minutes <= 0:
            continue

        fga = p.fga
        fta = p.fta
        tov = p.tov
        pts = p.pts
        fgm = p.fgm
        fg3m = p.fg3m

        ts_denom = 2 * (fga + 0.44 * fta) if (fga + fta) > 0 else None
        ts_pct = pts / ts_denom if ts_denom else None
        efg_pct = (fgm + 0.5 * fg3m) / fga if fga > 0 else None
        tov_pct = tov / (fga + 0.44 * fta + tov) if (fga + fta + tov) > 0 else None

        team_min = team.minutes if team.minutes > 0 else 240.0
        team_poss = (
            (team.fga + 0.44 * team.fta - 0 + team.tov)
            + (opp.fga + 0.44 * opp.fta - 0 + opp.tov)
        ) / 2.0
        poss = team_poss if team_poss > 0 else None
        pace = team_poss * 48 / (team_min / 5) if team_min > 0 else None

        usg_denom = (team.fga + 0.44 * team.fta + team.tov)
        usg_pct = None
        if usg_denom > 0 and team_min > 0:
            usg_pct = 100 * (fga + 0.44 * fta + tov) * (team_min / 5) / (p.minutes * usg_denom)

        out.append(
            PlayerAdvancedRow(
                game_id=p.game_id,
                season=season,
                season_type=season_type,
                player_id=p.player_id,
                team_id=p.team_id,
                ts_pct=ts_pct,
                efg_pct=efg_pct,
                usg_pct=usg_pct,
                ast_pct=None,
                tov_pct=tov_pct,
                reb_pct=None,
                possessions=poss,
                pace=pace,
                metric_source="computed",
            )
        )
    return out
