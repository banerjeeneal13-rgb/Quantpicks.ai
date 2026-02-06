import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_PATH = Path(DATA_DIR / "nba_player_logs_points_all.csv")

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
TIMEOUT_SECONDS = 25


def _season_label(dt: datetime) -> str:
    year = dt.year
    if dt.month >= 8:
        return f"{year}-{str(year + 1)[-2:]}"
    return f"{year - 1}-{str(year)[-2:]}"


def _date_range(start: datetime, end: datetime) -> list[str]:
    days = []
    cur = start.date()
    while cur <= end.date():
        days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


def _get_json(path: str) -> dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.get(url, timeout=TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response for {path}")
    return data


def _parse_fg(value: str) -> tuple[int, int]:
    if not value or "-" not in value:
        return 0, 0
    made, att = value.split("-", 1)
    try:
        return int(made), int(att)
    except ValueError:
        return 0, 0


def _to_int(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_matchup(team_abbr: str, home_abbr: str, away_abbr: str) -> str:
    if team_abbr == home_abbr:
        return f"{home_abbr} vs {away_abbr}"
    if team_abbr == away_abbr:
        return f"{away_abbr} @ {home_abbr}"
    return f"{home_abbr} vs {away_abbr}"


def _iter_events(start: datetime, end: datetime) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for d in _date_range(start, end):
        data = _get_json(f"scoreboard?dates={d}")
        events.extend(data.get("events", []) or [])
    return events


def _parse_event_meta(event: dict[str, Any]) -> tuple[str, str, str, datetime | None]:
    comps = event.get("competitions", []) or []
    comp = comps[0] if comps else {}
    competitors = comp.get("competitors", []) or []
    home_abbr = ""
    away_abbr = ""
    for c in competitors:
        team = c.get("team") or {}
        abbr = str(team.get("abbreviation") or "").strip().upper()
        if c.get("homeAway") == "home":
            home_abbr = abbr
        elif c.get("homeAway") == "away":
            away_abbr = abbr
    event_id = str(event.get("id") or "")
    dt = None
    try:
        dt = datetime.fromisoformat(str(event.get("date") or "").replace("Z", "+00:00"))
    except Exception:
        dt = None
    return event_id, home_abbr, away_abbr, dt


def _rows_from_summary(summary: dict[str, Any], home_abbr: str, away_abbr: str, game_date: str, season: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    players = summary.get("boxscore", {}).get("players", []) or []
    for team_entry in players:
        team = team_entry.get("team") or {}
        team_abbr = str(team.get("abbreviation") or "").strip().upper()
        if not team_abbr:
            continue
        matchup = _build_matchup(team_abbr, home_abbr, away_abbr)
        stats_groups = team_entry.get("statistics", []) or []
        if not stats_groups:
            continue
        stat_group = stats_groups[0]
        names = stat_group.get("names", []) or []
        athletes = stat_group.get("athletes", []) or []
        name_idx = {n: i for i, n in enumerate(names)}
        for athlete in athletes:
            if athlete.get("didNotPlay") or athlete.get("ejected"):
                continue
            stats = athlete.get("stats", []) or []
            if not stats:
                continue
            min_val = stats[name_idx.get("MIN", -1)] if "MIN" in name_idx else ""
            minutes = _to_float(min_val)
            if minutes < 5:
                continue
            fg = stats[name_idx.get("FG", -1)] if "FG" in name_idx else ""
            fg3 = stats[name_idx.get("3PT", -1)] if "3PT" in name_idx else ""
            ft = stats[name_idx.get("FT", -1)] if "FT" in name_idx else ""
            fgm, fga = _parse_fg(fg)
            fg3m, fg3a = _parse_fg(fg3)
            ftm, fta = _parse_fg(ft)

            row = {
                "player_name": str((athlete.get("athlete") or {}).get("displayName") or "").strip(),
                "season": season,
                "GAME_DATE": game_date,
                "MATCHUP": matchup,
                "MIN": minutes,
                "PTS": _to_int(stats[name_idx.get("PTS", -1)] if "PTS" in name_idx else 0),
                "REB": _to_int(stats[name_idx.get("REB", -1)] if "REB" in name_idx else 0),
                "AST": _to_int(stats[name_idx.get("AST", -1)] if "AST" in name_idx else 0),
                "STL": _to_int(stats[name_idx.get("STL", -1)] if "STL" in name_idx else 0),
                "BLK": _to_int(stats[name_idx.get("BLK", -1)] if "BLK" in name_idx else 0),
                "TOV": _to_int(stats[name_idx.get("TO", -1)] if "TO" in name_idx else 0),
                "FGM": fgm,
                "FGA": fga,
                "FG3M": fg3m,
                "FG3A": fg3a,
                "FTM": ftm,
                "FTA": fta,
            }
            if row["player_name"]:
                rows.append(row)
    return rows


def main() -> None:
    start = datetime.now(timezone.utc) - timedelta(days=int(os.getenv("ESPN_DAYS_BACK", "10")))
    end = datetime.now(timezone.utc)
    start_env = os.getenv("ESPN_START_DATE", "").strip()
    end_env = os.getenv("ESPN_END_DATE", "").strip()
    if start_env:
        start = datetime.fromisoformat(start_env).replace(tzinfo=timezone.utc)
    if end_env:
        end = datetime.fromisoformat(end_env).replace(tzinfo=timezone.utc)

    events = _iter_events(start, end)
    if not events:
        print("No ESPN events found.")
        return

    all_rows: list[dict[str, Any]] = []
    for event in events:
        event_id, home_abbr, away_abbr, dt = _parse_event_meta(event)
        if not event_id or not dt:
            continue
        summary = _get_json(f"summary?event={event_id}")
        game_date = dt.date().isoformat()
        season = _season_label(dt)
        all_rows.extend(_rows_from_summary(summary, home_abbr, away_abbr, game_date, season))

    if not all_rows:
        print("No player rows parsed from ESPN summaries.")
        return

    if OUT_PATH.exists():
        with open(OUT_PATH, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
    else:
        existing = []

    merged = existing + all_rows
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in merged:
        key = (row.get("player_name", ""), row.get("GAME_DATE", ""), row.get("MATCHUP", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    fieldnames = [
        "player_name", "season", "GAME_DATE", "MATCHUP",
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
    ]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in deduped:
            writer.writerow(row)

    print(f"Saved {len(deduped)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
