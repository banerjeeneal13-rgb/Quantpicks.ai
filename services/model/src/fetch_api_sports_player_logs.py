import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_PATH = Path(os.getenv("API_SPORTS_LOGS_CSV") or (DATA_DIR / "nba_player_logs_points_all.csv"))

API_KEY = os.getenv("API_SPORTS_KEY") or os.getenv("NBA_API_SPORTS_KEY")
BASE_URL = "https://v2.nba.api-sports.io"

# Seasons allowed by your plan appear to be 2022-2024. Default to latest allowed.
_DEFAULT_SEASONS = ["2024"]
SEASONS = [s.strip() for s in os.getenv("API_SPORTS_SEASONS", "").split(",") if s.strip()] or _DEFAULT_SEASONS

SLEEP_BETWEEN_CALLS = float(os.getenv("API_SPORTS_SLEEP", "0.6"))
MAX_RETRIES = int(os.getenv("API_SPORTS_MAX_RETRIES", "4"))
TIMEOUT_SECONDS = int(os.getenv("API_SPORTS_TIMEOUT_SECONDS", "30"))
MAX_GAMES = int(os.getenv("API_SPORTS_MAX_GAMES", "0"))

START_DATE = os.getenv("API_SPORTS_START_DATE", "").strip()
END_DATE = os.getenv("API_SPORTS_END_DATE", "").strip()
DAYS_BACK = int(os.getenv("API_SPORTS_DAYS_BACK", "30"))

KEEP_COLS = [
    "player_name", "season", "GAME_DATE", "MATCHUP",
    "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
]


def _headers() -> dict[str, str]:
    return {"x-apisports-key": API_KEY}


def _request(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}/{path}"
    params = params or {}
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=TIMEOUT_SECONDS)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("errors"):
                errors = data.get("errors") or {}
                # API-Sports returns 200 with an error payload for rate limits / plan issues.
                if "rateLimit" in errors:
                    raise RuntimeError("rateLimit")
                if "requests" in errors:
                    raise RuntimeError("daily_limit")
                raise RuntimeError(f"API error: {errors}")
            return data
        except Exception as exc:
            last_err = exc
            if attempt >= MAX_RETRIES:
                break
            sleep_s = 65 if "rateLimit" in str(exc) else min(60, (2 ** attempt) * 1.2)
            time.sleep(sleep_s)
    raise RuntimeError(f"API request failed for {path}: {last_err}")


def _season_label(season: str) -> str:
    try:
        year = int(season)
    except Exception:
        return season
    return f"{year}-{str(year + 1)[-2:]}"


def _parse_date(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _date_window() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if START_DATE:
        start = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
    else:
        start = now - timedelta(days=DAYS_BACK)
    if END_DATE:
        end = datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc)
    else:
        end = now + timedelta(days=1)
    return start, end


def _load_games(season: str) -> list[dict[str, Any]]:
    data = _request("games", {"season": season, "league": "standard"})
    return data.get("response", []) if isinstance(data, dict) else []


def _matchup(team_code: str, home_code: str, away_code: str) -> str:
    if team_code == home_code:
        return f"{home_code} vs {away_code}"
    if team_code == away_code:
        return f"{away_code} @ {home_code}"
    # fallback if team code missing
    return f"{home_code} vs {away_code}"


def _row_from_player(season_label: str, game: dict[str, Any], player: dict[str, Any]) -> dict[str, Any]:
    team_code = str(player.get("team", {}).get("code") or "").strip()
    home_code = str(game.get("teams", {}).get("home", {}).get("code") or "").strip()
    away_code = str(game.get("teams", {}).get("visitors", {}).get("code") or "").strip()

    first = str(player.get("player", {}).get("firstname") or "").strip()
    last = str(player.get("player", {}).get("lastname") or "").strip()
    name = f"{first} {last}".strip()

    game_dt = _parse_date(str(game.get("date", {}).get("start") or ""))
    game_date = game_dt.date().isoformat() if game_dt else ""

    return {
        "player_name": name,
        "season": season_label,
        "GAME_DATE": game_date,
        "MATCHUP": _matchup(team_code, home_code, away_code),
        "MIN": player.get("min"),
        "PTS": player.get("points"),
        "REB": player.get("totReb"),
        "AST": player.get("assists"),
        "STL": player.get("steals"),
        "BLK": player.get("blocks"),
        "TOV": player.get("turnovers"),
        "FGM": player.get("fgm"),
        "FGA": player.get("fga"),
        "FG3M": player.get("tpm"),
        "FG3A": player.get("tpa"),
        "FTM": player.get("ftm"),
        "FTA": player.get("fta"),
    }


def main() -> None:
    if not API_KEY:
        raise RuntimeError("Missing API_SPORTS_KEY. Set API_SPORTS_KEY env var.")

    start_dt, end_dt = _date_window()
    all_rows: list[dict[str, Any]] = []

    hit_daily_limit = False

    for season in SEASONS:
        games = _load_games(season)
        season_label = _season_label(season)

        # filter to finished games within date window
        filtered = []
        for g in games:
            status = g.get("status", {})
            if isinstance(status, dict) and status.get("short") != 3:
                continue
            gdt = _parse_date(str(g.get("date", {}).get("start") or ""))
            if not gdt:
                continue
            if not (start_dt <= gdt <= end_dt):
                continue
            filtered.append(g)

        if filtered:
            filtered = sorted(
                filtered,
                key=lambda g: _parse_date(str(g.get("date", {}).get("start") or "")) or datetime.min.replace(tzinfo=timezone.utc),
            )
        if MAX_GAMES > 0:
            # keep most recent games
            filtered = filtered[-MAX_GAMES:]

        for game in filtered:
            gid = game.get("id")
            if not gid:
                continue
            try:
                stats = _request("players/statistics", {"game": gid})
            except RuntimeError as exc:
                if "daily_limit" in str(exc):
                    print("Daily request limit reached; stopping early.")
                    hit_daily_limit = True
                    break
                raise
            players = stats.get("response", []) if isinstance(stats, dict) else []
            for player in players:
                all_rows.append(_row_from_player(season_label, game, player))
            time.sleep(SLEEP_BETWEEN_CALLS)
        if hit_daily_limit:
            break

    if not all_rows:
        print("No rows fetched from API-Sports.")
        return

    df_new = pd.DataFrame(all_rows)
    df_new = df_new[KEEP_COLS]

    if OUT_PATH.exists():
        df_old = pd.read_csv(OUT_PATH)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    # Deduplicate by player + date + matchup
    df = df.drop_duplicates(subset=["player_name", "GAME_DATE", "MATCHUP"], keep="last")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"Saved {len(df)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
