import os
import pandas as pd
import time
from pathlib import Path
from typing import Optional
from tqdm import tqdm

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog, leaguegamelog

# ===== SETTINGS =====
# Default to past 5 seasons, override with NBA_SEASONS env (comma-separated).
_DEFAULT_SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]
_ENV_SEASONS = os.getenv("NBA_SEASONS", "")
SEASONS = [s.strip() for s in _ENV_SEASONS.split(",") if s.strip()] or _DEFAULT_SEASONS

# Increase these to reduce bans/timeouts (override via env vars)
SLEEP_BETWEEN_CALLS = float(os.getenv("NBA_SLEEP_BETWEEN_CALLS", "1.6"))
MAX_RETRIES = int(os.getenv("NBA_MAX_RETRIES", "6"))
TIMEOUT_SECONDS = int(os.getenv("NBA_TIMEOUT_SECONDS", "90"))
MAX_PLAYERS = int(os.getenv("NBA_MAX_PLAYERS", "0"))
START_INDEX = int(os.getenv("NBA_START_INDEX", "0"))
NBA_PROXY = os.getenv("NBA_API_PROXY", "").strip() or None
USE_LEAGUE_GAMELOG = os.getenv("NBA_USE_LEAGUE_GAMELOG", "0").strip() == "1"

OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "nba_player_logs_points_all.csv"
CHECKPOINT_PATH = Path(__file__).resolve().parents[1] / "data" / "nba_pull_checkpoint.csv"

KEEP_COLS = [
    "player_name","season","GAME_DATE","MATCHUP",
    "MIN","PTS","REB","AST","STL","BLK","TOV",
    "FGM","FGA","FG3M","FG3A","FTM","FTA",
]

# Headers that reduce blocks
NBA_HEADERS = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "x-nba-stats-token": "true",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "x-nba-stats-origin": "stats",
    "Referer": "https://www.nba.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_one_player_season(pid: int, name: str, season: str) -> Optional[pd.DataFrame]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            gl = playergamelog.PlayerGameLog(
                player_id=pid,
                season=season,
                timeout=TIMEOUT_SECONDS,
                headers=NBA_HEADERS,
                proxy=NBA_PROXY,
            )
            df = gl.get_data_frames()[0]
            if df is None or df.empty:
                return None
            df["player_name"] = name
            df["season"] = season
            return df

        except Exception as e:
            msg = str(e)

            # Exponential backoff (helps when NBA starts rate-limiting)
            wait = min(90, int((attempt ** 2) * 2))

            # If connection reset / aborted, wait longer
            if "ConnectionResetError" in msg or "Connection aborted" in msg:
                wait = max(wait, 45)

            print(f"Retry {attempt}/{MAX_RETRIES} -> {name} {season} | {msg[:140]} | sleep {wait}s")
            time.sleep(wait)

    return None

def load_checkpoint() -> set[str]:
    if CHECKPOINT_PATH.exists():
        df = pd.read_csv(CHECKPOINT_PATH)
        return set(df["key"].astype(str).tolist())
    return set()

def save_checkpoint(done_keys: set[str]):
    pd.DataFrame({"key": sorted(done_keys)}).to_csv(CHECKPOINT_PATH, index=False)

def append_rows_to_csv(df: pd.DataFrame):
    header = not OUT_PATH.exists()
    df.to_csv(OUT_PATH, mode="a", header=header, index=False)

def fetch_league_season(season: str) -> Optional[pd.DataFrame]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            lg = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                player_or_team_abbreviation="P",
                timeout=TIMEOUT_SECONDS,
                headers=NBA_HEADERS,
                proxy=NBA_PROXY,
            )
            df = lg.get_data_frames()[0]
            if df is None or df.empty:
                return None
            df["player_name"] = df["PLAYER_NAME"]
            df["season"] = season
            return df
        except Exception as e:
            msg = str(e)
            wait = min(90, int((attempt ** 2) * 2))
            if "ConnectionResetError" in msg or "Connection aborted" in msg:
                wait = max(wait, 45)
            print(f"Retry {attempt}/{MAX_RETRIES} -> LEAGUE {season} | {msg[:140]} | sleep {wait}s")
            time.sleep(wait)
    return None

if __name__ == "__main__":
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_checkpoint()

    if USE_LEAGUE_GAMELOG:
        print("Using leaguegamelog for bulk pulls.")
        print("Saving to:", OUT_PATH)
        print("Checkpoint:", CHECKPOINT_PATH)
        print("Already done:", len(done))

        total_saved = 0
        errors = 0

        for season in SEASONS:
            key = f"LEAGUE:{season}"
            if key in done:
                continue
            df = fetch_league_season(season)
            if df is None:
                errors += 1
            else:
                df = df[KEEP_COLS].copy()
                append_rows_to_csv(df)
                total_saved += len(df)
            done.add(key)
            save_checkpoint(done)
            time.sleep(SLEEP_BETWEEN_CALLS)

        print("DONE.")
        print("Rows saved:", total_saved)
        print("Errors:", errors)
        print("Output file:", OUT_PATH)
        print("Checkpoint file:", CHECKPOINT_PATH)
        raise SystemExit(0)

    active = [p for p in players.get_players() if p.get("is_active")]
    if START_INDEX:
        active = active[START_INDEX:]
    if MAX_PLAYERS > 0:
        active = active[:MAX_PLAYERS]
    print("Active players:", len(active))
    print("Saving to:", OUT_PATH)
    print("Checkpoint:", CHECKPOINT_PATH)

    print("Already done:", len(done))

    total_saved = 0
    errors = 0

    for p in tqdm(active):
        pid = int(p["id"])
        name = p["full_name"]

        for season in SEASONS:
            key = f"{pid}:{season}"
            if key in done:
                continue

            df = fetch_one_player_season(pid, name, season)

            if df is None:
                errors += 1
            else:
                df = df[KEEP_COLS].copy()
                append_rows_to_csv(df)
                total_saved += len(df)

            done.add(key)
            save_checkpoint(done)

            time.sleep(SLEEP_BETWEEN_CALLS)

    print("DONE.")
    print("Rows saved:", total_saved)
    print("Errors:", errors)
    print("Output file:", OUT_PATH)
    print("Checkpoint file:", CHECKPOINT_PATH)
