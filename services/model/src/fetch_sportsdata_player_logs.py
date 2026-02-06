import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

BASE_URL = "https://api.sportsdata.io/v3/nba/stats/json"
API_KEY = os.getenv("SPORTS_DATA_IO_KEY", "").strip()

SEASON = os.getenv("SPORTS_DATA_SEASON", "2024")
MAX_GAMES = int(os.getenv("SPORTS_DATA_MAX_GAMES", "1000"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SPORTS_DATA_SLEEP", "0.6"))
TIMEOUT_SECONDS = int(os.getenv("SPORTS_DATA_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("SPORTS_DATA_MAX_RETRIES", "4"))

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_PATH = Path(os.getenv("SPORTS_DATA_LOGS_OUT") or (DATA_DIR / "sportsdata_player_logs.csv"))
CHECKPOINT_PATH = Path(os.getenv("SPORTS_DATA_CHECKPOINT") or (DATA_DIR / "sportsdata_pull_checkpoint.csv"))

KEEP_COLS = [
    "player_name", "season", "GAME_DATE", "MATCHUP",
    "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
]


def sdio_get(path: str) -> list[dict]:
    if not API_KEY:
        raise RuntimeError("Missing SPORTS_DATA_IO_KEY env var")
    url = f"{BASE_URL}/{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(url, params={"key": API_KEY}, timeout=TIMEOUT_SECONDS)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            wait = min(90, int((attempt ** 2) * 2))
            print(f"Retry {attempt}/{MAX_RETRIES} -> {path} | {str(e)[:160]} | sleep {wait}s")
            time.sleep(wait)
    return []


def load_checkpoint() -> set[str]:
    if CHECKPOINT_PATH.exists():
        df = pd.read_csv(CHECKPOINT_PATH)
        return set(df["key"].astype(str).tolist())
    return set()


def save_checkpoint(done: set[str]):
    pd.DataFrame({"key": sorted(done)}).to_csv(CHECKPOINT_PATH, index=False)


def append_rows(df: pd.DataFrame):
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    header = not OUT_PATH.exists()
    df.to_csv(OUT_PATH, mode="a", header=header, index=False)


def format_matchup(home: str, away: str, team: str) -> str:
    if not home or not away or not team:
        return ""
    return f"{team} vs {away}" if team == home else f"{team} @ {home}"


def map_player_rows(rows: list[dict], player_name: str, season: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=KEEP_COLS)
    out = []
    for r in rows:
        team = r.get("Team") or r.get("TeamAbbreviation")
        home = r.get("HomeTeam")
        away = r.get("AwayTeam")
        matchup = format_matchup(home, away, team)
        out.append(
            {
                "player_name": player_name,
                "season": season,
                "GAME_DATE": r.get("Date"),
                "MATCHUP": matchup,
                "MIN": r.get("Minutes"),
                "PTS": r.get("Points"),
                "REB": r.get("Rebounds"),
                "AST": r.get("Assists"),
                "STL": r.get("Steals"),
                "BLK": r.get("BlockedShots"),
                "TOV": r.get("Turnovers"),
                "FGM": r.get("FieldGoalsMade"),
                "FGA": r.get("FieldGoalsAttempted"),
                "FG3M": r.get("ThreePointersMade"),
                "FG3A": r.get("ThreePointersAttempted"),
                "FTM": r.get("FreeThrowsMade"),
                "FTA": r.get("FreeThrowsAttempted"),
            }
        )
    df = pd.DataFrame(out)
    return df[KEEP_COLS]


def main():
    if not API_KEY:
        raise RuntimeError("Set SPORTS_DATA_IO_KEY in your environment.")

    players = sdio_get(f"PlayerSeasonStats/{SEASON}")
    if not players:
        raise RuntimeError(f"No players returned for season {SEASON}")

    done = load_checkpoint()
    total_rows = 0
    errors = 0

    for p in players:
        pid = p.get("PlayerID")
        name = p.get("Name") or p.get("PlayerName")
        if not pid or not name:
            continue
        key = f"{pid}:{SEASON}"
        if key in done:
            continue

        rows = sdio_get(f"PlayerGameStatsBySeason/{SEASON}/{pid}/{MAX_GAMES}")
        df = map_player_rows(rows, name, SEASON)
        if len(df) == 0:
            errors += 1
        else:
            append_rows(df)
            total_rows += len(df)

        done.add(key)
        save_checkpoint(done)
        time.sleep(SLEEP_BETWEEN_CALLS)

    print("DONE.")
    print("Rows saved:", total_rows)
    print("Errors:", errors)
    print("Output file:", OUT_PATH)
    print("Checkpoint file:", CHECKPOINT_PATH)


if __name__ == "__main__":
    main()
