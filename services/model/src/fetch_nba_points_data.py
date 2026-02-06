import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
import os
import time

def get_player_id(full_name: str) -> int:
    matches = players.find_players_by_full_name(full_name)
    if not matches:
        raise ValueError(f"Player not found: {full_name}")
    # best match is usually first
    return int(matches[0]["id"])

def fetch_player_points_logs(player_name: str, seasons: list[str]) -> pd.DataFrame:
    pid = get_player_id(player_name)
    frames = []
    for season in seasons:
        print("Fetching", player_name, season)
        gl = playergamelog.PlayerGameLog(player_id=pid, season=season, timeout=60)
        df = gl.get_data_frames()[0]
        # standardize
        df["player_name"] = player_name
        df["season"] = season
        frames.append(df)
        time.sleep(0.6)  # be gentle to NBA endpoints
    out = pd.concat(frames, ignore_index=True)
    return out

if __name__ == "__main__":
    # Start with a handful of high-volume players to validate pipeline
    players_to_pull = [
        "Nikola Jokic",
        "Luka Doncic",
        "Shai Gilgeous-Alexander",
        "Jayson Tatum",
        "Giannis Antetokounmpo",
    ]
    default_seasons = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]
    env_seasons = [s.strip() for s in os.getenv("NBA_SEASONS", "").split(",") if s.strip()]
    seasons = env_seasons or default_seasons

    all_rows = []
    for name in players_to_pull:
        try:
            all_rows.append(fetch_player_points_logs(name, seasons))
        except Exception as e:
            print("Skipped", name, "->", e)

    df = pd.concat(all_rows, ignore_index=True)

    # keep columns we care about
    keep = [
        "player_name","season","GAME_DATE","MATCHUP",
        "MIN","PTS","REB","AST","STL","BLK","TOV",
        "FGM","FGA","FG3M","FG3A","FTM","FTA",
    ]
    df = df[keep].copy()

    # write dataset
    out_path = "../data/nba_player_logs_points.csv"
    df.to_csv(out_path, index=False)
    print("Saved:", out_path, "rows=", len(df))
