import os
from pathlib import Path
import pandas as pd
import numpy as np

IN_CSV = Path(os.getenv("NBA_LOGS_CSV") or (Path(__file__).resolve().parents[1] / "data" / "nba_player_logs_points_all.csv"))
OUT_CSV = Path(__file__).resolve().parents[1] / "data" / "team_lineup_context_5.csv"

ROLL_N = 5

def main():
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Missing input: {IN_CSV}")

    df = pd.read_csv(IN_CSV)

    # Required columns we expect from your logs pull
    needed = ["player_name", "season", "GAME_DATE", "MATCHUP", "MIN", "PTS", "FGA"]
    for c in needed:
        if c not in df.columns:
            raise KeyError(f"Missing column {c} in {IN_CSV}. Has: {list(df.columns)}")

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df = df.dropna(subset=["GAME_DATE"])

    for c in ["MIN", "PTS", "FGA"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["MIN", "PTS", "FGA"])

    # Extract team abbreviation from MATCHUP (same logic you used)
    parts = df["MATCHUP"].astype(str).str.split(" ", n=2, expand=True)
    df["team_abbr"] = parts[0].astype(str).str.strip()

    # Pre-compute rotation sets per team/season (proxy for starters/rotation)
    player_avg_min = (
        df.groupby(["season", "team_abbr", "player_name"], as_index=False)["MIN"]
          .mean()
          .rename(columns={"MIN": "avg_min"})
    )

    starters_map = {}
    rotation_map = {}
    for (season, team), g in player_avg_min.groupby(["season", "team_abbr"]):
        g_sorted = g.sort_values("avg_min", ascending=False)
        starters_map[(season, team)] = set(g_sorted.head(5)["player_name"].astype(str))
        rotation_map[(season, team)] = set(g_sorted.head(8)["player_name"].astype(str))

    # Players who appeared in each team-game
    players_in_game = (
        df.groupby(["season", "GAME_DATE", "team_abbr"])["player_name"]
          .apply(lambda s: set(s.astype(str)))
          .reset_index()
          .rename(columns={"player_name": "players_in_game"})
    )

    def missing_count(row, ref_map):
        key = (row["season"], row["team_abbr"])
        ref = ref_map.get(key, set())
        return len(ref - row["players_in_game"])

    players_in_game["starter_out_count"] = players_in_game.apply(
        lambda r: missing_count(r, starters_map), axis=1
    )
    players_in_game["teammate_out_count"] = players_in_game.apply(
        lambda r: missing_count(r, rotation_map), axis=1
    )

    # Aggregate to team-game totals
    team_game = (
        df.groupby(["season", "GAME_DATE", "team_abbr"], as_index=False)
          .agg(
              team_min=("MIN", "sum"),
              team_pts=("PTS", "sum"),
              team_fga=("FGA", "sum"),
              n_players=("player_name", "nunique"),
          )
    )

    # Get top teammate minutes + fga shares for each team-game (captures missing starters patterns)
    df_sorted_min = df.sort_values(["season", "GAME_DATE", "team_abbr", "MIN"], ascending=[True, True, True, False])
    top_min = (
        df_sorted_min.groupby(["season", "GAME_DATE", "team_abbr"])
        .head(2)
        .groupby(["season", "GAME_DATE", "team_abbr"], as_index=False)
        .agg(
            top2_min_sum=("MIN", "sum"),
        )
    )

    df_sorted_fga = df.sort_values(["season", "GAME_DATE", "team_abbr", "FGA"], ascending=[True, True, True, False])
    top_fga = (
        df_sorted_fga.groupby(["season", "GAME_DATE", "team_abbr"])
        .head(2)
        .groupby(["season", "GAME_DATE", "team_abbr"], as_index=False)
        .agg(
            top2_fga_sum=("FGA", "sum"),
        )
    )

    ctx = team_game.merge(top_min, on=["season", "GAME_DATE", "team_abbr"], how="left").merge(
        top_fga, on=["season", "GAME_DATE", "team_abbr"], how="left"
    )
    ctx = ctx.merge(players_in_game, on=["season", "GAME_DATE", "team_abbr"], how="left")

    # Shares (avoid divide by zero)
    ctx["top2_min_share"] = (ctx["top2_min_sum"] / ctx["team_min"]).replace([np.inf, -np.inf], np.nan)
    ctx["top2_fga_share"] = (ctx["top2_fga_sum"] / ctx["team_fga"]).replace([np.inf, -np.inf], np.nan)

    # Rolling 5 game averages per team
    ctx = ctx.sort_values(["season", "team_abbr", "GAME_DATE"])
    for col in ["team_min", "team_pts", "team_fga", "n_players", "top2_min_share", "top2_fga_share"]:
        ctx[f"{col}_r{ROLL_N}"] = (
            ctx.groupby(["season", "team_abbr"])[col]
               .transform(lambda s: s.shift(1).rolling(ROLL_N).mean())
        )

    # Keep only rows where we have rolling context
    roll_cols = [f"{c}_r{ROLL_N}" for c in ["team_min", "team_pts", "team_fga", "n_players", "top2_min_share", "top2_fga_share"]]
    ctx = ctx.dropna(subset=roll_cols).reset_index(drop=True)

    # Normalize date format for joins
    ctx["GAME_DATE"] = pd.to_datetime(ctx["GAME_DATE"]).dt.date.astype(str)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    ctx.to_csv(OUT_CSV, index=False)

    print("Saved:", OUT_CSV)
    print("Rows:", len(ctx))
    print("Teams:", ctx["team_abbr"].nunique())
    print("Cols:", list(ctx.columns))
    head_str = ctx.head(5).to_string(index=False)
    try:
        print(head_str)
    except UnicodeEncodeError:
        print(head_str.encode("ascii", "backslashreplace").decode("ascii"))

if __name__ == "__main__":
    main()
