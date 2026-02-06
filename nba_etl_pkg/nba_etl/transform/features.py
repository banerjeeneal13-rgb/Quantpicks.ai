from __future__ import annotations

from pathlib import Path
import pandas as pd


ROLL_WINDOWS = [1, 3, 5, 10]


def build_features(games_csv: Path, player_csv: Path, team_csv: Path, out_csv: Path, out_parquet: Path) -> None:
    games = pd.read_csv(games_csv)
    players = pd.read_csv(player_csv)
    teams = pd.read_csv(team_csv)

    # normalize date
    games["game_date"] = pd.to_datetime(games["date_utc"], errors="coerce")
    players = players.merge(
        games[["game_id", "game_date", "home_team_id", "away_team_id"]],
        on="game_id",
        how="left",
    )
    players["is_home"] = (players["team_id"].astype(str) == players["home_team_id"].astype(str)).astype(int)
    players["opp_team_id"] = players.apply(
        lambda r: r["away_team_id"] if str(r["team_id"]) == str(r["home_team_id"]) else r["home_team_id"],
        axis=1,
    )

    # rest days
    players = players.sort_values(["player_id", "game_date"])
    players["rest_days"] = players.groupby("player_id")["game_date"].diff().dt.days.fillna(2).clip(lower=0)
    players["back_to_back"] = (players["rest_days"] <= 1).astype(int)

    stat_cols = ["pts", "reb", "ast", "stl", "blk", "tov", "fga", "fgm", "fg3a", "fg3m", "fta", "ftm", "minutes"]
    for col in stat_cols:
        for w in ROLL_WINDOWS:
            players[f"{col}_ma_{w}"] = players.groupby("player_id")[col].transform(
                lambda s: s.shift(1).rolling(w, min_periods=1).mean()
            )
            players[f"{col}_std_{w}"] = players.groupby("player_id")[col].transform(
                lambda s: s.shift(1).rolling(w, min_periods=1).std()
            )

    # opponent allowed stats (team defense proxy)
    teams = teams.merge(games[["game_id", "game_date", "home_team_id", "away_team_id"]], on="game_id", how="left")
    teams["opp_team_id"] = teams.apply(
        lambda r: r["away_team_id"] if str(r["team_id"]) == str(r["home_team_id"]) else r["home_team_id"], axis=1
    )
    opp = teams[["game_id", "team_id", "pts", "fga", "fgm", "reb", "game_date"]].copy()
    opp = opp.rename(
        columns={"team_id": "opp_team_id", "pts": "opp_pts", "fga": "opp_fga", "fgm": "opp_fgm", "reb": "opp_reb"}
    )
    teams = teams.merge(opp, on=["game_id", "opp_team_id"], how="left")

    teams = teams.sort_values(["team_id", "game_date"])
    for w in ROLL_WINDOWS:
        teams[f"opp_pts_allowed_ma_{w}"] = teams.groupby("team_id")["opp_pts"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=1).mean()
        )
        opp_fgm_roll = teams.groupby("team_id")["opp_fgm"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=1).sum()
        )
        opp_fga_roll = teams.groupby("team_id")["opp_fga"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=1).sum()
        )
        teams[f"opp_fg_pct_allowed_ma_{w}"] = opp_fgm_roll / opp_fga_roll.replace(0, pd.NA)
        teams[f"opp_reb_allowed_ma_{w}"] = teams.groupby("team_id")["opp_reb"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=1).mean()
        )

    # join opponent allowed features to players
    opp_feature_cols = [c for c in teams.columns if "_ma_" in c and c.startswith("opp_")]
    players = players.merge(
        teams[["game_id", "team_id"] + opp_feature_cols],
        left_on=["game_id", "opp_team_id"],
        right_on=["game_id", "team_id"],
        how="left",
        suffixes=("", "_opp"),
    )
    if "team_id_opp" in players.columns:
        players = players.drop(columns=["team_id_opp"])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    players.to_csv(out_csv, index=False)
    players.to_parquet(out_parquet, index=False)
