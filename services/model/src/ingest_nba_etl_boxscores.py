import os
from pathlib import Path
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

ETL_DIR = Path(os.getenv("NBA_ETL_OUTPUT_DIR") or (ROOT_DIR / "nba_etl_pkg" / "nba_etl_output"))
GAMES_CSV = ETL_DIR / "data" / "games.csv"
PLAYERS_CSV = ETL_DIR / "data" / "player_boxscores.csv"
TEAMS_CSV = ETL_DIR / "metadata" / "teams.csv"
TEAM_BOXSCORES_CSV = ETL_DIR / "data" / "team_boxscores.csv"

OUT_CSV = Path(os.getenv("NBA_ETL_LOGS_OUT") or (DATA_DIR / "nba_player_logs_points_all.csv"))
OUT_ETL_ADV = Path(os.getenv("NBA_ETL_ADV_OUT") or (DATA_DIR / "etl_player_advanced.csv"))

KEEP_COLS = [
    "player_name", "season", "GAME_DATE", "MATCHUP",
    "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
]

ETL_ADV_COLS = [
    "player_name",
    "season",
    "GAME_DATE",
    "etl_ts_pct",
    "etl_efg_pct",
    "etl_usg_pct",
    "etl_tov_pct",
    "etl_possessions",
    "etl_pace",
]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    return a / b.replace(0, pd.NA)


def _load_team_map(players: pd.DataFrame) -> pd.Series:
    parts = []
    if "team_id" in players.columns and "team_tricode" in players.columns:
        parts.append(players[["team_id", "team_tricode"]].dropna())
    if TEAMS_CSV.exists():
        teams = pd.read_csv(TEAMS_CSV)
        if "team_id" in teams.columns and "team_tricode" in teams.columns:
            parts.append(teams[["team_id", "team_tricode"]].dropna())
    if not parts:
        return pd.Series(dtype=str)
    merged = pd.concat(parts, ignore_index=True)
    merged["team_id"] = merged["team_id"].astype(str)
    merged["team_tricode"] = merged["team_tricode"].astype(str)
    merged = merged.drop_duplicates(subset=["team_id"], keep="last")
    return merged.set_index("team_id")["team_tricode"]


def main() -> None:
    if not GAMES_CSV.exists():
        raise RuntimeError(f"Missing games.csv at {GAMES_CSV}")
    if not PLAYERS_CSV.exists():
        raise RuntimeError(f"Missing player_boxscores.csv at {PLAYERS_CSV}")

    games = pd.read_csv(GAMES_CSV)
    players = pd.read_csv(PLAYERS_CSV)

    required_games = ["game_id", "season", "date_utc", "home_team_id", "away_team_id"]
    required_players = [
        "game_id", "player_name", "team_id", "team_tricode", "minutes",
        "pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    ]
    missing_games = [c for c in required_games if c not in games.columns]
    missing_players = [c for c in required_players if c not in players.columns]
    if missing_games:
        raise RuntimeError(f"Missing columns in games.csv: {missing_games}")
    if missing_players:
        raise RuntimeError(f"Missing columns in player_boxscores.csv: {missing_players}")

    team_map = _load_team_map(players)

    games["home_team_id"] = games["home_team_id"].astype(str)
    games["away_team_id"] = games["away_team_id"].astype(str)

    players["team_id"] = players["team_id"].astype(str)
    players["team_abbr"] = players["team_tricode"].astype(str).where(players["team_tricode"].notna(), None)
    if team_map is not None and not team_map.empty:
        players["team_abbr"] = players["team_abbr"].where(players["team_abbr"].notna(), players["team_id"].map(team_map))

    df = players.merge(
        games[["game_id", "season", "date_utc", "home_team_id", "away_team_id"]],
        on="game_id",
        how="left",
    )
    if "season" not in df.columns:
        if "season_x" in df.columns:
            df["season"] = df["season_x"]
        elif "season_y" in df.columns:
            df["season"] = df["season_y"]
    if "date_utc" not in df.columns and "date_utc_y" in df.columns:
        df["date_utc"] = df["date_utc_y"]

    df["GAME_DATE"] = pd.to_datetime(df["date_utc"], errors="coerce").dt.date.astype(str)
    df["home_abbr"] = df["home_team_id"].map(team_map)
    df["away_abbr"] = df["away_team_id"].map(team_map)

    df["MATCHUP"] = None
    home_mask = df["team_id"] == df["home_team_id"]
    away_mask = df["team_id"] == df["away_team_id"]
    df.loc[home_mask, "MATCHUP"] = df.loc[home_mask, "team_abbr"] + " vs " + df.loc[home_mask, "away_abbr"]
    df.loc[away_mask, "MATCHUP"] = df.loc[away_mask, "team_abbr"] + " @ " + df.loc[away_mask, "home_abbr"]

    out = pd.DataFrame(
        {
            "player_name": df["player_name"],
            "season": df["season"],
            "GAME_DATE": df["GAME_DATE"],
            "MATCHUP": df["MATCHUP"],
            "MIN": df["minutes"],
            "PTS": df["pts"],
            "REB": df["reb"],
            "AST": df["ast"],
            "STL": df["stl"],
            "BLK": df["blk"],
            "TOV": df["tov"],
            "FGM": df["fgm"],
            "FGA": df["fga"],
            "FG3M": df["fg3m"],
            "FG3A": df["fg3a"],
            "FTM": df["ftm"],
            "FTA": df["fta"],
        }
    )

    out = out.dropna(subset=["player_name", "GAME_DATE", "MATCHUP"])

    if OUT_CSV.exists():
        existing = pd.read_csv(OUT_CSV)
        merged = pd.concat([existing, out], ignore_index=True)
    else:
        merged = out

    merged = merged.drop_duplicates(subset=["player_name", "GAME_DATE", "MATCHUP"], keep="last")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)
    print(f"Saved {len(merged)} rows to {OUT_CSV}")

    # Build per-game advanced metrics from ETL boxscores when team totals are available.
    if TEAM_BOXSCORES_CSV.exists():
        teams = pd.read_csv(TEAM_BOXSCORES_CSV)
        req_team_cols = ["game_id", "team_id", "minutes", "fga", "fgm", "fg3m", "fta", "tov"]
        if all(c in teams.columns for c in req_team_cols):
            teams["game_id"] = teams["game_id"].astype(str)
            teams["team_id"] = teams["team_id"].astype(str)

            teams = teams[req_team_cols].copy()
            teams = teams.rename(
                columns={
                    "minutes": "team_min",
                    "fga": "team_fga",
                    "fgm": "team_fgm",
                    "fg3m": "team_fg3m",
                    "fta": "team_fta",
                    "tov": "team_tov",
                }
            )
            opp = teams.rename(
                columns={
                    "team_id": "opp_team_id",
                    "team_min": "opp_min",
                    "team_fga": "opp_fga",
                    "team_fgm": "opp_fgm",
                    "team_fg3m": "opp_fg3m",
                    "team_fta": "opp_fta",
                    "team_tov": "opp_tov",
                }
            )

            player_adv = players.merge(
                games[["game_id", "season", "date_utc", "home_team_id", "away_team_id"]],
                on="game_id",
                how="left",
            )
            if "season" not in player_adv.columns:
                if "season_x" in player_adv.columns:
                    player_adv["season"] = player_adv["season_x"]
                elif "season_y" in player_adv.columns:
                    player_adv["season"] = player_adv["season_y"]
            player_adv["game_id"] = player_adv["game_id"].astype(str)
            player_adv["team_id"] = player_adv["team_id"].astype(str)
            player_adv["GAME_DATE"] = pd.to_datetime(player_adv["date_utc"], errors="coerce").dt.date.astype(str)

            player_adv = player_adv.merge(teams, on=["game_id", "team_id"], how="left")
            player_adv["opp_team_id"] = player_adv.apply(
                lambda r: r["away_team_id"] if str(r["team_id"]) == str(r["home_team_id"]) else r["home_team_id"],
                axis=1,
            )
            player_adv = player_adv.merge(
                opp,
                left_on=["game_id", "opp_team_id"],
                right_on=["game_id", "opp_team_id"],
                how="left",
            )

            fga = player_adv["fga"].astype(float)
            fgm = player_adv["fgm"].astype(float)
            fg3m = player_adv["fg3m"].astype(float)
            fta = player_adv["fta"].astype(float)
            tov = player_adv["tov"].astype(float)
            pts = player_adv["pts"].astype(float)
            minutes = player_adv["minutes"].astype(float)

            ts_denom = 2 * (fga + 0.44 * fta)
            player_adv["etl_ts_pct"] = _safe_div(pts, ts_denom)
            player_adv["etl_efg_pct"] = _safe_div((fgm + 0.5 * fg3m), fga)
            player_adv["etl_tov_pct"] = _safe_div(tov, (fga + 0.44 * fta + tov))

            team_fga = player_adv["team_fga"].astype(float)
            team_fta = player_adv["team_fta"].astype(float)
            team_tov = player_adv["team_tov"].astype(float)
            opp_fga = player_adv["opp_fga"].astype(float)
            opp_fta = player_adv["opp_fta"].astype(float)
            opp_tov = player_adv["opp_tov"].astype(float)
            team_min = player_adv["team_min"].astype(float)

            team_poss = (team_fga + 0.44 * team_fta + team_tov + opp_fga + 0.44 * opp_fta + opp_tov) / 2.0
            player_adv["etl_possessions"] = team_poss
            player_adv["etl_pace"] = _safe_div(team_poss * 48, team_min / 5)

            usg_denom = team_fga + 0.44 * team_fta + team_tov
            player_adv["etl_usg_pct"] = _safe_div((fga + 0.44 * fta + tov) * (team_min / 5), minutes * usg_denom) * 100

            etl_out = player_adv[ETL_ADV_COLS].copy()
            etl_out = etl_out.dropna(subset=["player_name", "season", "GAME_DATE"])
            etl_out = etl_out.drop_duplicates(subset=["player_name", "season", "GAME_DATE"], keep="last")

            OUT_ETL_ADV.parent.mkdir(parents=True, exist_ok=True)
            etl_out.to_csv(OUT_ETL_ADV, index=False)
            print(f"Saved {len(etl_out)} rows to {OUT_ETL_ADV}")


if __name__ == "__main__":
    main()
