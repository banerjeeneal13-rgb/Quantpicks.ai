import os
from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

TRAIN_PARQUET = DATA_DIR / "points_training.parquet"
OUT_CSV = DATA_DIR / "points_feature_cache.csv"
INJURIES_CSV = DATA_DIR / "injuries_today.csv"
LOGS_CSV = Path(os.getenv("NBA_LOGS_CSV") or (DATA_DIR / "nba_player_logs_points_all.csv"))

# Must match train_points_model.py FEATURES
FEATURES = [
    "pts_ma_5", "pts_ma_10",
    "min_ma_5", "min_ma_10",
    "fga_ma_5", "fga_ma_10",
    "is_home", "days_rest",
    "team_pace", "team_off_rating", "team_def_rating",
    "opp_pace", "opp_off_rating", "opp_def_rating",
    "pace_diff", "def_diff",
    "usage_proxy", "pace_x_min", "def_x_usage", "opp_def_x_fga", "opp_pace_x_min",
    "pts_per_min_10", "pts_per_fga_10",
    "proj_min", "proj_pts_from_min",
    "teammate_out_count", "starter_out_count",
    "player_status_q", "player_status_d", "player_status_o",
    "opp_pos_def_fg_pct",
    "off_matchup_pts_per_min",
    "off_matchup_fga_per_min",
    "off_matchup_fg_pct",
    "off_matchup_fg3_pct",
    "def_matchup_fg_pct_allowed",
    "def_matchup_pts_per_min_allowed",
    "etl_ts_pct_ma_5", "etl_ts_pct_ma_10",
    "etl_efg_pct_ma_5", "etl_efg_pct_ma_10",
    "etl_usg_pct_ma_5", "etl_usg_pct_ma_10",
    "etl_tov_pct_ma_5", "etl_tov_pct_ma_10",
    "etl_possessions_ma_5", "etl_possessions_ma_10",
    "etl_pace_ma_5", "etl_pace_ma_10",
    "opp_zone_rim_fg_pct",
    "opp_zone_paint_fg_pct",
    "opp_zone_mid_fg_pct",
    "opp_zone_three_fg_pct",
    "rapm",
    "orapm",
    "drapm",
    "rapm_rank",
    "br_pg_min", "br_pg_pts", "br_pg_fga", "br_pg_3pa", "br_pg_fta",
    "br_pg_fg_pct", "br_pg_3p_pct", "br_pg_ft_pct", "br_pg_efg_pct",
    "br_pg_reb", "br_pg_ast", "br_pg_stl", "br_pg_blk", "br_pg_tov",
    "br_adv_per", "br_adv_ts_pct", "br_adv_3par", "br_adv_ftr",
    "br_adv_orb_pct", "br_adv_drb_pct", "br_adv_trb_pct",
    "br_adv_ast_pct", "br_adv_stl_pct", "br_adv_blk_pct",
    "br_adv_tov_pct", "br_adv_usg_pct",
    "br_adv_bpm", "br_adv_obpm", "br_adv_dbpm", "br_adv_vorp",
    "br_p36_pts", "br_p36_fga", "br_p36_3pa", "br_p36_fta",
    "br_p36_trb", "br_p36_ast", "br_p36_stl", "br_p36_blk", "br_p36_tov",
    "br_p100_pts", "br_p100_fga", "br_p100_3pa", "br_p100_fta",
    "br_p100_trb", "br_p100_ast", "br_p100_stl", "br_p100_blk", "br_p100_tov",
    "br_p100_ortg", "br_p100_drtg",
    "br_shot_avg_dist", "br_shot_pct_2p", "br_shot_pct_0_3", "br_shot_pct_3_10",
    "br_shot_pct_10_16", "br_shot_pct_16_3p", "br_shot_pct_3p",
    "br_shot_fg_2p", "br_shot_fg_0_3", "br_shot_fg_3_10", "br_shot_fg_10_16",
    "br_shot_fg_16_3p", "br_shot_fg_3p",
    "br_shot_ast_2p", "br_shot_ast_3p", "br_shot_dunks_pct",
    "br_shot_corner3_share", "br_shot_corner3_pct",
    "br_pbp_pg_pct", "br_pbp_sg_pct", "br_pbp_sf_pct", "br_pbp_pf_pct", "br_pbp_c_pct",
    "br_pbp_on_off_100", "br_pbp_net_100",
    "br_pbp_bad_pass_tov", "br_pbp_lost_ball_tov",
    "br_pbp_shoot_foul_drawn", "br_pbp_off_foul_drawn",
    "br_pbp_pts_ast", "br_pbp_fga_blocked",
    "br_team_pts_pg", "br_team_fg_pct", "br_team_3p_pct", "br_team_ft_pct",
    "br_team_trb_pg", "br_team_ast_pg", "br_team_tov_pg",
    "br_team_pts_100", "br_team_fg_pct_100", "br_team_3p_pct_100",
    "br_team_ft_pct_100", "br_team_trb_100", "br_team_ast_100", "br_team_tov_100",
    "br_opp_pts_pg", "br_opp_fg_pct", "br_opp_3p_pct", "br_opp_ft_pct",
    "br_opp_trb_pg", "br_opp_ast_pg", "br_opp_tov_pg",
    "br_opp_pts_100", "br_opp_fg_pct_100", "br_opp_3p_pct_100",
    "br_opp_ft_pct_100", "br_opp_trb_100", "br_opp_ast_100", "br_opp_tov_100",
]


def normalize_status(s: str) -> str:
    s = str(s or "").strip().upper()
    if s in ["OUT", "O"]:
        return "OUT"
    if s in ["Q", "QUESTIONABLE"]:
        return "Q"
    if s in ["D", "DOUBTFUL"]:
        return "D"
    if s in ["P", "PROBABLE"]:
        return "P"
    return s or "Q"


def infer_season_from_date(date_str: str) -> str | None:
    try:
        dt = pd.to_datetime(date_str, errors="coerce")
    except Exception:
        dt = None
    if dt is None or pd.isna(dt):
        return None
    year = int(dt.year)
    if int(dt.month) >= 8:
        start = year
        end = year + 1
    else:
        start = year - 1
        end = year
    return f"{start}-{str(end)[-2:]}"


def build_starters_map(df: pd.DataFrame) -> dict[tuple[str, str], set[str]]:
    if "team_abbr" not in df.columns:
        return {}
    player_avg_min = (
        df.groupby(["season", "team_abbr", "player_name"], as_index=False)["min_ma_10"]
          .mean()
          .rename(columns={"min_ma_10": "avg_min"})
    )
    starters = {}
    for (season, team), g in player_avg_min.groupby(["season", "team_abbr"]):
        top = g.sort_values("avg_min", ascending=False).head(5)
        starters[(season, team)] = set(top["player_name"].astype(str))
    return starters


def build_display_last10() -> pd.DataFrame:
    if not LOGS_CSV.exists():
        return pd.DataFrame(columns=["player_name", "season", "display_pts_last10", "display_min_last10", "display_fga_last10"])
    logs = pd.read_csv(LOGS_CSV)
    required = ["player_name", "season", "GAME_DATE", "MIN", "PTS", "FGA"]
    missing = [c for c in required if c not in logs.columns]
    if missing:
        return pd.DataFrame(columns=["player_name", "season", "display_pts_last10", "display_min_last10", "display_fga_last10"])

    logs["season"] = logs["season"].astype(str)
    logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"], errors="coerce", format="mixed").dt.date
    logs = logs[logs["GAME_DATE"].notna()].copy()
    logs["MIN"] = pd.to_numeric(logs["MIN"], errors="coerce")
    logs["PTS"] = pd.to_numeric(logs["PTS"], errors="coerce")
    logs["FGA"] = pd.to_numeric(logs["FGA"], errors="coerce")
    logs = logs[logs["MIN"].notna() & logs["PTS"].notna() & logs["FGA"].notna()]
    logs = logs[logs["MIN"] >= 5].copy()

    logs = logs.sort_values(["player_name", "season", "GAME_DATE"])

    def last10_stats(g: pd.DataFrame) -> pd.Series:
        last10 = g.tail(10)
        return pd.Series(
            {
                "display_pts_last10": float(last10["PTS"].mean()) if len(last10) else np.nan,
                "display_min_last10": float(last10["MIN"].mean()) if len(last10) else np.nan,
                "display_fga_last10": float(last10["FGA"].mean()) if len(last10) else np.nan,
            }
        )

    display = (
        logs.groupby(["player_name", "season"], as_index=False, group_keys=False)
        .apply(last10_stats)
        .reset_index(drop=True)
    )
    return display


def main():
    if not TRAIN_PARQUET.exists():
        raise RuntimeError(f"Missing {TRAIN_PARQUET}. Run build_points_dataset.py first.")

    df = pd.read_parquet(TRAIN_PARQUET)

    required = ["player_name", "season", "GAME_DATE", "team_abbr"] + FEATURES
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Feature cache build missing columns: {missing}. Have: {list(df.columns)}")

    # For each player, take their most recent row (latest GAME_DATE)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    df = df.sort_values(["player_name", "season", "GAME_DATE"]).copy()

    latest = (
        df.groupby(["player_name", "season"], as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    out = latest[["player_name", "season", "GAME_DATE", "team_abbr"] + FEATURES].copy()

    # Convert GAME_DATE back to string for predictable CSV behavior
    out["GAME_DATE"] = out["GAME_DATE"].dt.date.astype(str)

    # Add display-only last-10 stats (includes most recent 10 played games, MIN>=5)
    display = build_display_last10()
    if len(display) > 0:
        out = out.merge(display, how="left", on=["player_name", "season"])

    # Optionally override injury features with today's report
    if INJURIES_CSV.exists():
        inj = pd.read_csv(INJURIES_CSV)
        if len(inj) > 0:
            for c in ["player_name", "team_abbr", "status", "game_date"]:
                if c not in inj.columns:
                    inj[c] = None

            inj["player_name"] = inj["player_name"].astype(str).str.strip()
            inj["team_abbr"] = inj["team_abbr"].astype(str).str.strip().str.upper()
            inj["status"] = inj["status"].apply(normalize_status)
            inj["season"] = inj["game_date"].apply(infer_season_from_date)

            status_rank = {"OUT": 3, "D": 2, "Q": 1, "P": 0}
            inj["status_rank"] = inj["status"].map(status_rank).fillna(0).astype(int)
            inj_best = inj.sort_values("status_rank").groupby("player_name").tail(1)
            status_map = dict(zip(inj_best["player_name"], inj_best["status"]))

            out_sets = (
                inj[inj["status"] == "OUT"]
                .groupby("team_abbr")["player_name"]
                .apply(lambda s: set(s.astype(str)))
                .to_dict()
            )
            team_out_counts = {k: float(len(v)) for k, v in out_sets.items()}
            starters_map = build_starters_map(df)

            def apply_injury_row(row: pd.Series) -> pd.Series:
                team = str(row.get("team_abbr") or "").upper()
                season = str(row.get("season") or "")
                out_set = out_sets.get(team, set())
                row["teammate_out_count"] = team_out_counts.get(team, 0.0)
                row["starter_out_count"] = float(len(starters_map.get((season, team), set()) & out_set))

                status = status_map.get(str(row.get("player_name") or ""), "P")
                row["player_status_q"] = 1.0 if status == "Q" else 0.0
                row["player_status_d"] = 1.0 if status == "D" else 0.0
                row["player_status_o"] = 1.0 if status == "OUT" else 0.0
                return row

            out = out.apply(apply_injury_row, axis=1)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Saved: {OUT_CSV}")
    print("Rows:", len(out))
    print("Players:", out["player_name"].nunique())
    print("Cols:", list(out.columns))

    print(out.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
