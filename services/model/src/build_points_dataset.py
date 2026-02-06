import os
import json
from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

LOGS_CSV = Path(os.getenv("NBA_LOGS_CSV") or (DATA_DIR / "nba_player_logs_points_all.csv"))
TEAM_CTX_CSV = DATA_DIR / "team_context.csv"
LINEUP_CTX_CSV = DATA_DIR / "team_lineup_context_5.csv"
INJURIES_HISTORY_GLOB = "injuries_history*.csv"
DEFENDER_CSV = DATA_DIR / "defender_ratings.csv"
MATCHUPS_OFFENSE_CSV = Path(__file__).resolve().parents[1] / "matchups_offense_active_2025-26_Regular_Season.csv"
MATCHUPS_DEFENSE_CSV = Path(__file__).resolve().parents[1] / "matchups_defense_active_2025-26_Regular_Season.csv"
ETL_ADV_CSV = DATA_DIR / "etl_player_advanced.csv"
BR_PLAYER_PER_GAME_CSV = DATA_DIR / "Player Per Game.csv"
BR_ADVANCED_CSV = DATA_DIR / "Advanced.csv"
BR_PER36_CSV = DATA_DIR / "Per 36 Minutes.csv"
BR_PER100_CSV = DATA_DIR / "Per 100 Poss.csv"
BR_SHOOTING_CSV = DATA_DIR / "Player Shooting.csv"
BR_PLAYBYPLAY_CSV = DATA_DIR / "Player Play By Play.csv"
BR_TEAM_PER_GAME_CSV = DATA_DIR / "Team Stats Per Game.csv"
BR_TEAM_PER100_CSV = DATA_DIR / "Team Stats Per 100 Poss.csv"
BR_OPP_PER_GAME_CSV = DATA_DIR / "Opponent Stats Per Game.csv"
BR_OPP_PER100_CSV = DATA_DIR / "Opponent Stats Per 100 Poss.csv"

OUT_PARQUET = DATA_DIR / "points_training.parquet"
OUT_BASE_PARQUET = DATA_DIR / "market_training_base.parquet"

# Base features used by the predictor
BASE_FEATURES = [
    "pts_ma_5", "pts_ma_10",
    "reb_ma_5", "reb_ma_10",
    "ast_ma_5", "ast_ma_10",
    "fg3m_ma_5", "fg3m_ma_10",
    "blk_ma_5", "blk_ma_10",
    "stl_ma_5", "stl_ma_10",
    "min_ma_5", "min_ma_10",
    "fga_ma_5", "fga_ma_10",
    "is_home", "days_rest",
    "team_pace", "team_off_rating", "team_def_rating",
    "opp_pace", "opp_off_rating", "opp_def_rating",
    "pace_diff", "def_diff",
]

# Extra "matchup interaction" features (helps reduce dumb high P spikes)
INTERACTION_FEATURES = [
    "usage_proxy",                # fga_ma_10 / min_ma_10 (simple usage proxy)
    "pace_x_min",                 # pace_diff * min_ma_10
    "def_x_usage",                # def_diff * usage_proxy
    "opp_def_x_fga",              # opp_def_rating * fga_ma_10
    "opp_pace_x_min",             # opp_pace * min_ma_10
    "pts_per_min_10",             # recent scoring efficiency per minute
    "pts_per_fga_10",             # recent scoring efficiency per shot
    "proj_min",                   # blended minutes projection
    "proj_pts_from_min",          # efficiency * projected minutes
]

INJURY_FEATURES = [
    "teammate_out_count",         # missing rotation players (proxy)
    "starter_out_count",          # missing top-5 players (proxy)
    "player_status_q",            # 1 if questionable
    "player_status_d",            # 1 if doubtful
    "player_status_o",            # 1 if out
]

DEFENDER_FEATURES = [
    "opp_pos_def_fg_pct",         # opponent position defender FG% allowed (lower is tougher)
]

MATCHUP_FEATURES = [
    "off_matchup_pts_per_min",
    "off_matchup_fga_per_min",
    "off_matchup_fg_pct",
    "off_matchup_fg3_pct",
]

DEF_MATCHUP_FEATURES = [
    "def_matchup_fg_pct_allowed",
    "def_matchup_pts_per_min_allowed",
]

ETL_STAT_COLS = [
    "etl_ts_pct",
    "etl_efg_pct",
    "etl_usg_pct",
    "etl_tov_pct",
    "etl_possessions",
    "etl_pace",
]

ETL_FEATURES = [f"{c}_ma_5" for c in ETL_STAT_COLS] + [f"{c}_ma_10" for c in ETL_STAT_COLS]

ZONE_FEATURES = [
    "opp_zone_rim_fg_pct",
    "opp_zone_paint_fg_pct",
    "opp_zone_mid_fg_pct",
    "opp_zone_three_fg_pct",
]

RAPM_FEATURES = [
    "rapm",
    "orapm",
    "drapm",
    "rapm_rank",
]

BR_PLAYER_FEATURES = [
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
]

BR_TEAM_FEATURES = [
    "br_team_pts_pg", "br_team_fg_pct", "br_team_3p_pct", "br_team_ft_pct",
    "br_team_trb_pg", "br_team_ast_pg", "br_team_tov_pg",
    "br_team_pts_100", "br_team_fg_pct_100", "br_team_3p_pct_100",
    "br_team_ft_pct_100", "br_team_trb_100", "br_team_ast_100", "br_team_tov_100",
]

BR_OPP_FEATURES = [
    "br_opp_pts_pg", "br_opp_fg_pct", "br_opp_3p_pct", "br_opp_ft_pct",
    "br_opp_trb_pg", "br_opp_ast_pg", "br_opp_tov_pg",
    "br_opp_pts_100", "br_opp_fg_pct_100", "br_opp_3p_pct_100",
    "br_opp_ft_pct_100", "br_opp_trb_100", "br_opp_ast_100", "br_opp_tov_100",
]

ALL_FEATURES = (
    BASE_FEATURES
    + INTERACTION_FEATURES
    + INJURY_FEATURES
    + DEFENDER_FEATURES
    + MATCHUP_FEATURES
    + DEF_MATCHUP_FEATURES
    + ETL_FEATURES
    + ZONE_FEATURES
    + RAPM_FEATURES
    + BR_PLAYER_FEATURES
    + BR_TEAM_FEATURES
    + BR_OPP_FEATURES
)

EXTRA_ROLL_FEATURES = [
    "pra_ma_5", "pra_ma_10",
    "pr_ma_5", "pr_ma_10",
    "pa_ma_5", "pa_ma_10",
    "ra_ma_5", "ra_ma_10",
    "stocks_ma_5", "stocks_ma_10",
]

RAW_STAT_COLS = ["PTS", "REB", "AST", "FG3M", "BLK", "STL", "TOV", "MIN", "FGA"]


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


def normalize_br_season(value) -> str | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        end = int(s)
        start = end - 1
        return f"{start}-{str(end)[-2:]}"
    if "-" in s and len(s) >= 5:
        return s
    return s


def normalize_player_name(name: str) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return ""
    s = s.replace(".", "").replace("'", "").replace("-", " ")
    s = " ".join(s.split())
    return s


def load_injuries_history(paths: list[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        inj = pd.read_csv(path)
        if len(inj) > 0:
            frames.append(inj)
    if not frames:
        return pd.DataFrame(
            columns=["player_name", "team_abbr", "season", "GAME_DATE",
                     "player_status_q", "player_status_d", "player_status_o"]
        )
    inj = pd.concat(frames, ignore_index=True)
    if len(inj) == 0:
        return pd.DataFrame(
            columns=["player_name", "team_abbr", "season", "GAME_DATE",
                     "player_status_q", "player_status_d", "player_status_o"]
        )

    for c in ["player_name", "team_abbr", "status", "game_date", "season"]:
        if c not in inj.columns:
            inj[c] = None

    inj["player_name"] = inj["player_name"].astype(str).str.strip()
    inj["team_abbr"] = inj["team_abbr"].astype(str).str.strip().str.upper()
    inj["status"] = inj["status"].apply(normalize_status)
    inj["GAME_DATE"] = pd.to_datetime(inj["game_date"], errors="coerce").dt.date.astype(str)

    if inj["season"].isna().any():
        inj["season"] = inj["season"].where(inj["season"].notna(), inj["game_date"].apply(infer_season_from_date))

    status_rank = {"OUT": 3, "D": 2, "Q": 1, "P": 0}
    inj["status_rank"] = inj["status"].map(status_rank).fillna(0).astype(int)

    inj = (
        inj.sort_values("status_rank")
           .groupby(["player_name", "team_abbr", "season", "GAME_DATE"], as_index=False)
           .tail(1)
    )

    inj["player_status_q"] = (inj["status"] == "Q").astype(float)
    inj["player_status_d"] = (inj["status"] == "D").astype(float)
    inj["player_status_o"] = (inj["status"] == "OUT").astype(float)

    return inj[["player_name", "team_abbr", "season", "GAME_DATE",
                "player_status_q", "player_status_d", "player_status_o"]]


def normalize_position(pos: str) -> str | None:
    p = str(pos or "").strip().upper()
    if not p:
        return None
    if "-" in p:
        p = p.split("-", 1)[0].strip()
    if "/" in p:
        p = p.split("/", 1)[0].strip()
    if p.startswith("G"):
        return "G"
    if p.startswith("F"):
        return "F"
    if p.startswith("C"):
        return "C"
    return None


def parse_min_to_float(value: str | float | int) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            try:
                return float(parts[0]) + float(parts[1]) / 60.0
            except Exception:
                return None
    try:
        return float(s)
    except Exception:
        return None


def aggregate_weighted(
    df: pd.DataFrame,
    key_cols: list[str],
    weight_col: str,
    col_map: dict[str, str],
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=key_cols + list(col_map.values()))
    df = df.copy()
    if weight_col not in df.columns:
        df[weight_col] = 1.0
    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce").fillna(0.0)
    for src in col_map:
        if src not in df.columns:
            df[src] = np.nan
        df[src] = pd.to_numeric(df[src], errors="coerce")

    def wavg(g: pd.DataFrame) -> pd.Series:
        w = g[weight_col].values
        out = {}
        wsum = float(np.nansum(w))
        for src, dst in col_map.items():
            vals = g[src].values.astype(float)
            if wsum > 0:
                out[dst] = float(np.nansum(vals * w) / wsum)
            else:
                out[dst] = float(np.nanmean(vals)) if np.isfinite(vals).any() else np.nan
        return pd.Series(out)

    grouped = df.groupby(key_cols, as_index=False).apply(wavg).reset_index(drop=True)
    return grouped[key_cols + list(col_map.values())]


def load_br_player_stats() -> pd.DataFrame:
    frames = []

    def load_file(path: Path, weight_col: str, col_map: dict[str, str]) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=["player_name_norm", "season"] + list(col_map.values()))
        df = pd.read_csv(path)
        if len(df) == 0:
            return pd.DataFrame(columns=["player_name_norm", "season"] + list(col_map.values()))
        for c in ["player", "season"]:
            if c not in df.columns:
                df[c] = None
        df["season"] = df["season"].apply(normalize_br_season)
        df["player_name_norm"] = df["player"].apply(normalize_player_name)
        df = df[df["player_name_norm"] != ""].copy()
        return aggregate_weighted(df, ["player_name_norm", "season"], weight_col, col_map)

    per_game_cols = {
        "mp_per_game": "br_pg_min",
        "pts_per_game": "br_pg_pts",
        "fga_per_game": "br_pg_fga",
        "x3pa_per_game": "br_pg_3pa",
        "fta_per_game": "br_pg_fta",
        "fg_percent": "br_pg_fg_pct",
        "x3p_percent": "br_pg_3p_pct",
        "ft_percent": "br_pg_ft_pct",
        "e_fg_percent": "br_pg_efg_pct",
        "trb_per_game": "br_pg_reb",
        "ast_per_game": "br_pg_ast",
        "stl_per_game": "br_pg_stl",
        "blk_per_game": "br_pg_blk",
        "tov_per_game": "br_pg_tov",
    }
    advanced_cols = {
        "per": "br_adv_per",
        "ts_percent": "br_adv_ts_pct",
        "x3p_ar": "br_adv_3par",
        "f_tr": "br_adv_ftr",
        "orb_percent": "br_adv_orb_pct",
        "drb_percent": "br_adv_drb_pct",
        "trb_percent": "br_adv_trb_pct",
        "ast_percent": "br_adv_ast_pct",
        "stl_percent": "br_adv_stl_pct",
        "blk_percent": "br_adv_blk_pct",
        "tov_percent": "br_adv_tov_pct",
        "usg_percent": "br_adv_usg_pct",
        "bpm": "br_adv_bpm",
        "obpm": "br_adv_obpm",
        "dbpm": "br_adv_dbpm",
        "vorp": "br_adv_vorp",
    }
    per36_cols = {
        "pts_per_36_min": "br_p36_pts",
        "fga_per_36_min": "br_p36_fga",
        "x3pa_per_36_min": "br_p36_3pa",
        "fta_per_36_min": "br_p36_fta",
        "trb_per_36_min": "br_p36_trb",
        "ast_per_36_min": "br_p36_ast",
        "stl_per_36_min": "br_p36_stl",
        "blk_per_36_min": "br_p36_blk",
        "tov_per_36_min": "br_p36_tov",
    }
    per100_cols = {
        "pts_per_100_poss": "br_p100_pts",
        "fga_per_100_poss": "br_p100_fga",
        "x3pa_per_100_poss": "br_p100_3pa",
        "fta_per_100_poss": "br_p100_fta",
        "trb_per_100_poss": "br_p100_trb",
        "ast_per_100_poss": "br_p100_ast",
        "stl_per_100_poss": "br_p100_stl",
        "blk_per_100_poss": "br_p100_blk",
        "tov_per_100_poss": "br_p100_tov",
        "o_rtg": "br_p100_ortg",
        "d_rtg": "br_p100_drtg",
    }
    shooting_cols = {
        "avg_dist_fga": "br_shot_avg_dist",
        "percent_fga_from_x2p_range": "br_shot_pct_2p",
        "percent_fga_from_x0_3_range": "br_shot_pct_0_3",
        "percent_fga_from_x3_10_range": "br_shot_pct_3_10",
        "percent_fga_from_x10_16_range": "br_shot_pct_10_16",
        "percent_fga_from_x16_3p_range": "br_shot_pct_16_3p",
        "percent_fga_from_x3p_range": "br_shot_pct_3p",
        "fg_percent_from_x2p_range": "br_shot_fg_2p",
        "fg_percent_from_x0_3_range": "br_shot_fg_0_3",
        "fg_percent_from_x3_10_range": "br_shot_fg_3_10",
        "fg_percent_from_x10_16_range": "br_shot_fg_10_16",
        "fg_percent_from_x16_3p_range": "br_shot_fg_16_3p",
        "fg_percent_from_x3p_range": "br_shot_fg_3p",
        "percent_assisted_x2p_fg": "br_shot_ast_2p",
        "percent_assisted_x3p_fg": "br_shot_ast_3p",
        "percent_dunks_of_fga": "br_shot_dunks_pct",
        "percent_corner_3s_of_3pa": "br_shot_corner3_share",
        "corner_3_point_percent": "br_shot_corner3_pct",
    }
    pbp_cols = {
        "pg_percent": "br_pbp_pg_pct",
        "sg_percent": "br_pbp_sg_pct",
        "sf_percent": "br_pbp_sf_pct",
        "pf_percent": "br_pbp_pf_pct",
        "c_percent": "br_pbp_c_pct",
        "on_court_plus_minus_per_100_poss": "br_pbp_on_off_100",
        "net_plus_minus_per_100_poss": "br_pbp_net_100",
        "bad_pass_turnover": "br_pbp_bad_pass_tov",
        "lost_ball_turnover": "br_pbp_lost_ball_tov",
        "shooting_foul_drawn": "br_pbp_shoot_foul_drawn",
        "offensive_foul_drawn": "br_pbp_off_foul_drawn",
        "points_generated_by_assists": "br_pbp_pts_ast",
        "fga_blocked": "br_pbp_fga_blocked",
    }

    frames.append(load_file(BR_PLAYER_PER_GAME_CSV, "g", per_game_cols))
    frames.append(load_file(BR_ADVANCED_CSV, "mp", advanced_cols))
    frames.append(load_file(BR_PER36_CSV, "g", per36_cols))
    frames.append(load_file(BR_PER100_CSV, "g", per100_cols))
    frames.append(load_file(BR_SHOOTING_CSV, "g", shooting_cols))
    frames.append(load_file(BR_PLAYBYPLAY_CSV, "g", pbp_cols))

    merged = None
    for frame in frames:
        if len(frame) == 0:
            continue
        merged = frame if merged is None else merged.merge(frame, how="outer", on=["player_name_norm", "season"])

    if merged is None:
        return pd.DataFrame(columns=["player_name_norm", "season"] + BR_PLAYER_FEATURES)

    for col in BR_PLAYER_FEATURES:
        if col not in merged.columns:
            merged[col] = np.nan

    return merged[["player_name_norm", "season"] + BR_PLAYER_FEATURES]


def load_br_team_stats() -> tuple[pd.DataFrame, pd.DataFrame]:
    team_frames = []
    opp_frames = []

    def load_team(path: Path, col_map: dict[str, str]) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=["season", "team_abbr"] + list(col_map.values()))
        df = pd.read_csv(path)
        if len(df) == 0:
            return pd.DataFrame(columns=["season", "team_abbr"] + list(col_map.values()))
        for c in ["season", "abbreviation"]:
            if c not in df.columns:
                df[c] = None
        df["season"] = df["season"].apply(normalize_br_season)
        df["team_abbr"] = df["abbreviation"].astype(str).str.strip().str.upper()
        for src in col_map:
            if src not in df.columns:
                df[src] = np.nan
            df[src] = pd.to_numeric(df[src], errors="coerce")
        out = df[["season", "team_abbr"] + list(col_map.keys())].rename(columns=col_map)
        return out

    team_pg_cols = {
        "pts_per_game": "br_team_pts_pg",
        "fg_percent": "br_team_fg_pct",
        "x3p_percent": "br_team_3p_pct",
        "ft_percent": "br_team_ft_pct",
        "trb_per_game": "br_team_trb_pg",
        "ast_per_game": "br_team_ast_pg",
        "tov_per_game": "br_team_tov_pg",
    }
    team_100_cols = {
        "pts_per_100_poss": "br_team_pts_100",
        "fg_percent": "br_team_fg_pct_100",
        "x3p_percent": "br_team_3p_pct_100",
        "ft_percent": "br_team_ft_pct_100",
        "trb_per_100_poss": "br_team_trb_100",
        "ast_per_100_poss": "br_team_ast_100",
        "tov_per_100_poss": "br_team_tov_100",
    }
    opp_pg_cols = {
        "opp_pts_per_game": "br_opp_pts_pg",
        "opp_fg_percent": "br_opp_fg_pct",
        "opp_x3p_percent": "br_opp_3p_pct",
        "opp_ft_percent": "br_opp_ft_pct",
        "opp_trb_per_game": "br_opp_trb_pg",
        "opp_ast_per_game": "br_opp_ast_pg",
        "opp_tov_per_game": "br_opp_tov_pg",
    }
    opp_100_cols = {
        "opp_pts_per_100_poss": "br_opp_pts_100",
        "opp_fg_percent": "br_opp_fg_pct_100",
        "opp_x3p_percent": "br_opp_3p_pct_100",
        "opp_ft_percent": "br_opp_ft_pct_100",
        "opp_trb_per_100_poss": "br_opp_trb_100",
        "opp_ast_per_100_poss": "br_opp_ast_100",
        "opp_tov_per_100_poss": "br_opp_tov_100",
    }

    team_frames.append(load_team(BR_TEAM_PER_GAME_CSV, team_pg_cols))
    team_frames.append(load_team(BR_TEAM_PER100_CSV, team_100_cols))
    opp_frames.append(load_team(BR_OPP_PER_GAME_CSV, opp_pg_cols))
    opp_frames.append(load_team(BR_OPP_PER100_CSV, opp_100_cols))

    team_df = None
    for frame in team_frames:
        if len(frame) == 0:
            continue
        team_df = frame if team_df is None else team_df.merge(frame, how="outer", on=["season", "team_abbr"])

    opp_df = None
    for frame in opp_frames:
        if len(frame) == 0:
            continue
        opp_df = frame if opp_df is None else opp_df.merge(frame, how="outer", on=["season", "team_abbr"])

    if team_df is None:
        team_df = pd.DataFrame(columns=["season", "team_abbr"] + BR_TEAM_FEATURES)
    if opp_df is None:
        opp_df = pd.DataFrame(columns=["season", "team_abbr"] + BR_OPP_FEATURES)

    for col in BR_TEAM_FEATURES:
        if col not in team_df.columns:
            team_df[col] = np.nan
    for col in BR_OPP_FEATURES:
        if col not in opp_df.columns:
            opp_df[col] = np.nan

    return (
        team_df[["season", "team_abbr"] + BR_TEAM_FEATURES],
        opp_df[["season", "team_abbr"] + BR_OPP_FEATURES],
    )


def build_player_team_map(df: pd.DataFrame) -> pd.DataFrame:
    if "team_abbr" not in df.columns:
        return pd.DataFrame(columns=["season", "player_name", "team_abbr"])
    counts = (
        df.groupby(["season", "player_name", "team_abbr"], as_index=False)
          .size()
          .rename(columns={"size": "games"})
    )
    if len(counts) == 0:
        return pd.DataFrame(columns=["season", "player_name", "team_abbr"])
    top = (
        counts.sort_values(["season", "player_name", "games"], ascending=[True, True, False])
              .groupby(["season", "player_name"], as_index=False)
              .head(1)
    )
    return top[["season", "player_name", "team_abbr"]]


def load_defense_matchups(path: Path, team_map: pd.DataFrame) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed"])
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed"])

    for c in ["SEASON", "POSITION", "DEF_PLAYER_NAME", "MATCHUP_MIN", "PLAYER_PTS", "MATCHUP_FG_PCT"]:
        if c not in df.columns:
            df[c] = None

    df["season"] = df["SEASON"].astype(str)
    df["player_name"] = df["DEF_PLAYER_NAME"].astype(str).str.strip()
    df["pos"] = df["POSITION"].apply(normalize_position)
    df["matchup_min"] = df["MATCHUP_MIN"].apply(parse_min_to_float)
    df["player_pts"] = pd.to_numeric(df["PLAYER_PTS"], errors="coerce")
    df["matchup_fg_pct"] = pd.to_numeric(df["MATCHUP_FG_PCT"], errors="coerce")

    df = df.dropna(subset=["season", "player_name", "pos", "matchup_min", "player_pts"]).reset_index(drop=True)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed"])

    df = df.merge(team_map, how="left", on=["season", "player_name"])
    df = df.dropna(subset=["team_abbr"])
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed"])

    df["def_matchup_pts_per_min_allowed"] = df["player_pts"] / df["matchup_min"].replace(0, np.nan)
    df["weight"] = df["matchup_min"].where(df["matchup_min"] > 0, 1.0)

    grouped = (
        df.groupby(["season", "team_abbr", "pos"], as_index=False)
          .apply(
              lambda g: pd.Series(
                  {
                      "def_matchup_fg_pct_allowed": np.average(g["matchup_fg_pct"], weights=g["weight"]),
                      "def_matchup_pts_per_min_allowed": np.average(g["def_matchup_pts_per_min_allowed"], weights=g["weight"]),
                  }
              )
          )
    )
    return grouped[["season", "team_abbr", "pos", "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed"]]


def load_offense_matchups(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=[
            "season",
            "player_name",
            "off_matchup_pts_per_min",
            "off_matchup_fga_per_min",
            "off_matchup_fg_pct",
            "off_matchup_fg3_pct",
        ])
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "season",
            "player_name",
            "off_matchup_pts_per_min",
            "off_matchup_fga_per_min",
            "off_matchup_fg_pct",
            "off_matchup_fg3_pct",
        ])

    for c in ["SEASON", "POSITION", "OFF_PLAYER_NAME", "MATCHUP_MIN", "PLAYER_PTS", "MATCHUP_FGA", "MATCHUP_FG_PCT", "MATCHUP_FG3_PCT"]:
        if c not in df.columns:
            df[c] = None

    df = df[df["POSITION"].astype(str).str.upper() == "TOTAL"].copy()
    df["season"] = df["SEASON"].astype(str)
    df["player_name"] = df["OFF_PLAYER_NAME"].astype(str).str.strip()
    df["matchup_min"] = df["MATCHUP_MIN"].apply(parse_min_to_float)
    df["player_pts"] = pd.to_numeric(df["PLAYER_PTS"], errors="coerce")
    df["matchup_fga"] = pd.to_numeric(df["MATCHUP_FGA"], errors="coerce")
    df["matchup_fg_pct"] = pd.to_numeric(df["MATCHUP_FG_PCT"], errors="coerce")
    df["matchup_fg3_pct"] = pd.to_numeric(df["MATCHUP_FG3_PCT"], errors="coerce")

    df = df.dropna(subset=["season", "player_name", "matchup_min", "player_pts"]).reset_index(drop=True)
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "season",
            "player_name",
            "off_matchup_pts_per_min",
            "off_matchup_fga_per_min",
            "off_matchup_fg_pct",
            "off_matchup_fg3_pct",
        ])

    df["off_matchup_pts_per_min"] = df["player_pts"] / df["matchup_min"].replace(0, np.nan)
    df["off_matchup_fga_per_min"] = df["matchup_fga"] / df["matchup_min"].replace(0, np.nan)
    df["off_matchup_fg_pct"] = df["matchup_fg_pct"]
    df["off_matchup_fg3_pct"] = df["matchup_fg3_pct"]

    out = df[["season", "player_name", "off_matchup_pts_per_min",
              "off_matchup_fga_per_min", "off_matchup_fg_pct", "off_matchup_fg3_pct"]]
    return out.drop_duplicates(subset=["season", "player_name"])


def load_defender_ratings(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "pos_def_fg_pct"])

    for c in ["season", "team_abbr", "player_name", "player_position", "def_fg_pct", "def_fga"]:
        if c not in df.columns:
            df[c] = None

    df["season"] = df["season"].astype(str)
    df["team_abbr"] = df["team_abbr"].astype(str).str.strip().str.upper()
    df["player_name"] = df["player_name"].astype(str).str.strip()
    df["pos"] = df["player_position"].apply(normalize_position)
    df["def_fg_pct"] = pd.to_numeric(df["def_fg_pct"], errors="coerce")
    df["def_fga"] = pd.to_numeric(df["def_fga"], errors="coerce").fillna(0.0)

    df = df.dropna(subset=["season", "team_abbr", "player_name", "pos", "def_fg_pct"]).reset_index(drop=True)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr", "pos", "pos_def_fg_pct"])

    df["weight"] = df["def_fga"].where(df["def_fga"] > 0, 1.0)
    grouped = (
        df.groupby(["season", "team_abbr", "pos"], as_index=False)
          .apply(lambda g: np.average(g["def_fg_pct"], weights=g["weight"]))
          .rename(columns={0: "pos_def_fg_pct"})
    )

    if "pos_def_fg_pct" not in grouped.columns:
        grouped = grouped.rename(columns={grouped.columns[-1]: "pos_def_fg_pct"})

    return grouped[["season", "team_abbr", "pos", "pos_def_fg_pct"]]


def load_player_positions(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "player_name", "pos"])
    for c in ["season", "player_name", "player_position"]:
        if c not in df.columns:
            df[c] = None
    df["season"] = df["season"].astype(str)
    df["player_name"] = df["player_name"].astype(str).str.strip()
    df["pos"] = df["player_position"].apply(normalize_position)
    df = df.dropna(subset=["season", "player_name", "pos"]).reset_index(drop=True)
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "player_name", "pos"])
    return df[["season", "player_name", "pos"]].drop_duplicates()


def parse_matchup_to_team_opp(matchup: str):
    """
    MATCHUP examples:
      "DEN vs HOU" (home)
      "DEN @ HOU"  (away)
    Returns (team_abbr, opp_abbr, is_home)
    """
    if not isinstance(matchup, str) or len(matchup) < 5:
        return None, None, None

    if " vs " in matchup:
        left, right = matchup.split(" vs ")
        return left.strip(), right.strip(), 1.0
    if " @ " in matchup:
        left, right = matchup.split(" @ ")
        return left.strip(), right.strip(), 0.0

    return None, None, None


def add_roll(group: pd.DataFrame) -> pd.DataFrame:
    # group sorted by date
    g = group.sort_values("GAME_DATE").copy()
    # pandas groupby.apply may drop grouping columns; restore from group name when missing
    if "player_name" not in g.columns or "season" not in g.columns:
        group_key = getattr(group, "name", None)
        if isinstance(group_key, tuple) and len(group_key) >= 2:
            g["player_name"] = group_key[0]
            g["season"] = group_key[1]

    # rolling means with shift so today's row doesn't leak today's points
    for col, out5, out10 in [
        ("PTS", "pts_ma_5", "pts_ma_10"),
        ("REB", "reb_ma_5", "reb_ma_10"),
        ("AST", "ast_ma_5", "ast_ma_10"),
        ("FG3M", "fg3m_ma_5", "fg3m_ma_10"),
        ("BLK", "blk_ma_5", "blk_ma_10"),
        ("STL", "stl_ma_5", "stl_ma_10"),
        ("TOV", "tov_ma_5", "tov_ma_10"),
        ("MIN", "min_ma_5", "min_ma_10"),
        ("FGA", "fga_ma_5", "fga_ma_10"),
    ]:
        s = g[col].astype(float)
        g[out5] = s.shift(1).rolling(5, min_periods=3).mean()
        g[out10] = s.shift(1).rolling(10, min_periods=5).mean()

    # ETL advanced metrics rolling (if available)
    for col in ETL_STAT_COLS:
        if col in g.columns:
            s = g[col].astype(float)
            g[f"{col}_ma_5"] = s.shift(1).rolling(5, min_periods=3).mean()
            g[f"{col}_ma_10"] = s.shift(1).rolling(10, min_periods=5).mean()

    # days_rest (difference between games)
    dates = pd.to_datetime(g["GAME_DATE"], errors="coerce", format="mixed")
    g = g[dates.notna()].copy()
    g["days_rest"] = dates.diff().dt.days
    g["days_rest"] = g["days_rest"].clip(lower=0).fillna(2)

    return g


def join_team_context(df: pd.DataFrame, team_ctx: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      team_pace, team_off_rating, team_def_rating
      opp_pace, opp_off_rating, opp_def_rating
    """
    tc = team_ctx.copy()
    tc["season"] = tc["season"].astype(str)
    tc["team_abbr"] = tc["team_abbr"].astype(str)

    # join team
    df = df.merge(
        tc[["season", "team_abbr", "pace", "off_rating", "def_rating"]],
        how="left",
        left_on=["season", "team_abbr"],
        right_on=["season", "team_abbr"],
    ).rename(
        columns={"pace": "team_pace", "off_rating": "team_off_rating", "def_rating": "team_def_rating"}
    )

    # join opponent
    df = df.merge(
        tc[["season", "team_abbr", "pace", "off_rating", "def_rating"]],
        how="left",
        left_on=["season", "opp_abbr"],
        right_on=["season", "team_abbr"],
        suffixes=("", "_oppjoin"),
    ).rename(
        columns={"pace": "opp_pace", "off_rating": "opp_off_rating", "def_rating": "opp_def_rating"}
    )

    # clean extra columns
    if "team_abbr_oppjoin" in df.columns:
        df = df.drop(columns=["team_abbr_oppjoin"])

    return df


def load_zone_team_stats() -> pd.DataFrame:
    files = sorted(DATA_DIR.glob("zone_stats_team_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return pd.DataFrame(columns=["season", "team_abbr"] + ZONE_FEATURES)

    data = json.loads(files[0].read_text(encoding="utf-8"))
    meta = data.get("metadata", {}) or {}
    season = meta.get("Season")
    if not season:
        name = files[0].stem
        digits = "".join(ch for ch in name if ch.isdigit())
        if len(digits) >= 6:
            season = f"{digits[:4]}-{digits[4:6]}"
    if not season:
        season = ""

    tables = data.get("tables") or []
    target = None
    for t in tables:
        cols = t.get("columns") or []
        if "TEAM_ABBREVIATION" in cols and "SHOT_ZONE_BASIC" in cols:
            target = t
            break
    if not target:
        return pd.DataFrame(columns=["season", "team_abbr"] + ZONE_FEATURES)

    cols = target["columns"]
    rows = target["rows"]
    df = pd.DataFrame(rows, columns=cols)

    for c in ["TEAM_ABBREVIATION", "SHOT_ZONE_BASIC", "FGM", "FGA", "FG_PCT"]:
        if c not in df.columns:
            df[c] = np.nan

    df["TEAM_ABBREVIATION"] = df["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
    df["SHOT_ZONE_BASIC"] = df["SHOT_ZONE_BASIC"].astype(str).str.strip()
    df["FGM"] = pd.to_numeric(df["FGM"], errors="coerce")
    df["FGA"] = pd.to_numeric(df["FGA"], errors="coerce")
    df["FG_PCT"] = pd.to_numeric(df["FG_PCT"], errors="coerce")

    def zone_bucket(z: str) -> str | None:
        if z == "Restricted Area":
            return "rim"
        if z == "In The Paint (Non-RA)":
            return "paint"
        if z == "Mid-Range":
            return "mid"
        if z in ("Corner 3", "Left Corner 3", "Right Corner 3", "Above the Break 3"):
            return "three"
        return None

    df["zone"] = df["SHOT_ZONE_BASIC"].apply(zone_bucket)
    df = df[df["zone"].notna()].copy()
    if len(df) == 0:
        return pd.DataFrame(columns=["season", "team_abbr"] + ZONE_FEATURES)

    df["fg_pct"] = np.where(
        df["FGA"] > 0,
        (df["FGM"] / df["FGA"]) * 100.0,
        df["FG_PCT"],
    )
    df.loc[df["fg_pct"] <= 1, "fg_pct"] = df.loc[df["fg_pct"] <= 1, "fg_pct"] * 100.0

    grouped = (
        df.groupby(["TEAM_ABBREVIATION", "zone"], as_index=False)
          .apply(
              lambda g: pd.Series(
                  {
                      "fgm": float(g["FGM"].sum(skipna=True)),
                      "fga": float(g["FGA"].sum(skipna=True)),
                      "fg_pct": float(np.average(g["fg_pct"], weights=g["FGA"].fillna(1.0))),
                  }
              )
          )
    )

    pivot = grouped.pivot(index="TEAM_ABBREVIATION", columns="zone", values="fg_pct").reset_index()
    pivot.columns.name = None
    pivot = pivot.rename(
        columns={
            "TEAM_ABBREVIATION": "team_abbr",
            "rim": "opp_zone_rim_fg_pct",
            "paint": "opp_zone_paint_fg_pct",
            "mid": "opp_zone_mid_fg_pct",
            "three": "opp_zone_three_fg_pct",
        }
    )
    pivot["season"] = str(season)
    for c in ZONE_FEATURES:
        if c not in pivot.columns:
            pivot[c] = np.nan
    return pivot[["season", "team_abbr"] + ZONE_FEATURES]


def join_lineup_context(df: pd.DataFrame, lineup_ctx: pd.DataFrame) -> pd.DataFrame:
    lc = lineup_ctx.copy()
    lc["season"] = lc["season"].astype(str)
    lc["team_abbr"] = lc["team_abbr"].astype(str)
    lc["GAME_DATE"] = pd.to_datetime(lc["GAME_DATE"]).dt.date.astype(str)

    keep = ["season", "GAME_DATE", "team_abbr", "teammate_out_count", "starter_out_count"]
    lc = lc[keep]

    df = df.merge(
        lc,
        how="left",
        left_on=["season", "GAME_DATE", "team_abbr"],
        right_on=["season", "GAME_DATE", "team_abbr"],
    )
    return df


def load_rapm_stats() -> pd.DataFrame:
    rapm_path = DATA_DIR / "rapm_gameflow.csv"
    if not rapm_path.exists():
        return pd.DataFrame(columns=["player_name", "season"] + RAPM_FEATURES)
    df = pd.read_csv(rapm_path)
    if len(df) == 0:
        return pd.DataFrame(columns=["player_name", "season"] + RAPM_FEATURES)
    for c in ["player_name", "season"]:
        if c not in df.columns:
            df[c] = None
    df["player_name"] = df["player_name"].astype(str).str.strip()
    df["season"] = df["season"].astype(str)
    for c in RAPM_FEATURES:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["player_name", "season"] + RAPM_FEATURES].drop_duplicates()


def build_interactions(df: pd.DataFrame) -> pd.DataFrame:
    # usage proxy: shots per minute (smoothed)
    eps = 1e-6
    df["usage_proxy"] = df["fga_ma_10"] / (df["min_ma_10"] + eps)
    df["pts_per_min_10"] = df["pts_ma_10"] / (df["min_ma_10"] + eps)
    df["pts_per_fga_10"] = df["pts_ma_10"] / (df["fga_ma_10"] + eps)
    df["proj_min"] = (0.6 * df["min_ma_5"]) + (0.4 * df["min_ma_10"])
    df["proj_pts_from_min"] = df["pts_per_min_10"] * df["proj_min"]

    # matchup diffs
    df["pace_diff"] = df["team_pace"] - df["opp_pace"]
    df["def_diff"] = df["opp_def_rating"] - df["team_off_rating"]  # higher = tougher relative

    # interactions (simple but effective)
    df["pace_x_min"] = df["pace_diff"] * df["min_ma_10"]
    df["def_x_usage"] = df["def_diff"] * df["usage_proxy"]
    df["opp_def_x_fga"] = df["opp_def_rating"] * df["fga_ma_10"]
    df["opp_pace_x_min"] = df["opp_pace"] * df["min_ma_10"]

    # Clip extreme values to reduce outlier-driven predictions
    df["usage_proxy"] = df["usage_proxy"].clip(lower=0.05, upper=1.2)
    df["pts_per_min_10"] = df["pts_per_min_10"].clip(lower=0.1, upper=1.5)
    df["pts_per_fga_10"] = df["pts_per_fga_10"].clip(lower=0.2, upper=2.5)
    df["proj_min"] = df["proj_min"].clip(lower=5, upper=42)
    df["proj_pts_from_min"] = df["proj_pts_from_min"].clip(lower=2, upper=45)
    df["pace_diff"] = df["pace_diff"].clip(lower=-10, upper=10)
    df["def_diff"] = df["def_diff"].clip(lower=-15, upper=15)
    df["team_pace"] = df["team_pace"].clip(lower=90, upper=125)
    df["opp_pace"] = df["opp_pace"].clip(lower=90, upper=125)
    df["team_off_rating"] = df["team_off_rating"].clip(lower=90, upper=125)
    df["opp_def_rating"] = df["opp_def_rating"].clip(lower=90, upper=125)

    return df


def main():
    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs csv: {LOGS_CSV}")

    df = pd.read_csv(LOGS_CSV)

    # Required columns we expect from your pull
    required = ["player_name", "season", "GAME_DATE", "MATCHUP", "MIN", "PTS", "FGA", "REB", "AST", "FG3M", "BLK", "STL", "TOV"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    df["season"] = df["season"].astype(str)
    game_dates = pd.to_datetime(df["GAME_DATE"], errors="coerce", format="mixed")
    df = df[game_dates.notna()].copy()
    df["GAME_DATE"] = game_dates.dt.date.astype(str)

    # Merge ETL advanced metrics (per-game) if available
    if ETL_ADV_CSV.exists():
        etl = pd.read_csv(ETL_ADV_CSV)
        for c in ["player_name", "season", "GAME_DATE"]:
            if c not in etl.columns:
                etl[c] = None
        for c in ETL_STAT_COLS:
            if c not in etl.columns:
                etl[c] = np.nan
        etl["player_name"] = etl["player_name"].astype(str).str.strip()
        etl["season"] = etl["season"].astype(str)
        etl["GAME_DATE"] = pd.to_datetime(etl["GAME_DATE"], errors="coerce").dt.date.astype(str)
        df = df.merge(etl, how="left", on=["player_name", "season", "GAME_DATE"])
    else:
        for col in ETL_STAT_COLS:
            df[col] = np.nan

    # Parse matchup to team/opponent + home/away
    parsed = df["MATCHUP"].apply(parse_matchup_to_team_opp)
    df["team_abbr"] = parsed.apply(lambda x: x[0])
    df["opp_abbr"] = parsed.apply(lambda x: x[1])
    df["is_home"] = parsed.apply(lambda x: x[2])

    # Drop rows where matchup failed
    df = df.dropna(subset=["team_abbr", "opp_abbr", "is_home"]).reset_index(drop=True)

    # Filter low-minute/DNP rows before rolling to avoid skewed last-10 averages
    df = df[df["MIN"].astype(float) >= 5].reset_index(drop=True)
    df["player_name_norm"] = df["player_name"].apply(normalize_player_name)

    # Add rolling form
    df = df.groupby(["player_name", "season"], group_keys=False).apply(add_roll)
    df["pra_ma_5"] = df["pts_ma_5"] + df["reb_ma_5"] + df["ast_ma_5"]
    df["pra_ma_10"] = df["pts_ma_10"] + df["reb_ma_10"] + df["ast_ma_10"]
    df["pr_ma_5"] = df["pts_ma_5"] + df["reb_ma_5"]
    df["pr_ma_10"] = df["pts_ma_10"] + df["reb_ma_10"]
    df["pa_ma_5"] = df["pts_ma_5"] + df["ast_ma_5"]
    df["pa_ma_10"] = df["pts_ma_10"] + df["ast_ma_10"]
    df["ra_ma_5"] = df["reb_ma_5"] + df["ast_ma_5"]
    df["ra_ma_10"] = df["reb_ma_10"] + df["ast_ma_10"]
    df["stocks_ma_5"] = df["blk_ma_5"] + df["stl_ma_5"]
    df["stocks_ma_10"] = df["blk_ma_10"] + df["stl_ma_10"]

    # Load team context
    if not TEAM_CTX_CSV.exists():
        raise RuntimeError(f"Missing team context file: {TEAM_CTX_CSV}. Run fetch_team_context.py first.")
    team_ctx = pd.read_csv(TEAM_CTX_CSV)

    df = join_team_context(df, team_ctx)

    # Basketball-Reference player season stats (optional; season-level features)
    br_player = load_br_player_stats()
    if len(br_player) > 0:
        df = df.merge(br_player, how="left", on=["player_name_norm", "season"])

    # Optional zone defense stats (NBA zone client output)
    zone_df = load_zone_team_stats()
    if len(zone_df) > 0:
        df = df.merge(
            zone_df,
            how="left",
            left_on=["season", "opp_abbr"],
            right_on=["season", "team_abbr"],
        )
        if "team_abbr_x" in df.columns:
            df = df.rename(columns={"team_abbr_x": "team_abbr"})
        if "team_abbr_y" in df.columns:
            df = df.drop(columns=["team_abbr_y"])

    # Load lineup context (injury proxies)
    if not LINEUP_CTX_CSV.exists():
        raise RuntimeError(f"Missing lineup context file: {LINEUP_CTX_CSV}. Run build_team_lineup_context.py first.")
    lineup_ctx = pd.read_csv(LINEUP_CTX_CSV)
    df = join_lineup_context(df, lineup_ctx)

    # Basketball-Reference team/opponent season stats (optional)
    br_team, br_opp = load_br_team_stats()
    if len(br_team) > 0:
        df = df.merge(br_team, how="left", on=["season", "team_abbr"])
    if len(br_opp) > 0:
        df = df.merge(
            br_opp,
            how="left",
            left_on=["season", "opp_abbr"],
            right_on=["season", "team_abbr"],
        )
        if "team_abbr_x" in df.columns:
            df = df.rename(columns={"team_abbr_x": "team_abbr"})
        if "team_abbr_y" in df.columns:
            df = df.drop(columns=["team_abbr_y"])

    # Optional RAPM advanced stats
    rapm_df = load_rapm_stats()
    if len(rapm_df) > 0:
        df = df.merge(rapm_df, how="left", on=["player_name", "season"])

    # Load historical injury statuses when available
    injury_files = sorted(DATA_DIR.glob(INJURIES_HISTORY_GLOB))
    if injury_files:
        inj_hist = load_injuries_history(injury_files)
        if len(inj_hist) > 0:
            df = df.merge(
                inj_hist,
                how="left",
                on=["player_name", "team_abbr", "season", "GAME_DATE"],
            )

    # Load defender data (position-level proxy) when available
    if DEFENDER_CSV.exists():
        defender_pos = load_player_positions(DEFENDER_CSV)
        if len(defender_pos) > 0:
            df = df.merge(defender_pos, how="left", on=["season", "player_name"])

        opp_def = load_defender_ratings(DEFENDER_CSV)
        if len(opp_def) > 0:
            df = df.merge(
                opp_def,
                how="left",
                left_on=["season", "opp_abbr", "pos"],
                right_on=["season", "team_abbr", "pos"],
            )
            if "team_abbr_x" in df.columns:
                df = df.rename(columns={"team_abbr_x": "team_abbr"})
            if "team_abbr_y" in df.columns:
                df = df.drop(columns=["team_abbr_y"])
            if "pos_def_fg_pct" in df.columns:
                df["opp_pos_def_fg_pct"] = df["pos_def_fg_pct"]
                df = df.drop(columns=["pos_def_fg_pct"])

    # Build diffs + interaction features
    df = build_interactions(df)

    # Injury feature defaults (logs only include games played)
    for col in [
        "teammate_out_count",
        "starter_out_count",
        "player_status_q",
        "player_status_d",
        "player_status_o",
    ]:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = df[col].fillna(0.0)

    if "opp_pos_def_fg_pct" not in df.columns:
        df["opp_pos_def_fg_pct"] = np.nan
    else:
        team_avg = (
            df.groupby(["season", "opp_abbr"], as_index=False)["opp_pos_def_fg_pct"]
              .mean()
              .rename(columns={"opp_pos_def_fg_pct": "team_def_fg_pct"})
        )
        df = df.merge(
            team_avg,
            how="left",
            left_on=["season", "opp_abbr"],
            right_on=["season", "opp_abbr"],
        )
        df["opp_pos_def_fg_pct"] = df["opp_pos_def_fg_pct"].fillna(df["team_def_fg_pct"])
        df = df.drop(columns=["team_def_fg_pct"])

    league_avg = float(df["opp_pos_def_fg_pct"].mean()) if df["opp_pos_def_fg_pct"].notna().any() else 0.0
    df["opp_pos_def_fg_pct"] = df["opp_pos_def_fg_pct"].fillna(league_avg)

    # Defensive matchup features (map defenders to teams, then aggregate)
    team_map = build_player_team_map(df)
    def_matchups = load_defense_matchups(MATCHUPS_DEFENSE_CSV, team_map)
    if len(def_matchups) > 0:
        df = df.merge(
            def_matchups,
            how="left",
            left_on=["season", "opp_abbr", "pos"],
            right_on=["season", "team_abbr", "pos"],
        )
        if "team_abbr_x" in df.columns:
            df = df.rename(columns={"team_abbr_x": "team_abbr"})
        if "team_abbr_y" in df.columns:
            df = df.drop(columns=["team_abbr_y"])

    # Load matchup offense features (player-level)
    offense_matchups = load_offense_matchups(MATCHUPS_OFFENSE_CSV)
    if len(offense_matchups) > 0:
        df = df.merge(
            offense_matchups,
            how="left",
            on=["season", "player_name"],
        )

    for col in MATCHUP_FEATURES:
        if col not in df.columns:
            df[col] = np.nan

    matchup_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in MATCHUP_FEATURES}
    for col in MATCHUP_FEATURES:
        df[col] = df[col].fillna(matchup_avg.get(col, 0.0))

    for col in DEF_MATCHUP_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    def_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in DEF_MATCHUP_FEATURES}
    for col in DEF_MATCHUP_FEATURES:
        df[col] = df[col].fillna(def_avg.get(col, 0.0))

    for col in ETL_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    etl_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in ETL_FEATURES}
    for col in ETL_FEATURES:
        df[col] = df[col].fillna(etl_avg.get(col, 0.0))

    for col in ZONE_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    zone_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in ZONE_FEATURES}
    for col in ZONE_FEATURES:
        df[col] = df[col].fillna(zone_avg.get(col, 0.0))

    for col in RAPM_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    rapm_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in RAPM_FEATURES}
    for col in RAPM_FEATURES:
        df[col] = df[col].fillna(rapm_avg.get(col, 0.0))

    br_features = BR_PLAYER_FEATURES + BR_TEAM_FEATURES + BR_OPP_FEATURES
    for col in br_features:
        if col not in df.columns:
            df[col] = np.nan
    br_avg = {col: float(df[col].mean()) if df[col].notna().any() else 0.0 for col in br_features}
    for col in br_features:
        df[col] = df[col].fillna(br_avg.get(col, 0.0))

    # Target: next-game points is the current row’s actual PTS
    df["y_pts"] = df["PTS"].astype(float)

    # Require enough history for rolling stats + context
    need_cols = ALL_FEATURES + ["y_pts"]
    df = df.dropna(subset=need_cols).reset_index(drop=True)


    # Defender feature coverage summary
    if "opp_pos_def_fg_pct" in df.columns:
        nonzero = int((df["opp_pos_def_fg_pct"] > 0).sum())
        total = int(len(df))
        print("Defender feature coverage:", nonzero, "/", total)

    # Save parquet
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df_out = df[["player_name", "season", "GAME_DATE", "team_abbr"] + ALL_FEATURES + ["y_pts"]].copy()

    df_out.to_parquet(OUT_PARQUET, index=False)

    print(f"Saved: {OUT_PARQUET}")
    print("Rows:", len(df_out))
    print("Players:", df_out["player_name"].nunique())
    print("Feature cols:", len(ALL_FEATURES))

    base_cols = ["player_name", "season", "GAME_DATE", "team_abbr"] + RAW_STAT_COLS + ALL_FEATURES + EXTRA_ROLL_FEATURES
    df_base = df[base_cols].copy()
    df_base.to_parquet(OUT_BASE_PARQUET, index=False)
    print(f"Saved: {OUT_BASE_PARQUET}")


if __name__ == "__main__":
    main()
