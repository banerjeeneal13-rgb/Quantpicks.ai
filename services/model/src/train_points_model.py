import json
from pathlib import Path
import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.linear_model import LogisticRegression

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"

TRAIN_PARQUET = DATA_DIR / "points_training.parquet"
CALIBRATION_CSV = DATA_DIR / "calibration_points_props.csv"
MODEL_OUT = MODELS_DIR / "points_model_v4.joblib"
REPORT_OUT = MODELS_DIR / "points_training_report_v4.json"

# Must match build_points_dataset.py
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

P_CLIP_LO = 0.03
P_CLIP_HI = 0.97

# If P feels too high overall, bump this up (1.10 -> 1.25 is typical)
STD_MULTIPLIER = 1.20


def normal_cdf(x: np.ndarray) -> np.ndarray:
    # approx CDF using erf
    from math import erf, sqrt
    return 0.5 * (1.0 + np.vectorize(erf)(x / np.sqrt(2.0)))


def p_over_from_mu_sigma(mu: np.ndarray, sigma: np.ndarray, line: np.ndarray) -> np.ndarray:
    # P(X > line) for Normal(mu, sigma)
    z = (mu - line) / np.maximum(sigma, 1e-6)
    return normal_cdf(z)


def train():
    if not TRAIN_PARQUET.exists():
        raise RuntimeError(f"Missing training parquet: {TRAIN_PARQUET}. Run build_points_dataset.py first.")

    df = pd.read_parquet(TRAIN_PARQUET)

    missing = [c for c in FEATURES + ["y_pts"] if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in training data: {missing}. Have: {list(df.columns)}")

    X = df[FEATURES].astype(float)
    baseline = df["pts_ma_10"].astype(float)
    # Predict delta from recent baseline to reduce extreme under/over bias
    y = (df["y_pts"].astype(float) - baseline)

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = HistGradientBoostingRegressor(
        max_depth=3,
        max_leaf_nodes=31,
        learning_rate=0.05,
        max_iter=350,
        min_samples_leaf=30,
        l2_regularization=0.1,
        random_state=42,
    )

    model.fit(X_train, y_train)

    pred_val = model.predict(X_val)
    resid = (y_val.values - pred_val)

    mae = float(mean_absolute_error(y_val, pred_val))
    rmse = float(np.sqrt(mean_squared_error(y_val, pred_val)))
    r2 = float(r2_score(y_val, pred_val))

    # Global uncertainty from residuals
    global_std = float(np.std(resid))
    global_std = max(global_std, 2.0)

    # Player-specific std map (optional; helps “confidence realism”)
    player_std_map = {}
    if "player_name" in df.columns:
        # We have player_name in dataset by design
        val_players = df.loc[X_val.index, "player_name"].astype(str).values
        tmp = pd.DataFrame({"player": val_players, "resid": resid})
        for p, g in tmp.groupby("player"):
            if len(g) >= 25:
                player_std_map[p] = float(np.std(g["resid"].values))

    # Build probability calibrator:
    # We'll create a proxy "line" by using the historical median points.
    # It’s not perfect but it stabilizes probability extremes.
    calibrator = None
    line_proxy = None
    if CALIBRATION_CSV.exists():
        calib = pd.read_csv(CALIBRATION_CSV)
        if "p_raw" in calib.columns and "y_hit" in calib.columns:
            calib = calib.dropna(subset=["p_raw", "y_hit"])
            if len(calib) > 0:
                p_raw = calib["p_raw"].astype(float).values
                y_bin = calib["y_hit"].astype(int).values
                calibrator = LogisticRegression(solver="lbfgs")
                calibrator.fit(p_raw.reshape(-1, 1), y_bin)
        else:
            calib = calib.dropna(subset=["line", "y_hit"])
            if len(calib) > 0:
                Xc = calib[FEATURES].astype(float)
                mu_c = model.predict(Xc) + Xc["pts_ma_10"].astype(float).values
                sigma_c = np.full_like(mu_c, global_std) * STD_MULTIPLIER
                line_c = calib["line"].astype(float).values
                p_raw = p_over_from_mu_sigma(mu_c, sigma_c, line_c)
                y_bin = calib["y_hit"].astype(int).values

                calibrator = LogisticRegression(solver="lbfgs")
                calibrator.fit(p_raw.reshape(-1, 1), y_bin)
    else:
        line_proxy = float(df["y_pts"].median())
        mu_val = pred_val + baseline.values[X_val.index]
        sigma_val = np.full_like(mu_val, global_std) * STD_MULTIPLIER
        line_val = np.full_like(mu_val, line_proxy)
        p_raw = p_over_from_mu_sigma(mu_val, sigma_val, line_val)
        y_bin = (y_val.values > line_proxy).astype(int)
        calibrator = LogisticRegression(solver="lbfgs")
        calibrator.fit(p_raw.reshape(-1, 1), y_bin)

    artifact = {
        "model": model,
        "features": FEATURES,
        "target_mode": "delta_from_pts_ma_10",
        "baseline_feature": "pts_ma_10",
        "delta_scale": 1.0,
        "proj_blend_weight": 0.5,
        "baseline_floor_pct": 0.85,
        "global_std": global_std,
        "player_std_map": player_std_map,
        "std_multiplier": STD_MULTIPLIER,
        "p_clip_lo": P_CLIP_LO,
        "p_clip_hi": P_CLIP_HI,
        "calibrator": calibrator,
        "calibration_line_proxy": line_proxy,
        "calibration_source": "historical_props" if CALIBRATION_CSV.exists() else "median_proxy",
    }

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, MODEL_OUT)

    report = {
        "model_path": str(MODEL_OUT),
        "train_rows": int(len(df)),
        "val_mae": mae,
        "val_rmse": rmse,
        "val_r2": r2,
        "global_std": global_std,
        "std_multiplier": STD_MULTIPLIER,
        "p_clip": [P_CLIP_LO, P_CLIP_HI],
        "player_std_entries": int(len(player_std_map)),
        "calibration_line_proxy": line_proxy,
        "calibration_source": "historical_props" if CALIBRATION_CSV.exists() else "median_proxy",
    }

    REPORT_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Saved model to {MODEL_OUT}")
    print(f"Saved report to {REPORT_OUT}")
    print("VAL MAE:", round(mae, 3))
    print("VAL RMSE:", round(rmse, 3))
    print("VAL R2:", round(r2, 3))
    print("VAL global_std:", round(global_std, 3))
    print("Player std entries:", len(player_std_map))


if __name__ == "__main__":
    train()
