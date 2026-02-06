from __future__ import annotations

import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import joblib

from market_features import MARKET_FEATURES, MARKET_TARGETS, MARKET_MODEL_NAMES

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"

BASE_PARQUET = DATA_DIR / "market_training_base.parquet"


def _p_over_from_mu_sigma(mu: np.ndarray, sigma: np.ndarray, line: np.ndarray) -> np.ndarray:
    z = (line - mu) / np.maximum(sigma, 1e-6)
    return 1.0 - 0.5 * (1.0 + np.vectorize(np.math.erf)(z / np.sqrt(2.0)))


def _compute_target(df: pd.DataFrame, target_key: str) -> pd.Series:
    if target_key == "PRA":
        return df["PTS"] + df["REB"] + df["AST"]
    if target_key == "PA":
        return df["PTS"] + df["AST"]
    if target_key == "PR":
        return df["PTS"] + df["REB"]
    if target_key == "RA":
        return df["REB"] + df["AST"]
    if target_key == "STOCKS":
        return df["BLK"] + df["STL"]
    return df[target_key]


def train(market: str):
    if market not in MARKET_FEATURES:
        raise RuntimeError(f"Unknown market: {market}")

    if not BASE_PARQUET.exists():
        raise RuntimeError(f"Missing base dataset: {BASE_PARQUET}. Run build_points_dataset.py first.")

    df = pd.read_parquet(BASE_PARQUET)
    features = MARKET_FEATURES[market]
    target_key = MARKET_TARGETS[market]

    required = ["player_name"] + features + ["PTS", "REB", "AST", "FG3M", "BLK", "STL"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in base dataset: {missing}")

    y = _compute_target(df, target_key).astype(float)
    X = df[features].copy()
    for c in features:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask].astype(float)
    y = y[mask]
    players = df.loc[mask, "player_name"].astype(str).values

    if len(X) < 100:
        raise RuntimeError(f"Not enough training rows for {market}: {len(X)}")

    X_train, X_val, y_train, y_val, p_train, p_val = train_test_split(
        X, y, players, test_size=0.2, random_state=42
    )

    model = HistGradientBoostingRegressor(
        max_depth=6,
        learning_rate=0.06,
        max_iter=400,
        random_state=42,
    )
    model.fit(X_train, y_train)

    pred_val = model.predict(X_val)
    resid = y_val.values - pred_val
    mae = float(mean_absolute_error(y_val, pred_val))
    rmse = float(np.sqrt(mean_squared_error(y_val, pred_val)))
    r2 = float(r2_score(y_val, pred_val))

    global_std = float(np.std(resid)) if len(resid) else 0.0
    player_std_map = {}
    if len(resid) > 0:
        tmp = pd.DataFrame({"player": p_val, "resid": resid})
        player_std_map = (
            tmp.groupby("player")["resid"].std().dropna().to_dict()
        )

    calibrator = None
    line_proxy = float(np.median(y_val))
    sigma_val = np.full_like(pred_val, global_std if global_std > 0 else 6.0)
    line_val = np.full_like(pred_val, line_proxy)
    p_raw = _p_over_from_mu_sigma(pred_val, sigma_val, line_val)
    y_bin = (y_val.values > line_proxy).astype(int)
    if len(np.unique(y_bin)) > 1:
        calibrator = LogisticRegression(solver="lbfgs")
        calibrator.fit(p_raw.reshape(-1, 1), y_bin)

    artifact = {
        "model": model,
        "features": features,
        "global_std": global_std if global_std > 0 else 6.0,
        "player_std_map": player_std_map,
        "calibrator": calibrator,
        "p_clip_lo": 0.03,
        "p_clip_hi": 0.97,
        "std_multiplier": 1.0,
        "calibration_line_proxy": line_proxy,
        "calibration_source": "median_proxy",
    }

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_name = MARKET_MODEL_NAMES[market]
    model_path = MODELS_DIR / model_name
    joblib.dump(artifact, model_path)

    report = {
        "market": market,
        "model_path": str(model_path),
        "train_rows": int(len(X)),
        "val_rows": int(len(y_val)),
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "global_std": float(artifact["global_std"]),
        "player_std_entries": int(len(player_std_map)),
        "calibration_line_proxy": line_proxy,
        "calibration_source": "median_proxy",
    }

    report_path = MODELS_DIR / f"{market}_training_report_v1.json"
    report_path.write_text(json.dumps(report, indent=2))

    print(f"Saved model to {model_path}")
    print(f"Saved report to {report_path}")
    print("VAL MAE:", round(mae, 3))
    print("VAL RMSE:", round(rmse, 3))
    print("VAL R2:", round(r2, 3))
    print("VAL global_std:", round(global_std, 3))
    print("Player std entries:", len(player_std_map))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", required=True, help="market key, e.g. player_rebounds")
    args = parser.parse_args()
    train(args.market.strip())


if __name__ == "__main__":
    main()
