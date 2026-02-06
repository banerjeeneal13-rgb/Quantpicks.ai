from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from market_features import MARKET_FEATURES, MARKET_CACHE_NAMES

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
BASE_PARQUET = DATA_DIR / "market_training_base.parquet"


def build_cache(market: str):
    if market not in MARKET_FEATURES:
        raise RuntimeError(f"Unknown market: {market}")
    if not BASE_PARQUET.exists():
        raise RuntimeError(f"Missing base dataset: {BASE_PARQUET}. Run build_points_dataset.py first.")

    df = pd.read_parquet(BASE_PARQUET)
    features = MARKET_FEATURES[market]

    required = ["player_name", "GAME_DATE"] + features
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Base dataset missing columns: {missing}")

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df = df.dropna(subset=["player_name", "GAME_DATE"]).copy()

    df = df.sort_values("GAME_DATE", ascending=False)
    df = df.dropna(subset=features)
    latest = df.groupby("player_name", as_index=False).head(1)

    out_cols = ["player_name", "GAME_DATE"] + features
    out = latest[out_cols].copy()

    out_path = DATA_DIR / MARKET_CACHE_NAMES[market]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"Saved: {out_path}")
    print("Rows:", len(out))
    print("Players:", out["player_name"].nunique())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", required=True, help="market key, e.g. player_rebounds")
    args = parser.parse_args()
    build_cache(args.market.strip())


if __name__ == "__main__":
    main()
