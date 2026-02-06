import pandas as pd
import numpy as np

if __name__ == "__main__":
    df = pd.read_parquet("../data/points_training.parquet")

    # player-level std of actual points
    stds = (
        df.groupby("player_name")["y_pts"]
        .std()
        .fillna(df["y_pts"].std())
        .clip(lower=2.5, upper=18.0)
    )

    out = stds.reset_index()
    out.columns = ["player_name", "player_std"]
    out.to_csv("../data/player_points_std.csv", index=False)

    print("Saved ../data/player_points_std.csv rows=", len(out))
    print("Global std fallback =", round(float(df["y_pts"].std()), 3))
