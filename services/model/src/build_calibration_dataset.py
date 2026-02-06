import math
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
EXTERNAL_DIR = Path(__file__).resolve().parents[3] / "external"

HIST_PROPS_CSV = DATA_DIR / "historical_props_player_points.csv"
LOGS_CSV = EXTERNAL_DIR / "all_data.csv"
TRAIN_PARQUET = DATA_DIR / "points_training.parquet"
OUT_CSV = DATA_DIR / "calibration_points_props.csv"


def norm_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip().lower()
    s = s.replace("â€™", "'").replace("'", "").replace(".", "")
    suffixes = [" jr", " sr", " ii", " iii", " iv", " v"]
    for suf in suffixes:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return " ".join(s.split())


def main():
    if not HIST_PROPS_CSV.exists():
        raise RuntimeError(f"Missing historical props: {HIST_PROPS_CSV}")
    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs: {LOGS_CSV}")
    if not TRAIN_PARQUET.exists():
        raise RuntimeError(f"Missing training parquet: {TRAIN_PARQUET}")

    props = pd.read_csv(HIST_PROPS_CSV)
    props = props[props["market"] == "player_points"].copy()
    props["player_norm"] = props["player_name"].apply(norm_name)
    props["game_date"] = pd.to_datetime(props["game_date"], errors="coerce").dt.date.astype(str)

    props = props[props["side"] == "over"].copy()
    props["line"] = pd.to_numeric(props["line"], errors="coerce")
    props = props.dropna(subset=["player_norm", "game_date", "line"])

    # median line per player per game
    lines = (
        props.groupby(["player_norm", "game_date"], as_index=False)["line"]
        .median()
        .rename(columns={"line": "line_med"})
    )

    logs = pd.read_csv(LOGS_CSV, usecols=["GAME_DATE", "PlayerName", "PTS"])
    logs["game_date"] = pd.to_datetime(logs["GAME_DATE"], errors="coerce").dt.date.astype(str)
    logs["player_norm"] = logs["PlayerName"].apply(norm_name)
    logs["PTS"] = pd.to_numeric(logs["PTS"], errors="coerce")
    logs = logs.dropna(subset=["player_norm", "game_date", "PTS"])

    merged = lines.merge(logs, on=["player_norm", "game_date"], how="inner")
    merged["y_hit"] = (merged["PTS"] > merged["line_med"]).astype(int)

    train = pd.read_parquet(TRAIN_PARQUET)
    train["player_norm"] = train["player_name"].apply(norm_name)
    train["game_date"] = pd.to_datetime(train["GAME_DATE"], errors="coerce").dt.date.astype(str)

    keep_cols = [c for c in train.columns if c not in ["y_pts"]]
    train = train[keep_cols]

    calib = merged.merge(
        train,
        left_on=["player_norm", "game_date"],
        right_on=["player_norm", "game_date"],
        how="inner",
    )

    calib = calib.rename(columns={"line_med": "line"})
    calib = calib.drop(columns=["PlayerName", "GAME_DATE"], errors="ignore")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    calib.to_csv(OUT_CSV, index=False)

    print("Saved:", OUT_CSV)
    print("Rows:", len(calib))
    print("Hit rate:", float(calib["y_hit"].mean()) if len(calib) > 0 else math.nan)


if __name__ == "__main__":
    main()
