from pathlib import Path
import pandas as pd

from dotenv import load_dotenv
from supabase_client import get_supabase

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_CSV = DATA_DIR / "calibration_points_props.csv"

CHUNK_SIZE = 1000


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    sb = get_supabase()

    all_rows = []
    offset = 0

    while True:
        res = (
            sb.table("predictions")
            .select("p_raw,hit,market,side")
            .not_.is_("p_raw", "null")
            .not_.is_("hit", "null")
            .eq("market", "player_points")
            .eq("side", "over")
            .range(offset, offset + CHUNK_SIZE - 1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break
        all_rows.extend(rows)
        offset += CHUNK_SIZE

    df = pd.DataFrame(all_rows)
    if len(df) == 0:
        raise RuntimeError("No calibration rows found in predictions.")

    df = df.rename(columns={"hit": "y_hit"})
    df = df[["p_raw", "y_hit"]]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

    print("Saved:", OUT_CSV)
    print("Rows:", len(df))
    print("Hit rate:", float(df["y_hit"].mean()))


if __name__ == "__main__":
    main()
