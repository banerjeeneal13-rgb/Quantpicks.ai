from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

from supabase_client import get_supabase

CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "injuries_today.csv"


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


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    if len(df) == 0:
        print("CSV is empty, nothing to upload.")
        return

    # Required columns (create if missing)
    for c in ["sport", "game_date", "player_name", "team_abbr", "status", "detail", "source"]:
        if c not in df.columns:
            df[c] = None

    df["sport"] = df["sport"].fillna("NBA").astype(str)
    df["player_name"] = df["player_name"].astype(str).str.strip()
    df["team_abbr"] = df["team_abbr"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].apply(normalize_status)
    df["detail"] = df["detail"].where(df["detail"].notna(), None)
    df["source"] = df["source"].fillna("manual").astype(str)

    # Convert game_date to ISO string (JSON safe)
    gd = pd.to_datetime(df["game_date"], errors="coerce")
    df["game_date"] = gd.dt.strftime("%Y-%m-%d")
    df.loc[gd.isna(), "game_date"] = None

    keep = ["sport", "game_date", "player_name", "team_abbr", "status", "detail", "source"]
    rows = df[keep].to_dict(orient="records")

    sb = get_supabase()

    # Upsert is fine even without a unique constraint; later we can add one.
    sb.table("injuries").upsert(rows).execute()

    print("Uploaded injuries rows:", len(rows))
    print("Done.")


if __name__ == "__main__":
    main()
