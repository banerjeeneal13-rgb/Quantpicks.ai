from pathlib import Path
import pandas as pd

from dotenv import load_dotenv
from supabase_client import get_supabase

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
EXTERNAL_DIR = Path(__file__).resolve().parents[3] / "external"
LOGS_CSV = EXTERNAL_DIR / "all_data.csv"

CHUNK_SIZE = 500


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
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs csv: {LOGS_CSV}")

    logs = pd.read_csv(LOGS_CSV, usecols=["GAME_DATE", "PlayerName", "PTS"])
    logs["game_date"] = pd.to_datetime(logs["GAME_DATE"], errors="coerce").dt.date.astype(str)
    logs["player_norm"] = logs["PlayerName"].apply(norm_name)
    logs["PTS"] = pd.to_numeric(logs["PTS"], errors="coerce")
    logs = logs.dropna(subset=["game_date", "player_norm", "PTS"])

    score_map = {
        (row.player_norm, row.game_date): float(row.PTS)
        for row in logs.itertuples(index=False)
    }

    sb = get_supabase()

    updated = 0
    offset = 0
    count_res = (
        sb.table("predictions")
        .select("id", count="exact")
        .is_("actual_value", "null")
        .execute()
    )
    total = int(count_res.count or 0)

    while offset < total:
        res = (
            sb.table("predictions")
            .select("id,player_name,game_date,line,side")
            .is_("actual_value", "null")
            .order("game_date", desc=False)
            .range(offset, offset + CHUNK_SIZE - 1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        patch = []
        for r in rows:
            player_norm = norm_name(r.get("player_name"))
            game_date = str(r.get("game_date") or "")
            actual = score_map.get((player_norm, game_date))
            if actual is None:
                continue
            line = float(r.get("line") or 0)
            side = str(r.get("side") or "").lower()
            if side == "over":
                hit = actual > line
            elif side == "under":
                hit = actual < line
            else:
                hit = None
            patch.append(
                {
                    "id": r.get("id"),
                    "actual_value": actual,
                    "hit": hit,
                }
            )

        for i in range(0, len(patch), CHUNK_SIZE):
            chunk = patch[i : i + CHUNK_SIZE]
            if chunk:
                sb.table("predictions").upsert(chunk).execute()
                updated += len(chunk)

        offset += CHUNK_SIZE

    print("Updated predictions:", updated)


if __name__ == "__main__":
    main()
