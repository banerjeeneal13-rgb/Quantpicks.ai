from pathlib import Path
import pandas as pd

from dotenv import load_dotenv
from supabase_client import get_supabase

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


def compute_profit(result: str, stake: float, odds: float) -> float:
    if result == "win":
        return stake * (odds - 1)
    if result == "lose":
        return -stake
    return 0.0


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs csv: {LOGS_CSV}")

    logs = pd.read_csv(
        LOGS_CSV,
        usecols=["GAME_DATE", "PlayerName", "PTS", "REB", "AST", "FG3M", "BLK", "STL", "TOV"],
    )
    logs["game_date"] = pd.to_datetime(logs["GAME_DATE"], errors="coerce").dt.date.astype(str)
    logs["player_norm"] = logs["PlayerName"].apply(norm_name)
    for c in ["PTS", "REB", "AST", "FG3M", "BLK", "STL", "TOV"]:
        logs[c] = pd.to_numeric(logs[c], errors="coerce")
    logs = logs.dropna(subset=["game_date", "player_norm"])

    stat_map = {
        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_turnovers": "TOV",
    }

    sb = get_supabase()

    updated = 0
    offset = 0
    count_res = (
        sb.table("bets")
        .select("id", count="exact")
        .is_("result", "null")
        .execute()
    )
    total = int(count_res.count or 0)

    while offset < total:
        res = (
            sb.table("bets")
            .select("id,player_name,market,side,line,odds,stake,starts_at")
            .is_("result", "null")
            .order("created_at", desc=False)
            .range(offset, offset + CHUNK_SIZE - 1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        patch = []
        for r in rows:
            market = str(r.get("market") or "")
            stat_col = stat_map.get(market)
            starts_at = r.get("starts_at")
            if not starts_at:
                continue
            try:
                game_date = pd.to_datetime(starts_at, errors="coerce").date().isoformat()
            except Exception:
                continue

            player_norm = norm_name(r.get("player_name"))
            row = logs[(logs["player_norm"] == player_norm) & (logs["game_date"] == game_date)]
            if row.empty:
                continue
            if stat_col:
                actual = float(row.iloc[0][stat_col])
            else:
                pts = float(row.iloc[0]["PTS"])
                reb = float(row.iloc[0]["REB"])
                ast = float(row.iloc[0]["AST"])
                if market == "player_points_rebounds_assists":
                    actual = pts + reb + ast
                elif market == "player_points_rebounds":
                    actual = pts + reb
                elif market == "player_points_assists":
                    actual = pts + ast
                elif market == "player_rebounds_assists":
                    actual = reb + ast
                elif market == "player_blocks_steals":
                    actual = float(row.iloc[0]["BLK"]) + float(row.iloc[0]["STL"])
                else:
                    continue
            line = float(r.get("line") or 0)
            side = str(r.get("side") or "").lower()
            if side == "over":
                result = "win" if actual > line else "lose"
            elif side == "under":
                result = "win" if actual < line else "lose"
            else:
                continue

            stake = float(r.get("stake") or 0)
            odds = float(r.get("odds") or 0)
            profit = compute_profit(result, stake, odds)

            patch.append(
                {
                    "id": r.get("id"),
                    "result": result,
                    "profit": profit,
                }
            )

        for i in range(0, len(patch), CHUNK_SIZE):
            chunk = patch[i : i + CHUNK_SIZE]
            if chunk:
                sb.table("bets").upsert(chunk).execute()
                updated += len(chunk)

        offset += CHUNK_SIZE

    print("Updated bets:", updated)


if __name__ == "__main__":
    main()
