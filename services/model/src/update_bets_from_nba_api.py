from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2
from supabase_client import get_supabase

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


def fetch_boxscore_map(game_date: str) -> dict:
    board = scoreboardv2.ScoreboardV2(game_date=game_date)
    games = board.get_data_frames()[0]
    if games.empty:
        return {}

    game_ids = games["GAME_ID"].astype(str).tolist()
    out = {}
    for gid in game_ids:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid)
        players = box.get_data_frames()[0]
        if players.empty:
            continue
        for _, p in players.iterrows():
            out[(norm_name(p.get("PLAYER_NAME")), gid)] = {
                "PTS": float(p.get("PTS") or 0),
                "REB": float(p.get("REB") or 0),
                "AST": float(p.get("AST") or 0),
                "FG3M": float(p.get("FG3M") or 0),
                "BLK": float(p.get("BLK") or 0),
                "STL": float(p.get("STL") or 0),
                "TOV": float(p.get("TO") or 0),
            }
    return out


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    sb = get_supabase()

    stat_map = {
        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_turnovers": "TOV",
    }

    updated = 0
    offset = 0
    count_res = (
        sb.table("bets")
        .select("id", count="exact")
        .is_("result", "null")
        .execute()
    )
    total = int(count_res.count or 0)

    date_cache = {}

    while offset < total:
        res = (
            sb.table("bets")
            .select("id,player_name,market,side,line,odds,stake,starts_at,event_id")
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
            starts_at = r.get("starts_at")
            if not starts_at:
                continue
            try:
                game_date = pd.to_datetime(starts_at, errors="coerce").strftime("%m/%d/%Y")
            except Exception:
                continue

            if game_date not in date_cache:
                date_cache[game_date] = fetch_boxscore_map(game_date)

            player_norm = norm_name(r.get("player_name"))
            stats = None

            if r.get("event_id"):
                stats = date_cache[game_date].get((player_norm, str(r.get("event_id"))))
            if stats is None:
                # fallback: ignore game_id and use first match
                for (pn, _gid), s in date_cache[game_date].items():
                    if pn == player_norm:
                        stats = s
                        break

            if stats is None:
                continue

            market = str(r.get("market") or "")
            stat_col = stat_map.get(market)
            if stat_col:
                actual = float(stats[stat_col])
            else:
                pts = float(stats["PTS"])
                reb = float(stats["REB"])
                ast = float(stats["AST"])
                if market == "player_points_rebounds_assists":
                    actual = pts + reb + ast
                elif market == "player_points_rebounds":
                    actual = pts + reb
                elif market == "player_points_assists":
                    actual = pts + ast
                elif market == "player_rebounds_assists":
                    actual = reb + ast
                elif market == "player_blocks_steals":
                    actual = float(stats["BLK"]) + float(stats["STL"])
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
