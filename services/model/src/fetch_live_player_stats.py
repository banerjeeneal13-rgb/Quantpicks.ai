from pathlib import Path
from datetime import datetime
import time
import pandas as pd
from dotenv import load_dotenv

from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2
from supabase_client import get_supabase

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
SLEEP_BETWEEN_GAMES = 0.5
CHUNK_SIZE = 500


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    sb = get_supabase()

    today = datetime.utcnow().strftime("%m/%d/%Y")
    board = scoreboardv2.ScoreboardV2(game_date=today)
    games = board.get_data_frames()[0]
    if games.empty:
        print("No games today.")
        return

    # GameStatusID: 1 = scheduled, 2 = in-progress, 3 = final
    live_games = games[games["GAME_STATUS_ID"].isin([2, 3])]
    if live_games.empty:
        print("No live/final games yet.")
        return

    rows = []
    for _, g in live_games.iterrows():
        game_id = str(g["GAME_ID"])
        game_date = str(g["GAME_DATE_EST"] or "")[:10]

        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        players = box.get_data_frames()[0]
        if players.empty:
            continue

        for _, p in players.iterrows():
            rows.append(
                {
                    "game_id": game_id,
                    "game_date": game_date,
                    "team_abbr": p.get("TEAM_ABBREVIATION"),
                    "player_name": p.get("PLAYER_NAME"),
                    "minutes": p.get("MIN"),
                    "pts": p.get("PTS"),
                    "reb": p.get("REB"),
                    "ast": p.get("AST"),
                    "fg3m": p.get("FG3M"),
                    "blk": p.get("BLK"),
                    "stl": p.get("STL"),
                    "tov": p.get("TO"),
                }
            )
        time.sleep(SLEEP_BETWEEN_GAMES)

    if not rows:
        print("No player rows.")
        return

    total = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        res = sb.table("live_player_stats").upsert(
            chunk,
            on_conflict="game_id,player_name",
        ).execute()
        data = getattr(res, "data", None)
        if isinstance(data, list):
            total += len(data)
        else:
            total += len(chunk)

    print("Upserted live stats:", total)


if __name__ == "__main__":
    main()
