import os
import csv
from datetime import datetime, timezone
from typing import List, Dict, Any

from dotenv import load_dotenv

from supabase_client import get_supabase

CHUNK_SIZE = 250


def parse_float(value):
    try:
        return float(value)
    except Exception:
        return None


def parse_str(value):
    s = str(value).strip()
    return s if s else None


def parse_iso_dt(value):
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def parse_date(value):
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        return None


def load_rows(csv_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            player_name = parse_str(row.get("player_name"))
            market = parse_str(row.get("market"))
            line = parse_float(row.get("line"))
            book = parse_str(row.get("book"))
            if book:
                book = book.lower()

            if not player_name or not market or line is None or not book:
                continue

            pulled_at = parse_iso_dt(row.get("pulled_at")) or now_iso
            game_date = parse_date(row.get("game_date"))
            game = parse_str(row.get("game"))
            event_id = parse_str(row.get("event_id"))
            over_odds = parse_float(row.get("over_odds"))
            under_odds = parse_float(row.get("under_odds"))
            notes = parse_str(row.get("notes"))

            rows.append(
                {
                    "pulled_at": pulled_at,
                    "game_date": game_date,
                    "game": game,
                    "event_id": event_id,
                    "player_name": player_name,
                    "market": market,
                    "line": line,
                    "over_odds": over_odds,
                    "under_odds": under_odds,
                    "book": book,
                    "source": "manual",
                    "notes": notes,
                }
            )
    return rows


def upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sb = get_supabase()

    total = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        res = sb.table("manual_odds").upsert(
            chunk,
            on_conflict="player_name,market,line,book,pulled_at"
        ).execute()

        data = getattr(res, "data", None)
        if isinstance(data, list):
            total += len(data)
        else:
            total += len(chunk)
    return total


def main():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    csv_path = os.environ.get("MANUAL_ODDS_CSV", "").strip()
    if not csv_path:
        raise RuntimeError("Set MANUAL_ODDS_CSV or pass via env to point at a CSV file.")

    rows = load_rows(csv_path)
    if not rows:
        print("No valid rows found.")
        return

    upserted = upsert_rows(rows)
    print(f"Upserted {upserted} manual odds rows.")


if __name__ == "__main__":
    main()
