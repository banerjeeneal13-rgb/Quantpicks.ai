import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import csv

import requests
import pandas as pd
from dotenv import load_dotenv

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
LOGS_CSV = Path(__file__).resolve().parents[3] / "external" / "all_data.csv"
OUT_CSV = DATA_DIR / "historical_props_player_points.csv"

SPORT_KEY = "basketball_nba"
REGIONS = "us"
MARKETS = "player_points"
ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"

SLEEP_BETWEEN_CALLS = 0.5


def odds_api_get(url: str, params: dict, timeout=60) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        msg = r.text[:500] if r.text else ""
        raise RuntimeError(f"Odds API error {r.status_code}: {msg}")
    return r.json()


def iter_dates(start_date: datetime, end_date: datetime):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def parse_commence_date(iso_str: str) -> str | None:
    if not iso_str:
        return None
    try:
        s = str(iso_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        return None


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = (os.getenv("ODDS_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing ODDS_API_KEY in services/model/.env")

    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs csv: {LOGS_CSV}")

    logs = pd.read_csv(LOGS_CSV, usecols=["GAME_DATE"])
    logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"], errors="coerce")
    start_date = logs["GAME_DATE"].min()
    end_date = logs["GAME_DATE"].max()
    if pd.isna(start_date) or pd.isna(end_date):
        raise RuntimeError("Could not infer date range from logs.")

    start_date = start_date.date()
    end_date = end_date.date()

    snapshot_hour = int(os.getenv("HIST_SNAPSHOT_HOUR_UTC", "23"))
    snapshot_min = int(os.getenv("HIST_SNAPSHOT_MIN_UTC", "0"))

    events_url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
    odds_url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
    max_events = int(os.getenv("HIST_MAX_EVENTS_PER_DAY", "0"))

    existing_keys = set()
    if OUT_CSV.exists():
        try:
            with OUT_CSV.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_keys.add(
                        (row.get("event_id"), row.get("book"), row.get("player_name"),
                         row.get("side"), row.get("line"), row.get("market"))
                    )
        except Exception:
            existing_keys = set()

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with OUT_CSV.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "snapshot_date",
                "event_id",
                "commence_time",
                "game_date",
                "book",
                "market",
                "player_name",
                "side",
                "line",
                "odds",
            ],
        )
        if OUT_CSV.stat().st_size == 0:
            writer.writeheader()

        for d in iter_dates(start_date, end_date):
            snapshot_dt = datetime(d.year, d.month, d.day, snapshot_hour, snapshot_min, tzinfo=timezone.utc)
            params = {
                "apiKey": api_key,
                "dateFormat": DATE_FORMAT,
                "date": snapshot_dt.isoformat().replace("+00:00", "Z"),
            }
            events_payload = odds_api_get(events_url, params=params, timeout=60)
            events = events_payload.get("data") or []
            if max_events > 0:
                events = events[:max_events]

            for ev in events:
                event_id = str(ev.get("id") or "")
                commence_time = ev.get("commence_time")
                game_date = parse_commence_date(commence_time)
                if not event_id:
                    continue

                odds_params = {
                    "apiKey": api_key,
                    "regions": REGIONS,
                    "markets": MARKETS,
                    "oddsFormat": ODDS_FORMAT,
                    "dateFormat": DATE_FORMAT,
                    "date": snapshot_dt.isoformat().replace("+00:00", "Z"),
                }
                odds_payload = odds_api_get(f"{odds_url}/{event_id}/odds", params=odds_params, timeout=60)
                data = odds_payload.get("data") or {}
                bookmakers = data.get("bookmakers") or []
                for bk in bookmakers:
                    book_key = str(bk.get("key") or "").strip().lower()
                    mkts = bk.get("markets") or []
                    for m in mkts:
                        if str(m.get("key") or "") != MARKETS:
                            continue
                        outcomes = m.get("outcomes") or []
                        for o in outcomes:
                            side = str(o.get("name") or "").strip().lower()
                            player_raw = str(o.get("description") or "").strip()
                            line = o.get("point")
                            odds = o.get("price")
                            if not player_raw or side not in ("over", "under") or line is None or odds is None:
                                continue

                            key = (event_id, book_key, player_raw, side, str(line), MARKETS)
                            if key in existing_keys:
                                continue
                            existing_keys.add(key)

                            writer.writerow(
                                {
                                    "snapshot_date": snapshot_dt.date().isoformat(),
                                    "event_id": event_id,
                                    "commence_time": commence_time,
                                    "game_date": game_date,
                                    "book": book_key,
                                    "market": MARKETS,
                                    "player_name": player_raw,
                                    "side": side,
                                    "line": line,
                                    "odds": odds,
                                }
                            )
            time.sleep(SLEEP_BETWEEN_CALLS)

    print("Saved:", OUT_CSV)


if __name__ == "__main__":
    main()
