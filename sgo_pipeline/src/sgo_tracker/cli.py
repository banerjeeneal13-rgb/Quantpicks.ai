"""CLI entrypoint for SportsGameOdds v2 collector.

Run:
  export SPORTS_GAME_ODDS_API_KEY="***"
  python -m sgo_tracker markets --out data/markets_nba.json
  python -m sgo_tracker fetch --limit 50 --out data/historical_props.csv
  python -m sgo_tracker fetch --event-id <EVENT_ID> --out data/historical_props.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime

from .config import load_settings
from .sgo_client import SGOClient
from .normalize import normalize_event_odds, utc_now_iso
from .market_discovery import write_markets_json
from .storage import append_rows


def setup_logger() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def dump_debug(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def cmd_events(args: argparse.Namespace) -> int:
    settings = load_settings()
    client = SGOClient(settings)
    payload = client.events(
        limit=args.limit,
        bookmaker_ids=args.bookmakers,
        event_id=args.event_id,
        include_alt_lines=False,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2)[:10000])
    return 0


def cmd_markets(args: argparse.Namespace) -> int:
    settings = load_settings()
    client = SGOClient(settings)
    payload = client.markets()
    if args.debug:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dump_debug(f"data/raw/markets_{ts}.json", payload)
    write_markets_json(args.out, payload)
    logging.info("Wrote markets to %s", args.out)
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    settings = load_settings()
    client = SGOClient(settings)

    include_alt_lines = bool(args.event_id)
    payload = client.events(
        limit=args.limit,
        bookmaker_ids=args.bookmakers,
        event_id=args.event_id,
        include_alt_lines=include_alt_lines,
    )

    if args.debug:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dump_debug(f"data/raw/events_{ts}.json", payload)

    events = payload.get("events") or payload.get("data") or payload
    if not isinstance(events, list):
        logging.error("Unexpected events payload shape.")
        return 2

    fetch_run_id = str(uuid.uuid4())
    timestamp_utc = utc_now_iso()

    rows = []
    for event in events:
        odds = event.get("odds") or {}
        rows.extend(normalize_event_odds(event, odds, fetch_run_id, timestamp_utc))

    added = append_rows(args.out, rows)
    logging.info("Appended %d rows to %s", added, args.out)
    return 0


def main() -> None:
    setup_logger()

    parser = argparse.ArgumentParser(prog="sgo_tracker")
    sub = parser.add_subparsers(dest="command")

    events_parser = sub.add_parser("events")
    events_parser.add_argument("--limit", type=int, default=10)
    events_parser.add_argument("--bookmakers", type=lambda x: x.split(","), default=None)
    events_parser.add_argument("--event-id", default=None)
    events_parser.set_defaults(func=cmd_events)

    markets_parser = sub.add_parser("markets")
    markets_parser.add_argument("--out", required=True)
    markets_parser.add_argument("--debug", action="store_true")
    markets_parser.set_defaults(func=cmd_markets)

    fetch_parser = sub.add_parser("fetch")
    fetch_parser.add_argument("--out", required=True)
    fetch_parser.add_argument("--limit", type=int, default=50)
    fetch_parser.add_argument("--bookmakers", type=lambda x: x.split(","), default=None)
    fetch_parser.add_argument("--event-id", default=None)
    fetch_parser.add_argument("--debug", action="store_true")
    fetch_parser.set_defaults(func=cmd_fetch)

    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(1)

    code = args.func(args)
    sys.exit(code)


if __name__ == "__main__":
    main()
