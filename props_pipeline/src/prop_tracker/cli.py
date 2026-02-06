"""CLI entrypoint for prop_tracker.

Env vars:
  ODDS_PROVIDER=theoddsapi
  ODDS_API_KEY=...
  REGIONS=us,au
  BOOKMAKERS=fanduel,draftkings,bet365
  SPORT=basketball_nba
  STATS_PROVIDER=csv|nba_api|api_sports
  API_SPORTS_KEY=...
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid

from dotenv import load_dotenv

from .config import load_settings
from .providers import get_provider
from .normalize import normalize_outcomes
from .storage import append_rows
from .evaluate import evaluate_props
from .fetch_stats import get_stats_provider
from .utils import utc_now_iso


MARKET_ALIASES = {
    "points": "player_points",
    "rebs": "player_rebounds",
    "rebounds": "player_rebounds",
    "asts": "player_assists",
    "assists": "player_assists",
    "threes": "player_threes",
    "combos": "combo",
    "stocks": "player_blocks_steals",
}

COMBO_MARKETS = [
    "player_points_rebounds_assists",
    "player_points_assists",
    "player_points_rebounds",
    "player_rebounds_assists",
]


def setup_logger() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_markets(value: str) -> list[str]:
    raw = [v.strip().lower() for v in value.split(",") if v.strip()]
    markets: list[str] = []
    for item in raw:
        mapped = MARKET_ALIASES.get(item, item)
        if mapped == "combo":
            markets.extend(COMBO_MARKETS)
        else:
            markets.append(mapped)
    return sorted(set(markets))


def cmd_fetch(args: argparse.Namespace) -> int:
    settings = load_settings()
    if not settings.odds_api_key:
        logging.error("ODDS_API_KEY missing; cannot fetch odds.")
        return 2

    provider = get_provider(settings)
    markets = parse_markets(args.markets)
    timestamp_utc = utc_now_iso()
    fetch_run_id = str(uuid.uuid4())

    events = list(provider.fetch_events(settings.sport))
    if not events:
        logging.warning("No events found for %s", settings.sport)
        return 0

    outcomes = []
    line_counts: dict[str, set[float]] = {}
    for event in events:
        logging.info("Fetching props for event %s", event.event_id)
        event_outcomes = list(
            provider.fetch_props(
                event,
                markets=markets,
                regions=settings.regions,
                bookmakers=settings.bookmakers,
            )
        )
        outcomes.extend(event_outcomes)
        for o in event_outcomes:
            key = f"{o.market_key}|{o.player_name}|{o.sportsbook}"
            line_counts.setdefault(key, set()).add(o.line)

    rows = normalize_outcomes(outcomes, provider.name, fetch_run_id, timestamp_utc)
    if not rows:
        logging.warning("No prop outcomes returned.")
        return 0

    added = append_rows(args.out, rows)
    logging.info("Appended %d new rows to %s", added, args.out)

    # Alternate lines warning if no player has multiple tiers for any requested market.
    has_alternates = any(len(lines) > 1 for lines in line_counts.values())
    if not has_alternates:
        logging.warning("Alternate lines not available from this provider/tier.")
    return 0


def cmd_evaluate(args: argparse.Namespace) -> int:
    out_path = evaluate_props(args.props, args.stats, args.out)
    logging.info("Wrote evaluation CSV to %s", out_path)
    return 0


def cmd_fetch_stats(args: argparse.Namespace) -> int:
    settings = load_settings()
    provider = get_stats_provider(settings)
    out_path = provider.fetch_stats(args.props, args.out)
    logging.info("Wrote stats CSV to %s", out_path)
    return 0


def main() -> None:
    load_dotenv()
    setup_logger()

    parser = argparse.ArgumentParser(prog="prop-tracker")
    sub = parser.add_subparsers(dest="command")

    fetch_parser = sub.add_parser("fetch")
    fetch_parser.add_argument("--markets", required=True, help="points,rebs,asts,threes,combos,stocks")
    fetch_parser.add_argument("--out", required=True, help="Output CSV path")
    fetch_parser.set_defaults(func=cmd_fetch)

    eval_parser = sub.add_parser("evaluate")
    eval_parser.add_argument("--props", required=True, help="Historical props CSV")
    eval_parser.add_argument("--stats", required=True, help="Actual stats CSV")
    eval_parser.add_argument("--out", required=True, help="Evaluation output CSV")
    eval_parser.set_defaults(func=cmd_evaluate)

    stats_parser = sub.add_parser("fetch-stats")
    stats_parser.add_argument("--props", required=True, help="Historical props CSV")
    stats_parser.add_argument("--out", required=True, help="Actual stats CSV output")
    stats_parser.set_defaults(func=cmd_fetch_stats)

    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(1)

    code = args.func(args)
    sys.exit(code)


if __name__ == "__main__":
    main()
