"""CLI for fetching the most recent completed NBA game from first-party sources."""
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from .clients.scoreboard_client import ScoreboardClient
from .clients.stats_client import StatsClient
from .http import RateLimiter
from .models import GameResult
from .normalize import most_recent_game, normalize_fallback, normalize_primary


DEFAULT_TTL_SECONDS = int(os.getenv("NBA_LATEST_CACHE_TTL", "600"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("NBA_LATEST_LOOKBACK_DAYS", "14"))
DEFAULT_TZ = os.getenv("NBA_LATEST_TIMEZONE", "America/New_York")
DEFAULT_CACHE_DIR = os.getenv("NBA_LATEST_CACHE_DIR", str(Path.home() / ".cache" / "nba_latest"))


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(levelname)s: %(message)s")


def _date_range(lookback_days: int, timezone: str) -> list[datetime]:
    tz = ZoneInfo(timezone)
    today = datetime.now(tz).date()
    return [today - timedelta(days=i) for i in range(lookback_days + 1)]


def _find_latest_game(
    lookback_days: int,
    timezone: str,
    cache_dir: Path,
    timeout_s: float,
    max_retries: int,
    backoff_base_s: float,
) -> GameResult:
    rate_limiter = RateLimiter(min_interval_s=1.0)
    primary = ScoreboardClient(
        cache_dir=cache_dir,
        ttl_seconds=DEFAULT_TTL_SECONDS,
        timeout_s=timeout_s,
        max_retries=max_retries,
        backoff_base_s=backoff_base_s,
        rate_limiter=rate_limiter,
    )
    fallback = StatsClient(
        cache_dir=cache_dir,
        ttl_seconds=DEFAULT_TTL_SECONDS,
        timeout_s=timeout_s,
        max_retries=max_retries,
        backoff_base_s=backoff_base_s,
        rate_limiter=rate_limiter,
    )

    last_error: Optional[Exception] = None
    for day in _date_range(lookback_days, timezone):
        try:
            res = primary.fetch_scoreboard(day)
            if res.stale:
                logging.warning("Using stale cached primary response for %s", day)
            games = normalize_primary(res.data, timezone)
            latest = most_recent_game(games)
            if latest:
                return latest
        except Exception as exc:
            last_error = exc

        try:
            res = fallback.fetch_scoreboard(day)
            if res.stale:
                logging.warning("Using stale cached fallback response for %s", day)
            games = normalize_fallback(res.data, timezone)
            latest = most_recent_game(games)
            if latest:
                return latest
        except Exception as exc:
            last_error = exc
            continue

    raise RuntimeError(f"No completed games found in lookback window. Last error: {last_error}")


def _print_table(game: GameResult) -> None:
    print("Latest Completed NBA Game")
    print("-" * 40)
    print(f"Game ID: {game.game_id}")
    print(f"Date:    {game.game_date_iso}")
    print(f"Status:  {game.status}")
    print(f"Away:    {game.away_team.tricode} {game.away_team.name or ''} - {game.away_team.score}")
    print(f"Home:    {game.home_team.tricode} {game.home_team.name or ''} - {game.home_team.score}")
    print(f"Source:  {game.source}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="nba_latest", description="Fetch the latest completed NBA game.")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--timezone", type=str, default=DEFAULT_TZ)
    parser.add_argument("--format", choices=["json", "table"], default="json")
    parser.add_argument("--cache-dir", type=str, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--log-level", choices=["debug", "info", "warning"], default="info")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff", type=float, default=0.5)
    args = parser.parse_args()

    _configure_logging(args.log_level)

    cache_dir = Path(args.cache_dir).expanduser()

    game = _find_latest_game(
        lookback_days=args.lookback_days,
        timezone=args.timezone,
        cache_dir=cache_dir,
        timeout_s=args.timeout,
        max_retries=args.max_retries,
        backoff_base_s=args.backoff,
    )

    if args.format == "json":
        print(json.dumps(asdict(game), ensure_ascii=False))
    else:
        _print_table(game)


if __name__ == "__main__":
    main()
