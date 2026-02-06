from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .clients.nba_cdn_client import NBA_CDNClient
from .clients.stats_nba_client import StatsNBAClient
from .http import RateLimiter
from .storage import append_rows_csv, ensure_dir, load_progress, save_progress, write_parquet
from .transform.normalize import (
    compute_advanced,
    normalize_cdn_boxscore,
    normalize_league_gamelog,
    normalize_stats_advanced,
    normalize_stats_boxscore,
)
from .transform.features import build_features


def _log_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _parse_seasons(value: str) -> list[str]:
    raw = [v.strip() for v in value.split(",") if v.strip()]
    return raw


def _season_label(year: int) -> str:
    return f"{year}-{str(year + 1)[-2:]}"


def _expand_season_range(start_year: int, end_year: int) -> list[str]:
    return [_season_label(y) for y in range(start_year, end_year + 1)]


def _extract_game_ids(rows: Iterable[dict]) -> list[str]:
    ids = set()
    for row in rows:
        gid = str(row.get("GAME_ID") or row.get("GAMEID") or "")
        if gid:
            ids.add(gid)
    return sorted(ids)


def _save_raw(raw_dir: Path, name: str, data: dict) -> None:
    ensure_dir(raw_dir)
    (raw_dir / name).write_text(json.dumps(data), encoding="utf-8")


def _with_meta(row: object, fetched_at: str) -> dict:
    if hasattr(row, "__dataclass_fields__"):
        payload = asdict(row)
    elif isinstance(row, dict):
        payload = dict(row)
    else:
        payload = row.__dict__ if hasattr(row, "__dict__") else {"value": row}
    payload["fetched_at_utc"] = fetched_at
    return payload


def _season_date_range(season: str) -> tuple[date, date]:
    parts = season.split("-")
    start_year = int(parts[0])
    if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 2:
        end_year = int(f"{str(start_year)[:2]}{parts[1]}")
    elif len(parts) > 1 and parts[1].isdigit():
        end_year = int(parts[1])
    else:
        end_year = start_year + 1
    # Approximate NBA season window
    start_date = date(start_year, 10, 1)
    end_date = date(end_year, 6, 30)
    return start_date, end_date


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_schedule_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _schedule_game_ids(
    cdn_client: NBA_CDNClient,
    season: str,
    season_type: str,
    start_date: date,
    end_date: date,
    raw_dir: Path,
    errors_path: Path,
    provenance_path: Path,
    processed_dates: set[str],
    max_days: int,
) -> tuple[list[str], list[dict]]:
    ids = set()
    calendar_rows: list[dict] = []
    try:
        data = cdn_client.fetch_schedule()
    except Exception as exc:
        _log_json(
            errors_path,
            {"season": season, "season_type": season_type, "error": f"schedule_failed: {exc}"},
        )
        return [], []

    _save_raw(raw_dir / "schedule", "scheduleLeagueV2.json", data)
    game_dates = data.get("leagueSchedule", {}).get("gameDates", []) if isinstance(data, dict) else []
    if not isinstance(game_dates, list):
        return [], []

    # Build date -> games map
    date_map: dict[str, list[dict]] = {}
    for day in game_dates:
        games = day.get("games", [])
        if not games:
            continue
        day_date = _parse_schedule_date(games[0].get("gameDateTimeUTC") or games[0].get("gameDateUTC"))
        if day_date is None:
            day_date = _parse_schedule_date(day.get("gameDate"))
        if day_date is None:
            continue
        day_key = day_date.isoformat()
        date_map[day_key] = games

    # Select dates within range
    selected = []
    for day_key in sorted(date_map.keys()):
        day_date = date.fromisoformat(day_key)
        if not (start_date <= day_date <= end_date):
            continue
        if day_key in processed_dates:
            continue
        selected.append(day_key)

    if max_days > 0:
        selected = selected[:max_days]

    for day_key in selected:
        games = date_map.get(day_key, [])
        for game in games:
            label = str(game.get("gameLabel") or "").lower()
            if season_type.lower().startswith("regular"):
                if "preseason" in label or "all-star" in label:
                    continue
            status = game.get("gameStatus")
            if status is not None:
                try:
                    if int(status) != 3:
                        continue
                except Exception:
                    pass
            game_id = str(game.get("gameId") or "")
            if not game_id:
                continue
            ids.add(game_id)
            calendar_rows.append(
                {"game_id": game_id, "season": season, "season_type": season_type, "game_date": day_key}
            )

        processed_dates.add(day_key)
        _log_json(
            provenance_path,
            {
                "endpoint": "scheduleLeagueV2",
                "date": day_key,
                "season": season,
                "season_type": season_type,
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                "source": "cdn",
            },
        )

    return sorted(ids), calendar_rows


def _scoreboard_game_ids(
    cdn_client: NBA_CDNClient,
    season: str,
    season_type: str,
    start_date: date,
    end_date: date,
    raw_dir: Path,
    errors_path: Path,
    provenance_path: Path,
    processed_dates: set[str],
    resume_from: date | None,
    max_days: int,
) -> tuple[list[str], list[dict]]:
    ids = set()
    calendar_rows: list[dict] = []
    processed_count = 0
    for day in _date_range(start_date, end_date):
        if resume_from and day < resume_from:
            continue
        day_key = day.isoformat()
        if day_key in processed_dates:
            continue
        if max_days > 0 and processed_count >= max_days:
            break
        try:
            data = cdn_client.fetch_scoreboard(day)
            _save_raw(raw_dir / "scoreboard", f"scoreboard_{day.strftime('%Y%m%d')}.json", data)
            games = []
            if isinstance(data, dict):
                if "scoreboard" in data:
                    games = data.get("scoreboard", {}).get("games", []) or []
                else:
                    games = data.get("games", []) or []
            for game in games:
                game_id = str(game.get("gameId") or "")
                if not game_id:
                    continue
                ids.add(game_id)
                calendar_rows.append(
                    {
                        "game_id": game_id,
                        "season": season,
                        "season_type": season_type,
                        "game_date": day.isoformat(),
                    }
                )
            _log_json(
                provenance_path,
                {
                    "endpoint": "scoreboard",
                    "date": day.isoformat(),
                    "season": season,
                    "season_type": season_type,
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "source": "cdn",
                },
            )
            processed_dates.add(day_key)
            processed_count += 1
        except Exception as exc:
            _log_json(
                errors_path,
                {
                    "season": season,
                    "season_type": season_type,
                    "date": day.isoformat(),
                    "error": f"scoreboard_failed: {exc}",
                },
            )
            continue
    return sorted(ids), calendar_rows


def cmd_fetch(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    data_dir = out_dir / "data"
    raw_dir = out_dir / "raw"
    meta_dir = out_dir / "metadata"
    ensure_dir(data_dir)
    ensure_dir(raw_dir)
    ensure_dir(meta_dir)

    progress_path = out_dir / "progress.json"
    progress = load_progress(progress_path)
    done_games = set(progress.get("games", []))
    processed_dates = set(progress.get("schedule_dates", []))
    if not processed_dates and progress.get("scoreboard_dates"):
        # Legacy dates came from non-historical scoreboard; reset to allow schedule-based pull.
        logging.warning("Resetting legacy scoreboard_dates; switching to schedule-based date tracking.")
        processed_dates = set()

    seasons = _parse_seasons(args.seasons)
    if args.season_start and args.season_end:
        seasons = _expand_season_range(int(args.season_start), int(args.season_end))

    season_types = [args.season_type]
    if args.playoffs:
        season_types.append("Playoffs")

    rate_limiter = RateLimiter(min_interval_s=args.rate_limit)
    cache_ttl = 0 if args.cache_forever else args.cache_ttl
    stats_client = StatsNBAClient(
        cache_dir=Path(args.cache_dir),
        ttl_seconds=cache_ttl,
        timeout_s=args.timeout,
        max_retries=args.max_retries,
        backoff_base_s=args.backoff,
        rate_limiter=rate_limiter,
    )
    cdn_client = NBA_CDNClient(
        cache_dir=Path(args.cache_dir),
        ttl_seconds=cache_ttl,
        timeout_s=args.timeout,
        max_retries=args.max_retries,
        backoff_base_s=args.backoff,
        rate_limiter=rate_limiter,
    )

    errors_path = meta_dir / "fetch_errors.jsonl"
    provenance_path = meta_dir / "endpoint_provenance.jsonl"

    for season in seasons:
        for season_type in season_types:
            logging.info("Fetching league gamelog for %s %s", season, season_type)
            try:
                gamelog = stats_client.fetch_league_gamelog(season, season_type)
                safe_type = season_type.replace(" ", "_")
                _save_raw(raw_dir / "leaguegamelog", f"leaguegamelog_{season}_{safe_type}.json", gamelog)
                rows = normalize_league_gamelog(gamelog, season, season_type)
                _log_json(
                    provenance_path,
                    {
                        "endpoint": "leaguegamelog",
                        "season": season,
                        "season_type": season_type,
                        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                        "source": "stats",
                    },
                )
            except Exception as exc:
                _log_json(errors_path, {"season": season, "season_type": season_type, "error": str(exc)})
                if not args.scoreboard_fallback:
                    continue
                logging.warning("leaguegamelog failed; falling back to CDN schedule for game IDs.")
                if args.date_start and args.date_end:
                    start_date = date.fromisoformat(args.date_start)
                    end_date = date.fromisoformat(args.date_end)
                else:
                    start_date, end_date = _season_date_range(season)
                resume_from = None
                if args.resume_from_date:
                    resume_from = date.fromisoformat(args.resume_from_date)
                    if resume_from > start_date:
                        start_date = resume_from
                def _process_game_ids(game_ids: list[str]) -> None:
                    if not game_ids:
                        return
                    logging.info("Found %d games via scoreboard for %s %s", len(game_ids), season, season_type)
                    for game_id in game_ids:
                        if game_id in done_games:
                            continue
                        fetched_at = datetime.now(timezone.utc).isoformat()
                        try:
                            raw = cdn_client.fetch_boxscore(game_id)
                            _save_raw(raw_dir / "boxscore_cdn", f"boxscore_{game_id}.json", raw)
                            game_row, player_rows, team_rows = normalize_cdn_boxscore(raw, season, season_type)
                            source = "cdn"
                        except Exception as exc_cdn:
                            try:
                                raw = stats_client.fetch_boxscore_traditional(game_id)
                                _save_raw(raw_dir / "boxscore_stats", f"boxscore_{game_id}.json", raw)
                                game_row, player_rows, team_rows = normalize_stats_boxscore(raw, season, season_type)
                                source = "stats"
                            except Exception as exc_stats:
                                _log_json(
                                    errors_path,
                                    {
                                        "game_id": game_id,
                                        "season": season,
                                        "season_type": season_type,
                                        "error": str(exc_stats),
                                        "cdn_error": str(exc_cdn),
                                    },
                                )
                                continue

                        _log_json(
                            provenance_path,
                            {
                                "endpoint": f"boxscore_{source}",
                                "game_id": game_id,
                                "season": season,
                                "season_type": season_type,
                                "fetched_at_utc": fetched_at,
                            },
                        )

                        game_dict = _with_meta(game_row, fetched_at)
                        player_dicts = [_with_meta(p, fetched_at) for p in player_rows]
                        team_dicts = [_with_meta(t, fetched_at) for t in team_rows]

                        append_rows_csv(data_dir / "games.csv", [game_dict])
                        append_rows_csv(data_dir / "player_boxscores.csv", player_dicts)
                        append_rows_csv(data_dir / "team_boxscores.csv", team_dicts)

                        adv_rows = []
                        try:
                            adv_raw = stats_client.fetch_boxscore_advanced(game_id)
                            _save_raw(raw_dir / "boxscore_advanced", f"boxscore_advanced_{game_id}.json", adv_raw)
                            adv_rows = normalize_stats_advanced(adv_raw, season, season_type)
                            _log_json(
                                provenance_path,
                                {
                                    "endpoint": "boxscoreadvancedv2",
                                    "game_id": game_id,
                                    "season": season,
                                    "season_type": season_type,
                                    "fetched_at_utc": fetched_at,
                                },
                            )
                        except Exception:
                            adv_rows = compute_advanced(player_rows, team_rows, season, season_type)
                            _log_json(
                                provenance_path,
                                {
                                    "endpoint": "computed_advanced",
                                    "game_id": game_id,
                                    "season": season,
                                    "season_type": season_type,
                                    "fetched_at_utc": fetched_at,
                                },
                            )

                        adv_dicts = [_with_meta(a, fetched_at) for a in adv_rows]
                        append_rows_csv(data_dir / "player_advanced.csv", adv_dicts)

                        players_meta = [
                            {"player_id": p.player_id, "player_name": p.player_name, "team_id": p.team_id}
                            for p in player_rows
                        ]
                        teams_meta = [
                            {"team_id": t.team_id, "team_tricode": t.team_tricode}
                            for t in team_rows
                        ]
                        append_rows_csv(meta_dir / "players.csv", players_meta)
                        append_rows_csv(meta_dir / "teams.csv", teams_meta)

                        if args.play_by_play:
                            try:
                                pbp_raw = stats_client.fetch_play_by_play(game_id)
                                _save_raw(raw_dir / "playbyplay", f"playbyplay_{game_id}.json", pbp_raw)
                                _log_json(
                                    provenance_path,
                                    {
                                        "endpoint": "playbyplayv2",
                                        "game_id": game_id,
                                        "season": season,
                                        "season_type": season_type,
                                        "fetched_at_utc": fetched_at,
                                    },
                                )
                            except Exception as exc:
                                _log_json(
                                    errors_path,
                                    {
                                        "game_id": game_id,
                                        "season": season,
                                        "season_type": season_type,
                                        "error": f"playbyplay_failed: {exc}",
                                    },
                                )

                        done_games.add(game_id)
                        progress["games"] = sorted(done_games)
                        save_progress(progress_path, progress)

                        logging.info("Processed game %s (%s)", game_id, source)

                if args.auto_loop:
                    while True:
                        before = len(processed_dates)
                        game_ids, season_calendar_rows = _schedule_game_ids(
                            cdn_client,
                            season=season,
                            season_type=season_type,
                            start_date=start_date,
                            end_date=end_date,
                            raw_dir=raw_dir,
                            errors_path=errors_path,
                            provenance_path=provenance_path,
                            processed_dates=processed_dates,
                            max_days=args.chunk_days,
                        )
                        progress["schedule_dates"] = sorted(processed_dates)
                        save_progress(progress_path, progress)
                        append_rows_csv(meta_dir / "season_calendar.csv", season_calendar_rows)
                        _process_game_ids(game_ids)
                        if len(processed_dates) == before:
                            break
                else:
                    game_ids, season_calendar_rows = _schedule_game_ids(
                        cdn_client,
                        season=season,
                        season_type=season_type,
                        start_date=start_date,
                        end_date=end_date,
                        raw_dir=raw_dir,
                        errors_path=errors_path,
                        provenance_path=provenance_path,
                        processed_dates=processed_dates,
                        max_days=args.chunk_days,
                    )
                    progress["schedule_dates"] = sorted(processed_dates)
                    save_progress(progress_path, progress)
                    append_rows_csv(meta_dir / "season_calendar.csv", season_calendar_rows)
                    _process_game_ids(game_ids)
                continue

            season_calendar_rows = []
            for row in rows:
                game_id = str(row.get("GAME_ID") or row.get("GAMEID") or "")
                game_date = row.get("GAME_DATE") or row.get("GAME_DATE_EST")
                if game_id:
                    season_calendar_rows.append(
                        {
                            "game_id": game_id,
                            "season": season,
                            "season_type": season_type,
                            "game_date": game_date,
                        }
                    )
            append_rows_csv(meta_dir / "season_calendar.csv", season_calendar_rows)

            game_ids = _extract_game_ids(rows)
            logging.info("Found %d games for %s %s", len(game_ids), season, season_type)

            for game_id in game_ids:
                if game_id in done_games:
                    continue
                fetched_at = datetime.now(timezone.utc).isoformat()
                try:
                    # Primary: CDN boxscore
                    raw = cdn_client.fetch_boxscore(game_id)
                    _save_raw(raw_dir / "boxscore_cdn", f"boxscore_{game_id}.json", raw)
                    game_row, player_rows, team_rows = normalize_cdn_boxscore(raw, season, season_type)
                    source = "cdn"
                except Exception as exc_cdn:
                    try:
                        # Fallback: stats.nba.com boxscore
                        raw = stats_client.fetch_boxscore_traditional(game_id)
                        _save_raw(raw_dir / "boxscore_stats", f"boxscore_{game_id}.json", raw)
                        game_row, player_rows, team_rows = normalize_stats_boxscore(raw, season, season_type)
                        source = "stats"
                    except Exception as exc_stats:
                        _log_json(
                            errors_path,
                            {
                                "game_id": game_id,
                                "season": season,
                                "season_type": season_type,
                                "error": str(exc_stats),
                                "cdn_error": str(exc_cdn),
                            },
                        )
                        continue

                _log_json(
                    provenance_path,
                    {
                        "endpoint": f"boxscore_{source}",
                        "game_id": game_id,
                        "season": season,
                        "season_type": season_type,
                        "fetched_at_utc": fetched_at,
                    },
                )

                game_dict = _with_meta(game_row, fetched_at)
                player_dicts = [_with_meta(p, fetched_at) for p in player_rows]
                team_dicts = [_with_meta(t, fetched_at) for t in team_rows]

                append_rows_csv(data_dir / "games.csv", [game_dict])
                append_rows_csv(data_dir / "player_boxscores.csv", player_dicts)
                append_rows_csv(data_dir / "team_boxscores.csv", team_dicts)

                # Advanced metrics: try sourced, else compute
                adv_rows = []
                try:
                    adv_raw = stats_client.fetch_boxscore_advanced(game_id)
                    _save_raw(raw_dir / "boxscore_advanced", f"boxscore_advanced_{game_id}.json", adv_raw)
                    adv_rows = normalize_stats_advanced(adv_raw, season, season_type)
                    _log_json(
                        provenance_path,
                        {
                            "endpoint": "boxscoreadvancedv2",
                            "game_id": game_id,
                            "season": season,
                            "season_type": season_type,
                            "fetched_at_utc": fetched_at,
                        },
                    )
                except Exception:
                    adv_rows = compute_advanced(player_rows, team_rows, season, season_type)
                    _log_json(
                        provenance_path,
                        {
                            "endpoint": "computed_advanced",
                            "game_id": game_id,
                            "season": season,
                            "season_type": season_type,
                            "fetched_at_utc": fetched_at,
                        },
                    )

                adv_dicts = [_with_meta(a, fetched_at) for a in adv_rows]
                append_rows_csv(data_dir / "player_advanced.csv", adv_dicts)

                # Metadata
                players_meta = [
                    {"player_id": p.player_id, "player_name": p.player_name, "team_id": p.team_id}
                    for p in player_rows
                ]
                teams_meta = [
                    {"team_id": t.team_id, "team_tricode": t.team_tricode}
                    for t in team_rows
                ]
                append_rows_csv(meta_dir / "players.csv", players_meta)
                append_rows_csv(meta_dir / "teams.csv", teams_meta)

                if args.play_by_play:
                    try:
                        pbp_raw = stats_client.fetch_play_by_play(game_id)
                        _save_raw(raw_dir / "playbyplay", f"playbyplay_{game_id}.json", pbp_raw)
                        _log_json(
                            provenance_path,
                            {
                                "endpoint": "playbyplayv2",
                                "game_id": game_id,
                                "season": season,
                                "season_type": season_type,
                                "fetched_at_utc": fetched_at,
                            },
                        )
                    except Exception as exc:
                        _log_json(
                            errors_path,
                            {
                                "game_id": game_id,
                                "season": season,
                                "season_type": season_type,
                                "error": f"playbyplay_failed: {exc}",
                            },
                        )

                done_games.add(game_id)
                progress["games"] = sorted(done_games)
                save_progress(progress_path, progress)

                logging.info("Processed game %s (%s)", game_id, source)

    # Build parquet outputs
    write_parquet(data_dir / "games.parquet", data_dir / "games.csv")
    write_parquet(data_dir / "player_boxscores.parquet", data_dir / "player_boxscores.csv")
    write_parquet(data_dir / "team_boxscores.parquet", data_dir / "team_boxscores.csv")
    write_parquet(data_dir / "player_advanced.parquet", data_dir / "player_advanced.csv")


def cmd_normalize(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    data_dir = out_dir / "data"
    write_parquet(data_dir / "games.parquet", data_dir / "games.csv")
    write_parquet(data_dir / "player_boxscores.parquet", data_dir / "player_boxscores.csv")
    write_parquet(data_dir / "team_boxscores.parquet", data_dir / "team_boxscores.csv")
    write_parquet(data_dir / "player_advanced.parquet", data_dir / "player_advanced.csv")
    logging.info("Parquet files written.")


def cmd_build_features(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    data_dir = out_dir / "data"
    features_dir = out_dir / "features"
    ensure_dir(features_dir)

    build_features(
        games_csv=data_dir / "games.csv",
        player_csv=data_dir / "player_boxscores.csv",
        team_csv=data_dir / "team_boxscores.csv",
        out_csv=features_dir / "player_features.csv",
        out_parquet=features_dir / "player_features.parquet",
    )
    logging.info("Feature tables written.")


def main() -> None:
    parser = argparse.ArgumentParser(prog="nba_etl")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning"])
    sub = parser.add_subparsers(dest="command")

    fetch = sub.add_parser("fetch")
    fetch.add_argument("--seasons", default="2022-23,2023-24", help="Comma-separated seasons")
    fetch.add_argument("--season-start", help="Start year (e.g., 2015)")
    fetch.add_argument("--season-end", help="End year (e.g., 2024)")
    fetch.add_argument("--season-type", default="Regular Season")
    fetch.add_argument("--playoffs", action="store_true")
    fetch.add_argument("--out-dir", default="nba_etl_output")
    fetch.add_argument("--cache-dir", default=str(Path.home() / ".cache" / "nba_etl"))
    fetch.add_argument("--cache-ttl", type=int, default=int(os.getenv("NBA_ETL_CACHE_TTL", "600")))
    fetch.add_argument("--cache-forever", action="store_true")
    fetch.add_argument("--date-start", help="Scoreboard fallback start date (YYYY-MM-DD)")
    fetch.add_argument("--date-end", help="Scoreboard fallback end date (YYYY-MM-DD)")
    fetch.add_argument("--resume-from-date", help="Skip scoreboard dates before this (YYYY-MM-DD)")
    fetch.add_argument("--chunk-days", type=int, default=7, help="Max scoreboard days per run (0 = no limit)")
    fetch.add_argument("--auto-loop", action="store_true", help="Keep looping chunks until date_end reached")
    fetch.add_argument(
        "--no-scoreboard-fallback",
        action="store_false",
        dest="scoreboard_fallback",
        default=True,
        help="Disable CDN schedule fallback for game IDs",
    )
    fetch.add_argument("--rate-limit", type=float, default=1.0)
    fetch.add_argument("--timeout", type=float, default=20.0)
    fetch.add_argument("--max-retries", type=int, default=3)
    fetch.add_argument("--backoff", type=float, default=0.5)
    fetch.add_argument("--play-by-play", action="store_true")
    fetch.set_defaults(func=cmd_fetch)

    normalize = sub.add_parser("normalize")
    normalize.add_argument("--out-dir", default="nba_etl_output")
    normalize.set_defaults(func=cmd_normalize)

    features = sub.add_parser("build-features")
    features.add_argument("--out-dir", default="nba_etl_output")
    features.set_defaults(func=cmd_build_features)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")
    args.func(args)


if __name__ == "__main__":
    main()
