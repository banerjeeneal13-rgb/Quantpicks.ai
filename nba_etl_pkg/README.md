# NBA ETL (First-Party Only)

This pipeline collects NBA box score data (player + team), optional advanced metrics,
and ML-ready features using only first-party NBA sources (nba.com, cdn.nba.com,
stats.nba.com).

No third-party sports APIs. No HTML scraping unless a JSON alternative is unavailable.

## Endpoint discovery (DevTools)
1. Open https://www.nba.com/games
2. DevTools -> Network -> filter for schedule, boxscore, stats
3. You should see JSON requests to:
   - https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json
   - https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json
   - https://stats.nba.com/stats/leaguegamelog
   - https://stats.nba.com/stats/boxscoretraditionalv2
   - https://stats.nba.com/stats/boxscoreadvancedv2
   - https://stats.nba.com/stats/playbyplayv2 (optional)

## Why headers matter
stats.nba.com blocks non-browser clients unless you send realistic headers. This tool sets:
- User-Agent, Accept, Referer, Origin, Accept-Language
- x-nba-stats-token, x-nba-stats-origin

## Install
```
cd nba_etl_pkg
pip install -e .
```

## Fetch data
Regular season:
```
nba_etl fetch --seasons 2022-23,2023-24 --season-type "Regular Season"
```

Regular + playoffs:
```
nba_etl fetch --seasons 2022-23,2023-24 --playoffs
```

With play-by-play:
```
nba_etl fetch --seasons 2022-23 --play-by-play
```

Cache forever:
```
nba_etl fetch --seasons 2022-23 --cache-forever
```

Disable schedule fallback (if you only want stats.nba.com leaguegamelog):
```
nba_etl fetch --seasons 2022-23 --no-scoreboard-fallback
```

Schedule fallback date bounds:
```
nba_etl fetch --seasons 2025-26 --date-start 2025-10-01 --date-end 2026-01-31
```

Resume from a specific date and limit per run:
```
nba_etl fetch --seasons 2025-26 --date-start 2025-10-01 --date-end 2026-01-31 --resume-from-date 2026-01-01 --chunk-days 7
```

Auto-loop chunks until date_end:
```
nba_etl fetch --seasons 2025-26 --date-start 2025-10-01 --date-end 2026-01-31 --chunk-days 7 --auto-loop
```

Normalize parquet:
```
nba_etl normalize --out-dir nba_etl_output
```

Build features:
```
nba_etl build-features --out-dir nba_etl_output
```

## Output files
`nba_etl_output/data/`
- games.csv + games.parquet
- player_boxscores.csv + player_boxscores.parquet
- team_boxscores.csv + team_boxscores.parquet
- player_advanced.csv + player_advanced.parquet

`nba_etl_output/metadata/`
- players.csv, teams.csv
- season_calendar.csv
- endpoint_provenance.jsonl
- fetch_errors.jsonl

`nba_etl_output/raw/`
- raw JSON per endpoint and game (safe reprocessing)

`nba_etl_output/features/`
- player_features.csv + player_features.parquet

## Provenance
Each row includes:
- season, game_id, player_id (where applicable)
- source (cdn or stats)
- fetched_at_utc (timestamp of data pull)

## Rate limiting and caching
- Default <= 1 request/sec
- Disk cache by URL+params (TTL default 10 minutes)
- Use --cache-forever to re-use cached data without re-fetching

## Tests
```
python -m pytest
```
All tests use fixture JSON (no live network).

## Notes
- Advanced metrics are pulled from stats.nba.com when available.
- If advanced endpoint fails, core metrics are computed (TS%, eFG%, USG%, pace, possessions).
- Feature building avoids target leakage by using shifted rolling windows.
