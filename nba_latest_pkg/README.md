# nba_latest

CLI tool to fetch the **most recent completed NBA game** from **first‑party NBA data sources** (no third‑party APIs, no HTML scraping).

## Endpoints (first‑party only)
**Primary (nba.com CDN, JSON):**
- `https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_YYYYMMDD.json`
- `https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard.json` (fallback within primary)

**Fallback (stats.nba.com, JSON):**
- `https://stats.nba.com/stats/scoreboardv2?GameDate=MM/DD/YYYY&LeagueID=00&DayOffset=0`

### How I found these (DevTools steps)
1. Open `https://www.nba.com/games` in your browser.
2. Open **DevTools → Network**.
3. Filter by `scoreboard` or `liveData`.
4. Click the JSON request to `cdn.nba.com/static/json/liveData/scoreboard/...`.
5. Right‑click → **Copy URL** to confirm the endpoint.
6. If CDN is unavailable, check requests to `stats.nba.com` (the scoreboardv2 endpoint is used by NBA’s own site).

## Why headers matter
`stats.nba.com` often blocks or throttles requests without realistic browser headers. The tool sends:
`User-Agent`, `Accept`, `Referer`, `Origin`, `Accept-Language`, and `x-nba-stats-*` headers to mimic nba.com traffic.

## Usage
Install dependencies:
```
pip install -e .
```

Run the CLI:
```
nba_latest --format json
```

Options:
```
--lookback-days N    (default 14)
--timezone TZ        (default America/New_York)
--format json|table
--cache-dir PATH
--log-level debug|info|warning
```

Example (table output):
```
nba_latest --format table --lookback-days 7
```

## Caching & Rate Limiting
- Responses are cached by URL + params with a **10‑minute TTL** (default).
- If the network fails, the tool will return the **most recent cached response** with a warning.
- Requests are **rate‑limited to 1 request/second**.

## Tests
```
pytest
```

Tests include:
- no games found in lookback window
- schema changes (missing fields)
- in‑progress games filtered out
- caching & TTL behavior

## Definition of Done
```
nba_latest --format json
```
prints the most recent completed NBA game in a normalized schema, and `pytest` passes.
