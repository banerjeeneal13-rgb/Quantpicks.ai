NBA Zone Stats Client Runbook

Purpose
- Retrieve NBA shot zone data via official stats.nba.com endpoints.
- Enforce compliance: rate limits, cache, fail-closed on access denial.

Operational Guardrails
- Sustained request rate: <= 1 req/sec.
- Cache TTL: 24 hours by default.
- Access denied cooldown: 60 minutes.

Commands
- Fetch player zone stats:
  - `python services/model/src/fetch_zone_stats.py --season 2025-26 --season_type "Regular Season" --entity player`
- Fetch team zone stats:
  - `python services/model/src/fetch_zone_stats.py --season 2025-26 --season_type "Regular Season" --entity team`

Caching
- Cache directory: `services/model/data/cache/nba_zone/`
- Cache key: sha256(endpoint + params)
- TTL: 24 hours (configurable)

Failure Modes
1) Access denied (403)
   - Client stops requests for 60 minutes.
   - Escalate to NBA data licensing or approved provider.

2) Rate limit (429)
   - Client backs off and retries once.
   - If 429 persists, stop and escalate.

3) Empty or malformed payloads
   - Client stops after repeated invalid payloads.
   - Escalate to provider or adjust endpoint parameters.

Monitoring
- Watch for `access_denied.json` marker in cache directory.
- Log status of cache hits/misses during scheduled jobs.

Escalation Path
- Request formal API access or licensing.
- Switch to a licensed data provider (Sportradar, Stats Perform).
- Document denial timestamps and headers (if any).
