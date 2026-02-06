"""Config loader for prop_tracker."""
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    odds_provider: str
    odds_api_key: str | None
    regions: list[str]
    bookmakers: list[str] | None
    sport: str
    stats_provider: str
    api_sports_key: str | None
    request_timeout_s: int
    max_retries: int
    backoff_base_s: float


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def load_settings() -> Settings:
    odds_provider = os.getenv("ODDS_PROVIDER", "theoddsapi").strip().lower()
    odds_api_key = os.getenv("ODDS_API_KEY")
    regions = _split_csv(os.getenv("REGIONS", "us"))
    bookmakers = _split_csv(os.getenv("BOOKMAKERS", ""))
    sport = os.getenv("SPORT", "basketball_nba").strip()
    stats_provider = os.getenv("STATS_PROVIDER", "csv").strip().lower()
    api_sports_key = os.getenv("API_SPORTS_KEY") or os.getenv("NBA_API_SPORTS_KEY")

    request_timeout_s = int(os.getenv("REQUEST_TIMEOUT_S", "20"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    backoff_base_s = float(os.getenv("BACKOFF_BASE_S", "0.5"))

    return Settings(
        odds_provider=odds_provider,
        odds_api_key=odds_api_key,
        regions=regions,
        bookmakers=bookmakers or None,
        sport=sport,
        stats_provider=stats_provider,
        api_sports_key=api_sports_key,
        request_timeout_s=request_timeout_s,
        max_retries=max_retries,
        backoff_base_s=backoff_base_s,
    )
