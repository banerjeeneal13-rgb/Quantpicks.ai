import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import requests


class AccessDeniedError(RuntimeError):
    pass


class RateLimitedError(RuntimeError):
    pass


class InvalidResponseError(RuntimeError):
    pass


@dataclass
class ZoneClientConfig:
    base_url: str = "https://stats.nba.com/stats"
    cache_ttl_seconds: int = 86400
    cache_dir: Path = Path(__file__).resolve().parents[1] / "data" / "cache" / "nba_zone"
    rate_limit_per_sec: float = 1.0
    deny_cooldown_seconds: int = 3600
    max_empty_responses: int = 2


class ZoneClient:
    def __init__(
        self,
        config: ZoneClientConfig,
        http_get: Optional[Callable[..., requests.Response]] = None,
    ) -> None:
        self.config = config
        self.http_get = http_get or requests.get
        self._last_request_ts = 0.0
        self._empty_count = 0
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)

    def _rate_limit(self) -> None:
        if self.config.rate_limit_per_sec <= 0:
            return
        min_interval = 1.0 / self.config.rate_limit_per_sec
        now = time.time()
        wait = min_interval - (now - self._last_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    def _cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        payload = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _cache_paths(self, key: str) -> Tuple[Path, Path]:
        data_path = self.config.cache_dir / f"{key}.json"
        meta_path = self.config.cache_dir / f"{key}.meta.json"
        return data_path, meta_path

    def _load_cache(self, key: str) -> Optional[Dict[str, Any]]:
        data_path, meta_path = self._cache_paths(key)
        if not data_path.exists() or not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if time.time() - meta.get("saved_at", 0) > self.config.cache_ttl_seconds:
                return None
            return json.loads(data_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save_cache(self, key: str, data: Dict[str, Any]) -> None:
        data_path, meta_path = self._cache_paths(key)
        data_path.write_text(json.dumps(data), encoding="utf-8")
        meta_path.write_text(json.dumps({"saved_at": time.time()}), encoding="utf-8")

    def _deny_marker_path(self) -> Path:
        return self.config.cache_dir / "access_denied.json"

    def _check_access_denied(self) -> None:
        marker = self._deny_marker_path()
        if not marker.exists():
            return
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
            until_ts = float(payload.get("until_ts", 0))
            if time.time() < until_ts:
                raise AccessDeniedError(payload.get("message", "Access denied."))
        except AccessDeniedError:
            raise
        except Exception:
            return

    def _mark_access_denied(self, message: str) -> None:
        marker = self._deny_marker_path()
        payload = {"until_ts": time.time() + self.config.deny_cooldown_seconds, "message": message}
        marker.write_text(json.dumps(payload), encoding="utf-8")

    def _validate_payload(self, data: Dict[str, Any]) -> None:
        result_sets = data.get("resultSets") or data.get("resultSet")
        if result_sets is None:
            self._empty_count += 1
            raise InvalidResponseError("Missing resultSets.")
        if isinstance(result_sets, list) and len(result_sets) == 0:
            self._empty_count += 1
            raise InvalidResponseError("Empty resultSets.")
        if self._empty_count > self.config.max_empty_responses:
            raise InvalidResponseError("Repeated empty responses.")

    def request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._check_access_denied()
        key = self._cache_key(endpoint, params)
        cached = self._load_cache(key)
        if cached is not None:
            return {"status": "cache_hit", "data": cached}

        self._rate_limit()
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {"Accept": "application/json", "User-Agent": "nba_zone_client/1.0"}
        resp = self.http_get(url, params=params, headers=headers, timeout=20)

        if resp.status_code == 403:
            msg = "NBA stats endpoint access denied. Escalate to licensing or approved access."
            self._mark_access_denied(msg)
            raise AccessDeniedError(msg)
        if resp.status_code == 429:
            time.sleep(5)
            resp = self.http_get(url, params=params, headers=headers, timeout=20)
            if resp.status_code == 429:
                raise RateLimitedError("Rate limited by NBA stats endpoint. Escalate.")
        if resp.status_code != 200:
            raise InvalidResponseError(f"Unexpected status {resp.status_code}")

        data = resp.json()
        self._validate_payload(data)
        self._save_cache(key, data)
        return {"status": "fetched", "data": data}


def zone_endpoint_for_entity(entity: str) -> str:
    if entity == "team":
        return "leaguedashteamshotlocations"
    return "leaguedashplayerptshot"


def default_params(season: str, season_type: str) -> Dict[str, Any]:
    return {
        "Season": season,
        "SeasonType": season_type,
        "PerMode": "PerGame",
        "LeagueID": "00",
        "MeasureType": "Opponent",
        "PlusMinus": "N",
        "PaceAdjust": "N",
        "Rank": "N",
        "Outcome": "",
        "Location": "",
        "Month": "0",
        "SeasonSegment": "",
        "DateFrom": "",
        "DateTo": "",
        "OpponentTeamID": "0",
        "VsConference": "",
        "VsDivision": "",
        "GameSegment": "",
        "Period": "0",
        "ShotClockRange": "",
        "LastNGames": "0",
    }


def normalize_tables(payload: Dict[str, Any]) -> Dict[str, Any]:
    result_sets = payload.get("resultSets") or payload.get("resultSet") or []
    if isinstance(result_sets, dict):
        result_sets = [result_sets]
    tables = []
    for rs in result_sets:
        headers = rs.get("headers") or []
        rows = rs.get("rowSet") or []
        tables.append(
            {
                "name": rs.get("name") or "result_set",
                "columns": headers,
                "rows": rows,
            }
        )
    return {"metadata": payload.get("parameters", {}), "tables": tables}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NBA shot zone stats (compliant).")
    parser.add_argument("--season", required=True, help="Season e.g., 2025-26")
    parser.add_argument("--season_type", default="Regular Season", help="Regular Season or Playoffs")
    parser.add_argument("--entity", choices=["player", "team"], default="player")
    parser.add_argument("--out", default="", help="Output JSON path")
    args = parser.parse_args()

    cfg = ZoneClientConfig()
    client = ZoneClient(cfg)
    endpoint = zone_endpoint_for_entity(args.entity)
    params = default_params(args.season, args.season_type)
    try:
        res = client.request(endpoint, params)
        normalized = normalize_tables(res["data"])
        out_path = args.out or str(
            Path(__file__).resolve().parents[1]
            / "data"
            / f"zone_stats_{args.entity}_{args.season.replace('-', '')}.json"
        )
        Path(out_path).write_text(json.dumps(normalized), encoding="utf-8")
        print("Saved:", out_path)
    except (AccessDeniedError, RateLimitedError, InvalidResponseError) as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
