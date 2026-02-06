import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

NBA_HEADERS = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "x-nba-stats-token": "true",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "x-nba-stats-origin": "stats",
    "Referer": "https://www.nba.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

SEASON = os.getenv("NBA_SEASON", "2025-26")
SEASON_TYPE = os.getenv("NBA_SEASON_TYPE", "Regular Season")
SLEEP_BETWEEN_CALLS = float(os.getenv("NBA_SLEEP_BETWEEN_CALLS", "1.6"))
MAX_RETRIES = int(os.getenv("NBA_MAX_RETRIES", "4"))
TIMEOUT_SECONDS = int(os.getenv("NBA_TIMEOUT_SECONDS", "25"))
MAX_PLAYERS = int(os.getenv("NBA_MAX_PLAYERS", "0"))
START_INDEX = int(os.getenv("NBA_START_INDEX", "0"))
NBA_PROXY = os.getenv("NBA_API_PROXY", "").strip() or None

OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "player_with_without.csv"


def nba_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=NBA_HEADERS, timeout=TIMEOUT_SECONDS, proxies={"https": NBA_PROXY} if NBA_PROXY else None)
            if resp.status_code == 200:
                return resp.json()
            msg = f"HTTP {resp.status_code}: {resp.text[:120]}"
        except Exception as exc:
            msg = str(exc)
        wait = min(60, attempt * 8)
        print(f"Retry {attempt}/{MAX_RETRIES} -> {msg} | sleep {wait}s")
        time.sleep(wait)
    raise RuntimeError("NBA stats request failed")


def parse_result_sets(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rs = data.get("resultSets") or data.get("resultSet")
    if rs is None:
        return []
    if isinstance(rs, dict):
        rs = [rs]
    out = []
    for r in rs:
        headers = r.get("headers") or []
        rows = r.get("rowSet") or []
        for row in rows:
            out.append({headers[i]: row[i] for i in range(len(headers))})
    return out


def status_from_row(row: Dict[str, Any]) -> Optional[str]:
    for key in ("GROUP_SET", "COURT_STATUS", "ON_OFF", "ON_OFF_COURT", "GROUP_NAME"):
        val = row.get(key)
        if not val:
            continue
        v = str(val).strip().lower()
        if v in ("on court", "oncourt", "on", "with", "on-court"):
            return "with"
        if v in ("off court", "offcourt", "off", "without", "off-court"):
            return "without"
    return None


def teammate_from_row(row: Dict[str, Any]) -> Optional[str]:
    for key in ("GROUP_VALUE", "TEAMMATE", "TEAMMATE_NAME", "GROUP_NAME"):
        val = row.get(key)
        if val:
            return str(val).strip()
    return None


def num(row: Dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except Exception:
        return 0.0


def fetch_players() -> List[Dict[str, Any]]:
    url = "https://stats.nba.com/stats/commonallplayers"
    params = {
        "LeagueID": "00",
        "Season": SEASON,
        "IsOnlyCurrentSeason": "1",
    }
    data = nba_get(url, params)
    rs = data.get("resultSets") or data.get("resultSet") or []
    if isinstance(rs, dict):
        rs = [rs]
    players = []
    for r in rs:
        headers = r.get("headers") or []
        rows = r.get("rowSet") or []
        idx_id = headers.index("PERSON_ID") if "PERSON_ID" in headers else None
        idx_name = headers.index("DISPLAY_FIRST_LAST") if "DISPLAY_FIRST_LAST" in headers else None
        idx_team = headers.index("TEAM_ABBREVIATION") if "TEAM_ABBREVIATION" in headers else None
        if idx_id is None or idx_name is None:
            continue
        for row in rows:
            players.append(
                {
                    "id": row[idx_id],
                    "name": row[idx_name],
                    "team": row[idx_team] if idx_team is not None else "",
                }
            )
    return players


def fetch_with_without(player_id: int) -> List[Dict[str, Any]]:
    url = "https://stats.nba.com/stats/playerwithwithout"
    params = {
        "PlayerID": player_id,
        "Season": SEASON,
        "SeasonType": SEASON_TYPE,
    }
    data = nba_get(url, params)
    return parse_result_sets(data)


def main() -> None:
    players = fetch_players()
    if START_INDEX:
        players = players[START_INDEX:]
    if MAX_PLAYERS > 0:
        players = players[:MAX_PLAYERS]

    updated_at = datetime.utcnow().strftime("%Y-%m-%d")
    out_lines = [
        "season,player_name,team_abbr,teammate_name,with_min,with_pts,with_fga,without_min,without_pts,without_fga,updated_at"
    ]

    for p in players:
        pid = int(p["id"])
        name = str(p["name"])
        team = str(p.get("team") or "")
        try:
            rows = fetch_with_without(pid)
        except Exception as exc:
            print(f"Skip {name}: {exc}")
            continue

        acc: Dict[str, Dict[str, float]] = {}
        for row in rows:
            teammate = teammate_from_row(row)
            if not teammate or teammate == name:
                continue
            status = status_from_row(row)
            if status not in ("with", "without"):
                continue
            if teammate not in acc:
                acc[teammate] = {
                    "with_min": 0.0, "with_pts": 0.0, "with_fga": 0.0,
                    "without_min": 0.0, "without_pts": 0.0, "without_fga": 0.0,
                }
            if status == "with":
                acc[teammate]["with_min"] = num(row, "MIN")
                acc[teammate]["with_pts"] = num(row, "PTS")
                acc[teammate]["with_fga"] = num(row, "FGA")
            else:
                acc[teammate]["without_min"] = num(row, "MIN")
                acc[teammate]["without_pts"] = num(row, "PTS")
                acc[teammate]["without_fga"] = num(row, "FGA")

        for teammate, v in acc.items():
            out_lines.append(
                ",".join(
                    [
                        SEASON,
                        name,
                        team,
                        teammate.replace(",", " "),
                        f"{v['with_min']:.3f}",
                        f"{v['with_pts']:.3f}",
                        f"{v['with_fga']:.3f}",
                        f"{v['without_min']:.3f}",
                        f"{v['without_pts']:.3f}",
                        f"{v['without_fga']:.3f}",
                        updated_at,
                    ]
                )
            )
        time.sleep(SLEEP_BETWEEN_CALLS)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("\n".join(out_lines), encoding="utf-8")
    print("Saved:", OUT_PATH)


if __name__ == "__main__":
    main()
