import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

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
SLEEP_BETWEEN_CALLS = float(os.getenv("NBA_SLEEP_BETWEEN_CALLS", "1.2"))
MAX_RETRIES = int(os.getenv("NBA_MAX_RETRIES", "4"))
TIMEOUT_SECONDS = int(os.getenv("NBA_TIMEOUT_SECONDS", "25"))

OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "team_defense_zones.csv"


def nba_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=NBA_HEADERS, timeout=TIMEOUT_SECONDS)
            if resp.status_code == 200:
                return resp.json()
            msg = f"HTTP {resp.status_code}: {resp.text[:120]}"
        except Exception as exc:
            msg = str(exc)
        wait = min(60, attempt * 8)
        print(f"Retry {attempt}/{MAX_RETRIES} -> {msg} | sleep {wait}s")
        time.sleep(wait)
    raise RuntimeError("NBA stats request failed")


def resultsets_to_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rs = data.get("resultSets") or data.get("resultSet")
    if rs is None:
        return []
    if isinstance(rs, dict):
        rs = [rs]
    shot_rs = None
    for r in rs:
        headers = r.get("headers") or []
        if "SHOT_ZONE_BASIC" in headers or r.get("name") == "ShotLocationTeamDashboard":
            shot_rs = r
            break
    if shot_rs is None:
        shot_rs = rs[0]
    headers = shot_rs.get("headers") or []
    rows = shot_rs.get("rowSet") or []
    out = []
    for row in rows:
        out.append({headers[i]: row[i] for i in range(len(headers))})
    return out


def zone_bucket(zone_basic: str) -> str | None:
    if not zone_basic:
        return None
    zone = str(zone_basic).strip()
    if zone == "Restricted Area":
        return "rim"
    if zone == "In The Paint (Non-RA)":
        return "paint"
    if zone == "Mid-Range":
        return "mid"
    if zone in ("Corner 3", "Left Corner 3", "Right Corner 3", "Above the Break 3"):
        return "three"
    return None


def main() -> None:
    url = "https://stats.nba.com/stats/leaguedashteamshotlocations"
    params = {
        "Season": SEASON,
        "SeasonType": SEASON_TYPE,
        "MeasureType": "Opponent",
        "PerMode": "PerGame",
        "LeagueID": "00",
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
    data = nba_get(url, params)
    rows = resultsets_to_rows(data)
    if not rows:
        raise RuntimeError("No shot location rows returned")

    acc: Dict[str, Dict[str, float]] = {}
    for r in rows:
        team = r.get("TEAM_ABBREVIATION") or r.get("TEAM_ABBREVATION")
        zone = zone_bucket(r.get("SHOT_ZONE_BASIC"))
        if not team or not zone:
            continue
        fga = float(r.get("FGA") or 0)
        fgm = float(r.get("FGM") or 0)
        if team not in acc:
            acc[team] = {
                "rim_fga": 0.0, "rim_fgm": 0.0,
                "paint_fga": 0.0, "paint_fgm": 0.0,
                "mid_fga": 0.0, "mid_fgm": 0.0,
                "three_fga": 0.0, "three_fgm": 0.0,
            }
        acc[team][f"{zone}_fga"] += fga
        acc[team][f"{zone}_fgm"] += fgm

    out_lines = [
        "season,team_abbr,rim_fga,rim_fg_pct_allowed,paint_fga,paint_fg_pct_allowed,mid_fga,mid_fg_pct_allowed,three_fga,three_fg_pct_allowed,updated_at"
    ]
    updated_at = datetime.utcnow().strftime("%Y-%m-%d")
    for team, v in sorted(acc.items()):
        def pct(fgm: float, fga: float) -> str:
            return f"{(fgm / fga * 100):.3f}" if fga > 0 else ""

        out_lines.append(
            ",".join(
                [
                    SEASON,
                    team,
                    f"{v['rim_fga']:.3f}",
                    pct(v["rim_fgm"], v["rim_fga"]),
                    f"{v['paint_fga']:.3f}",
                    pct(v["paint_fgm"], v["paint_fga"]),
                    f"{v['mid_fga']:.3f}",
                    pct(v["mid_fgm"], v["mid_fga"]),
                    f"{v['three_fga']:.3f}",
                    pct(v["three_fgm"], v["three_fga"]),
                    updated_at,
                ]
            )
        )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("\n".join(out_lines), encoding="utf-8")
    time.sleep(SLEEP_BETWEEN_CALLS)
    print("Saved:", OUT_PATH)


if __name__ == "__main__":
    main()
