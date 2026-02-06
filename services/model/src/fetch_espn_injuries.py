import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_PATH = Path(DATA_DIR / "injuries_today.csv")

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

TIMEOUT_SECONDS = 20


def _get_json(path: str) -> dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.get(url, timeout=TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response for {path}")
    return data


def _load_team_map() -> dict[str, str]:
    data = _get_json("teams")
    teams = (
        data.get("sports", [{}])[0]
        .get("leagues", [{}])[0]
        .get("teams", [])
    )
    out: dict[str, str] = {}
    for item in teams:
        team = item.get("team") or {}
        team_id = str(team.get("id") or "").strip()
        abbr = str(team.get("abbreviation") or "").strip().upper()
        if team_id and abbr:
            out[team_id] = abbr
    return out


def _normalize_status(s: str) -> str:
    s = str(s or "").strip().upper()
    if s.startswith("OUT"):
        return "OUT"
    if s.startswith("DOUBT"):
        return "D"
    if s.startswith("QUESTION"):
        return "Q"
    if s.startswith("PROB"):
        return "P"
    return s or "Q"


def main() -> None:
    team_map = _load_team_map()
    data = _get_json("injuries")
    injuries = data.get("injuries", []) if isinstance(data, dict) else []
    if not injuries:
        print("No injuries returned from ESPN endpoint.")
        return

    rows: list[dict[str, Any]] = []
    today = datetime.now(timezone.utc).date().isoformat()

    for team_entry in injuries:
        team_id = str(team_entry.get("id") or "").strip()
        team_abbr = team_map.get(team_id, "")
        items = team_entry.get("injuries", []) or []
        for item in items:
            athlete = item.get("athlete") or {}
            player_name = str(athlete.get("displayName") or "").strip()
            status = _normalize_status(item.get("status"))
            details = item.get("details") or {}
            reason = str(details.get("details") or details.get("type") or item.get("type") or "").strip()
            updated = str(item.get("date") or "").strip()
            rows.append(
                {
                    "league": "NBA",
                    "game_date": today,
                    "player_name": player_name,
                    "team_abbr": team_abbr,
                    "status": status,
                    "reason": reason,
                    "updated_at": updated,
                    "source": "espn",
                }
            )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["league", "game_date", "player_name", "team_abbr", "status", "reason", "updated_at", "source"]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Saved injuries rows: {len(rows)} -> {OUT_PATH}")


if __name__ == "__main__":
    main()
