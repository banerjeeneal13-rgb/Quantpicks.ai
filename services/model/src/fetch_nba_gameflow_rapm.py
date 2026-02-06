import json
import os
from pathlib import Path
from typing import Dict, List

import requests

BASE_URL = os.getenv("NBA_RAPM_BASE_URL", "https://www.nbagameflow.com/api").rstrip("/")
SEASON = os.getenv("NBA_RAPM_SEASON", "2025")
OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "rapm_gameflow.csv"


def normalize_name(name: str) -> str:
    return str(name or "").strip()


def fetch(endpoint: str) -> List[Dict]:
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        data = data.get("data", []) or data.get("results", []) or []
    return list(data)


def extract_fields(row: Dict) -> Dict:
    # Attempt to map common RAPM keys
    name = (
        row.get("name")
        or row.get("player_name")
        or row.get("player")
        or row.get("Player")
        or row.get("PLAYER_NAME")
    )
    rapm = row.get("rapm") or row.get("RAPM") or row.get("total") or row.get("TOTAL")
    orapm = row.get("orapm") or row.get("ORAPM") or row.get("offense") or row.get("OFF")
    drapm = row.get("drapm") or row.get("DRAPM") or row.get("defense") or row.get("DEF")
    rank = row.get("rank") or row.get("RANK")
    return {
        "player_name": normalize_name(name),
        "season": season_label(SEASON),
        "rapm": to_float(rapm),
        "orapm": to_float(orapm),
        "drapm": to_float(drapm),
        "rapm_rank": to_float(rank),
        "raw_json": json.dumps(row, separators=(",", ":"), ensure_ascii=True),
    }


def to_float(val):
    try:
        if val is None or val == "":
            return ""
        return float(val)
    except Exception:
        return ""


def season_label(season_year: str) -> str:
    try:
        y = int(season_year)
        return f"{y}-{str(y + 1)[-2:]}"
    except Exception:
        return str(season_year)


def main() -> None:
    rows = fetch(f"rapm_1?season={SEASON}")
    out = [extract_fields(r) for r in rows if extract_fields(r).get("player_name")]
    if not out:
        raise RuntimeError("No RAPM rows returned.")

    header = ["player_name", "season", "rapm", "orapm", "drapm", "rapm_rank", "raw_json"]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        f.write(",".join(header) + "\n")
        for r in out:
            f.write(
                f"{r['player_name'].replace(',', ' ')},{r['season']},{r['rapm']},{r['orapm']},{r['drapm']},{r['rapm_rank']},\"{r['raw_json'].replace('\"','\"\"')}\"\n"
            )
    print("Saved:", OUT_PATH)


if __name__ == "__main__":
    main()
