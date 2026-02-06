import json
import os
from dataclasses import asdict
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
ETL_DIR = Path(os.getenv("NBA_ETL_OUTPUT_DIR") or (REPO_ROOT / "nba_etl_pkg" / "nba_etl_output"))
RAW_DIR = ETL_DIR / "raw"
DATA_DIR = ETL_DIR / "data"
META_DIR = ETL_DIR / "metadata"

# Allow importing nba_etl package without install
import sys

sys.path.insert(0, str(REPO_ROOT / "nba_etl_pkg"))

from nba_etl.transform.normalize import (  # noqa: E402
    compute_advanced,
    normalize_cdn_boxscore,
    normalize_stats_boxscore,
)
from nba_etl.storage import ensure_dir, write_parquet  # noqa: E402


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _game_id_from_path(path: Path) -> str:
    name = path.stem.replace("boxscore_", "")
    return str(name).zfill(10)


def main() -> None:
    if not RAW_DIR.exists():
        raise SystemExit(f"Raw dir missing: {RAW_DIR}")
    ensure_dir(DATA_DIR)
    ensure_dir(META_DIR)

    def _season_from_game_id(gid: str) -> str:
        code = gid[3:5]
        try:
            year = 2000 + int(code)
        except Exception:
            year = 2000
        return f"{year}-{str(year + 1)[-2:]}"

    def _season_type_from_game_id(gid: str) -> str:
        if gid.startswith("004"):
            return "Playoffs"
        return "Regular Season"

    cdn_dir = RAW_DIR / "boxscore_cdn"
    stats_dir = RAW_DIR / "boxscore_stats"

    cdn_files = { _game_id_from_path(p): p for p in cdn_dir.glob("boxscore_*.json") } if cdn_dir.exists() else {}
    stats_files = { _game_id_from_path(p): p for p in stats_dir.glob("boxscore_*.json") } if stats_dir.exists() else {}
    all_game_ids = sorted(set(cdn_files) | set(stats_files))

    game_rows = []
    player_rows = []
    team_rows = []
    adv_rows = []
    players_meta = []
    teams_meta = []

    for game_id in all_game_ids:
        season = _season_from_game_id(str(game_id))
        season_type = _season_type_from_game_id(str(game_id))
        if game_id in cdn_files:
            raw = _load_json(cdn_files[game_id])
            game_row, player_box, team_box = normalize_cdn_boxscore(raw, season, season_type)
        else:
            raw = _load_json(stats_files[game_id])
            game_row, player_box, team_box = normalize_stats_boxscore(raw, season, season_type)

        game_rows.append(asdict(game_row))
        player_rows.extend(asdict(p) for p in player_box)
        team_rows.extend(asdict(t) for t in team_box)

        adv_rows.extend(asdict(a) for a in compute_advanced(player_box, team_box, season, season_type))
        players_meta.extend(
            {"player_id": p.player_id, "player_name": p.player_name, "team_id": p.team_id} for p in player_box
        )
        teams_meta.extend(
            {"team_id": t.team_id, "team_tricode": t.team_tricode} for t in team_box
        )

    # Overwrite outputs with rebuilt data
    pd.DataFrame(game_rows).to_csv(DATA_DIR / "games.csv", index=False)
    pd.DataFrame(player_rows).to_csv(DATA_DIR / "player_boxscores.csv", index=False)
    pd.DataFrame(team_rows).to_csv(DATA_DIR / "team_boxscores.csv", index=False)
    pd.DataFrame(adv_rows).to_csv(DATA_DIR / "player_advanced.csv", index=False)
    pd.DataFrame(players_meta).drop_duplicates().to_csv(META_DIR / "players.csv", index=False)
    pd.DataFrame(teams_meta).drop_duplicates().to_csv(META_DIR / "teams.csv", index=False)

    # Refresh parquet
    write_parquet(DATA_DIR / "games.parquet", DATA_DIR / "games.csv")
    write_parquet(DATA_DIR / "player_boxscores.parquet", DATA_DIR / "player_boxscores.csv")
    write_parquet(DATA_DIR / "team_boxscores.parquet", DATA_DIR / "team_boxscores.csv")
    write_parquet(DATA_DIR / "player_advanced.parquet", DATA_DIR / "player_advanced.csv")

    print(f"Rebuilt {len(game_rows)} games, {len(player_rows)} player rows, {len(team_rows)} team rows.")


if __name__ == "__main__":
    main()
