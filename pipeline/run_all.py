import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "services" / "model" / "data"
OUT_DIR = ROOT / "pipeline" / "output"


def run(cmd, cwd=None):
    print("RUN:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=cwd)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) NBA official logs / boxscores (existing ESPN fallback in repo)
    run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"), "services/model/src/fetch_espn_boxscores.py"], cwd=ROOT)
    run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"), "services/model/src/fetch_espn_injuries.py"], cwd=ROOT)

    # 2) Zone stats (compliant client; may fail closed)
    season = os.getenv("NBA_SEASON", "2025-26")
    try:
        run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"),
             "services/model/src/fetch_zone_stats.py",
             "--season", season,
             "--season_type", "Regular Season",
             "--entity", "team"], cwd=ROOT)
    except Exception as exc:
        print("Zone stats unavailable:", exc)

    # 2b) RAPM advanced stats (public API)
    try:
        run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"),
             "services/model/src/fetch_nba_gameflow_rapm.py"], cwd=ROOT)
    except Exception as exc:
        print("RAPM stats unavailable:", exc)

    # 3) Build training dataset + model + cache
    run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"), "services/model/src/build_points_dataset.py"], cwd=ROOT)
    run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"), "services/model/src/train_points_model.py"], cwd=ROOT)
    run([str(ROOT / "services" / "model" / ".venv" / "Scripts" / "python.exe"), "services/model/src/build_points_feature_cache.py"], cwd=ROOT)

    # 4) Emit simple coverage report
    report = {
        "has_boxscores": (DATA_DIR / "nba_player_logs_points_all.csv").exists(),
        "has_injuries": (DATA_DIR / "injuries_today.csv").exists(),
        "has_zone_stats": any(DATA_DIR.glob("zone_stats_team_*.json")),
        "has_feature_cache": (DATA_DIR / "points_feature_cache.csv").exists(),
    }
    (OUT_DIR / "coverage_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("Wrote:", OUT_DIR / "coverage_report.json")


if __name__ == "__main__":
    main()
