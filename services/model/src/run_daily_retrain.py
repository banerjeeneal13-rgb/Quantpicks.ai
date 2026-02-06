import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _run(cmd: list[str], label: str, allow_fail: bool = False, cwd: Path | None = None) -> None:
    print(f"\n== {label} ==")
    result = subprocess.run(cmd, check=False, cwd=cwd)
    if result.returncode != 0 and not allow_fail:
        raise RuntimeError(f"{label} failed with code {result.returncode}")


def main() -> None:
    _load_dotenv()

    # Default a larger "recent window" unless caller overrides.
    os.environ.setdefault("API_SPORTS_SEASONS", "2024")
    os.environ.setdefault("API_SPORTS_DAYS_BACK", "120")
    os.environ.setdefault("API_SPORTS_MAX_GAMES", "200")
    os.environ.setdefault("API_SPORTS_SLEEP", "6.5")  # API-Sports free plan ~10 req/min

    base = Path(__file__).resolve().parent
    fetch_script = base / "fetch_api_sports_player_logs.py"
    ingest_etl_script = base / "ingest_nba_etl_boxscores.py"
    repo_root = Path(__file__).resolve().parents[3]
    etl_pkg_dir = repo_root / "nba_etl_pkg"

    if os.getenv("API_SPORTS_KEY") or os.getenv("NBA_API_SPORTS_KEY"):
        _run([sys.executable, str(fetch_script)], "Fetch API-Sports player logs", allow_fail=True)
    else:
        print("API_SPORTS_KEY not set; skipping API-Sports log pull.")

    if etl_pkg_dir.exists():
        etl_out_dir = Path(os.getenv("NBA_ETL_OUTPUT_DIR") or (etl_pkg_dir / "nba_etl_output"))
        os.environ.setdefault("NBA_ETL_OUTPUT_DIR", str(etl_out_dir))

        seasons = os.getenv("NBA_ETL_SEASONS", "2025-26")
        season_type = os.getenv("NBA_ETL_SEASON_TYPE", "Regular Season")
        date_start = os.getenv("NBA_ETL_DATE_START", "2025-10-01")
        date_end = os.getenv("NBA_ETL_DATE_END", datetime.now(timezone.utc).date().isoformat())
        chunk_days = os.getenv("NBA_ETL_CHUNK_DAYS", "7")
        timeout = os.getenv("NBA_ETL_TIMEOUT", "30")
        max_retries = os.getenv("NBA_ETL_MAX_RETRIES", "2")
        backoff = os.getenv("NBA_ETL_BACKOFF", "1.0")
        cache_forever = os.getenv("NBA_ETL_CACHE_FOREVER", "1") not in ("0", "false", "False")
        auto_loop = os.getenv("NBA_ETL_AUTO_LOOP", "0") in ("1", "true", "True")

        fetch_cmd = [
            sys.executable,
            "-m",
            "nba_etl.cli",
            "fetch",
            "--seasons",
            seasons,
            "--season-type",
            season_type,
            "--out-dir",
            str(etl_out_dir),
            "--date-start",
            date_start,
            "--date-end",
            date_end,
            "--chunk-days",
            chunk_days,
            "--timeout",
            timeout,
            "--max-retries",
            max_retries,
            "--backoff",
            backoff,
        ]
        if cache_forever:
            fetch_cmd.append("--cache-forever")
        if auto_loop:
            fetch_cmd.append("--auto-loop")

        _run(fetch_cmd, "Fetch NBA ETL boxscores", allow_fail=True, cwd=etl_pkg_dir)
        _run([sys.executable, str(ingest_etl_script)], "Ingest NBA ETL boxscores", allow_fail=True)
    else:
        print("nba_etl_pkg not found; skipping ETL fetch/ingest.")

    _run([sys.executable, str(base / "build_points_dataset.py")], "Build points dataset")
    _run([sys.executable, str(base / "train_points_model.py")], "Train points model")
    _run([sys.executable, str(base / "build_points_feature_cache.py")], "Build points feature cache")


if __name__ == "__main__":
    main()
