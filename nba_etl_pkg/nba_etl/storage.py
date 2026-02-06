from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Any

import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_progress(progress_path: Path) -> dict[str, list[str]]:
    if not progress_path.exists():
        return {}
    try:
        return json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_progress(progress_path: Path, progress: dict[str, list[str]]) -> None:
    ensure_dir(progress_path.parent)
    progress_path.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def append_rows_csv(path: Path, rows: Iterable[Any]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    df = pd.DataFrame([r.__dict__ if hasattr(r, "__dict__") else r for r in rows])
    header = not path.exists()
    df.to_csv(path, mode="a", header=header, index=False)
    return len(df)


def write_parquet(path: Path, csv_path: Path) -> None:
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    df.to_parquet(path, index=False)
