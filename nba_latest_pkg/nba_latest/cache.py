from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional


def _hash_key(url: str, params: dict[str, Any]) -> str:
    payload = json.dumps({"url": url, "params": params}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def cache_path(cache_dir: Path, url: str, params: dict[str, Any]) -> Path:
    return cache_dir / f"{_hash_key(url, params)}.json"


def read_cache(cache_file: Path, ttl_seconds: int) -> tuple[Optional[dict[str, Any]], bool]:
    if not cache_file.exists():
        return None, False
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return None, False
    fetched_at = data.get("fetched_at", 0)
    if not isinstance(fetched_at, (int, float)):
        return None, False
    age = time.time() - float(fetched_at)
    return data.get("data"), age <= ttl_seconds


def write_cache(cache_file: Path, data: dict[str, Any]) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"fetched_at": time.time(), "data": data}
    cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
