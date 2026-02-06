import json
import time
from pathlib import Path

from nba_latest.cache import cache_path, read_cache, write_cache


def test_cache_roundtrip(tmp_path: Path):
    url = "https://example.com"
    params = {"a": 1}
    cache_file = cache_path(tmp_path, url, params)
    data = {"foo": "bar"}
    write_cache(cache_file, data)
    loaded, fresh = read_cache(cache_file, ttl_seconds=60)
    assert fresh is True
    assert loaded == data


def test_cache_ttl_expired(tmp_path: Path):
    url = "https://example.com"
    params = {"b": 2}
    cache_file = cache_path(tmp_path, url, params)
    payload = {"fetched_at": time.time() - 100, "data": {"x": 1}}
    cache_file.write_text(json.dumps(payload), encoding="utf-8")
    loaded, fresh = read_cache(cache_file, ttl_seconds=10)
    assert loaded == {"x": 1}
    assert fresh is False
