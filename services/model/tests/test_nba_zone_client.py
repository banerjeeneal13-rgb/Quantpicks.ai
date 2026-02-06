import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.model.src.nba_zone_client import (  # noqa: E402
    AccessDeniedError,
    InvalidResponseError,
    RateLimitedError,
    ZoneClient,
    ZoneClientConfig,
)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def make_client(tmp_path, responses):
    calls = {"count": 0}

    def fake_get(*args, **kwargs):
        idx = min(calls["count"], len(responses) - 1)
        calls["count"] += 1
        return responses[idx]

    cfg = ZoneClientConfig(cache_dir=Path(tmp_path), cache_ttl_seconds=3600, rate_limit_per_sec=0)
    return ZoneClient(cfg, http_get=fake_get)

class ZoneClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(__file__).resolve().parent / ".tmp_zone_cache"
        if self.tmp.exists():
            for p in self.tmp.glob("*"):
                p.unlink()
        else:
            self.tmp.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.tmp.exists():
            for p in self.tmp.glob("*"):
                p.unlink()

    def test_cache_hit(self):
        payload = {"resultSets": [{"name": "test", "headers": ["A"], "rowSet": [[1]]}]}
        client = make_client(self.tmp, [FakeResponse(200, payload)])
        res1 = client.request("endpoint", {"Season": "2025-26"})
        res2 = client.request("endpoint", {"Season": "2025-26"})
        self.assertEqual(res1["status"], "fetched")
        self.assertEqual(res2["status"], "cache_hit")

    def test_access_denied(self):
        client = make_client(self.tmp, [FakeResponse(403, {})])
        with self.assertRaises(AccessDeniedError):
            client.request("endpoint", {"Season": "2025-26"})

    def test_rate_limited(self):
        client = make_client(self.tmp, [FakeResponse(429, {}), FakeResponse(429, {})])
        with self.assertRaises(RateLimitedError):
            client.request("endpoint", {"Season": "2025-26"})

    def test_invalid_response(self):
        client = make_client(self.tmp, [FakeResponse(200, {"oops": 1})])
        with self.assertRaises(InvalidResponseError):
            client.request("endpoint", {"Season": "2025-26"})


if __name__ == "__main__":
    unittest.main()
