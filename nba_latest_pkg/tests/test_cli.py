import pytest

from nba_latest import cli


class DummyResult:
    def __init__(self, data, stale=False):
        self.data = data
        self.stale = stale


def test_no_games_in_lookback(monkeypatch, tmp_path):
    def fake_primary(self, day):
        return DummyResult({"scoreboard": {"games": []}})

    def fake_fallback(self, day):
        return DummyResult({"resultSets": []})

    monkeypatch.setattr(cli.ScoreboardClient, "fetch_scoreboard", fake_primary)
    monkeypatch.setattr(cli.StatsClient, "fetch_scoreboard", fake_fallback)

    with pytest.raises(RuntimeError):
        cli._find_latest_game(
            lookback_days=1,
            timezone="America/New_York",
            cache_dir=tmp_path,
            timeout_s=1,
            max_retries=0,
            backoff_base_s=0.1,
        )
