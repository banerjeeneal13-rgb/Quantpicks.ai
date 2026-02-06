import json
from pathlib import Path

import pytest

from nba_latest.normalize import normalize_primary, normalize_fallback, most_recent_game


def load_fixture(name: str) -> dict:
    path = Path(__file__).parent / "fixtures" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_normalize_primary_final():
    raw = load_fixture("primary_scoreboard.json")
    games = normalize_primary(raw, "America/New_York")
    assert len(games) == 1
    game = games[0]
    assert game.game_id == "0022400001"
    assert game.status == "FINAL"
    assert game.home_team.tricode == "LAL"
    assert game.away_team.tricode == "BOS"


def test_normalize_fallback_final():
    raw = load_fixture("fallback_scoreboard.json")
    games = normalize_fallback(raw, "America/New_York")
    assert len(games) == 1
    game = games[0]
    assert game.game_id == "0022400003"
    assert game.home_team.tricode == "LAL"
    assert game.away_team.tricode == "HOU"


def test_most_recent_game():
    raw = load_fixture("primary_scoreboard.json")
    games = normalize_primary(raw, "America/New_York")
    latest = most_recent_game(games)
    assert latest is not None


def test_primary_schema_missing():
    with pytest.raises(ValueError):
        normalize_primary({"bad": "data"}, "America/New_York")
