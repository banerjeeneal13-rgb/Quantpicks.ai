import json
from pathlib import Path

import pytest

from nba_etl.transform.normalize import (
    normalize_cdn_boxscore,
    normalize_stats_boxscore,
    normalize_stats_advanced,
)


def load_fixture(name: str) -> dict:
    path = Path(__file__).parent / "fixtures" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_normalize_cdn_boxscore():
    raw = load_fixture("cdn_boxscore.json")
    game, players, teams = normalize_cdn_boxscore(raw, "2023-24", "Regular Season")
    assert game.game_id == "0022400001"
    assert len(players) == 2
    assert len(teams) == 2


def test_normalize_stats_boxscore():
    raw = load_fixture("stats_boxscore.json")
    game, players, teams = normalize_stats_boxscore(raw, "2023-24", "Regular Season")
    assert game.game_id == "0022400002"
    assert len(players) == 2
    assert len(teams) == 2


def test_normalize_stats_advanced():
    raw = load_fixture("stats_advanced.json")
    rows = normalize_stats_advanced(raw, "2023-24", "Regular Season")
    assert rows[0].player_id == "2544"


def test_missing_schema():
    with pytest.raises(ValueError):
        normalize_cdn_boxscore({"bad": "data"}, "2023-24", "Regular Season")
