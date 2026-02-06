import pytest

from nba_etl.cli import _extract_game_ids


def test_extract_game_ids():
    rows = [{"GAME_ID": "1"}, {"GAME_ID": "2"}, {"GAME_ID": "1"}]
    assert _extract_game_ids(rows) == ["1", "2"]
