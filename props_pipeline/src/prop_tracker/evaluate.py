"""Evaluation pipeline for props vs actual stats."""
from __future__ import annotations

import pandas as pd

from .normalize import canonical_market
from .utils import normalize_player_name


MARKET_TO_STAT = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "pra": "pra",
    "pa": "pa",
    "pr": "pr",
    "ra": "ra",
    "stocks": "stocks",
}


def evaluate_props(props_csv: str, stats_csv: str, out_csv: str) -> str:
    props = pd.read_csv(props_csv)
    stats = pd.read_csv(stats_csv)

    if props.empty:
        raise RuntimeError("Props CSV is empty")
    if stats.empty:
        raise RuntimeError("Stats CSV is empty")

    props["player_name_norm"] = props["player_name"].astype(str).map(normalize_player_name)
    stats["player_name_norm"] = stats["player_name"].astype(str).map(normalize_player_name)

    if "stat_key" not in props.columns:
        props["stat_key"] = props["market_key"].apply(
            lambda x: canonical_market(str(x))[1] if canonical_market(str(x)) else x
        )

    stats["stat_key"] = stats["stat_key"].map(lambda x: MARKET_TO_STAT.get(str(x), str(x)))

    joined = props.merge(
        stats,
        how="left",
        on=["event_id", "player_name_norm", "stat_key"],
        suffixes=("", "_stat"),
    )

    def compute_hit(row: pd.Series):
        val = row.get("actual_value")
        if pd.isna(val):
            return None
        outcome = str(row.get("outcome") or "").lower()
        line = row.get("line")
        if pd.isna(line):
            return None
        if float(val) == float(line):
            return None
        if outcome == "over":
            return float(val) > float(line)
        if outcome == "under":
            return float(val) < float(line)
        return None

    def compute_notes(row: pd.Series) -> str | None:
        notes = []
        if pd.isna(row.get("actual_value")):
            notes.append("missing_stats")
        elif float(row.get("actual_value")) == float(row.get("line")):
            notes.append("push")
        return ",".join(notes) if notes else None

    joined["hit"] = joined.apply(compute_hit, axis=1)
    joined["notes"] = joined.apply(compute_notes, axis=1)

    joined.to_csv(out_csv, index=False)
    return out_csv
