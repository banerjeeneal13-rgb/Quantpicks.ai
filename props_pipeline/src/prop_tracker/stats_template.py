from __future__ import annotations

import pandas as pd

from .normalize import canonical_market


def build_stats_template(props_csv: str, out_csv: str) -> str:
    props = pd.read_csv(props_csv)
    if props.empty:
        raise RuntimeError("Props CSV is empty")

    if "stat_key" not in props.columns:
        props["stat_key"] = props["market_key"].apply(
            lambda x: canonical_market(str(x))[1] if canonical_market(str(x)) else x
        )

    template = props[["event_id", "event_start_utc", "player_name", "stat_key"]].drop_duplicates()
    template["actual_value"] = pd.NA
    template.to_csv(out_csv, index=False)
    return out_csv
