import time
import pandas as pd
from pathlib import Path

from nba_api.stats.endpoints import leaguedashteamstats
from nba_api.stats.static import teams as static_teams

SEASONS = ["2023-24", "2024-25"]
SLEEP = 2.0
TIMEOUT = 120

OUT = Path(__file__).resolve().parents[1] / "data" / "team_context.csv"


def _team_id_to_abbr_map() -> dict[int, str]:
    mapping = {}
    for t in static_teams.get_teams():
        mapping[int(t["id"])] = str(t.get("abbreviation") or "").strip()
    return mapping


def fetch_advanced_team_stats(season: str) -> pd.DataFrame:
    # "Advanced" is where PACE / OFF_RATING / DEF_RATING typically are
    resp = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Advanced",
        timeout=TIMEOUT,
    )
    df = resp.get_data_frames()[0].copy()
    df["season"] = season
    return df


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return ""


if __name__ == "__main__":
    id_to_abbr = _team_id_to_abbr_map()

    frames = []
    for s in SEASONS:
        print("Fetching ADVANCED team context:", s)
        frames.append(fetch_advanced_team_stats(s))
        time.sleep(SLEEP)

    all_df = pd.concat(frames, ignore_index=True)

    # Try to find columns, tolerate variations
    c_id = pick_col(all_df, ["TEAM_ID"])
    c_name = pick_col(all_df, ["TEAM_NAME"])
    c_abbr = pick_col(all_df, ["TEAM_ABBREVIATION", "TEAM_ABBR", "ABBREVIATION"])

    c_pace = pick_col(all_df, ["PACE"])
    c_off = pick_col(all_df, ["OFF_RATING", "OFFRTG", "OFFENSIVE_RATING"])
    c_def = pick_col(all_df, ["DEF_RATING", "DEFRTG", "DEFENSIVE_RATING"])
    c_net = pick_col(all_df, ["NET_RATING", "NETRTG"])

    missing = []
    for label, col in [("TEAM_ID", c_id), ("PACE", c_pace), ("OFF_RATING", c_off), ("DEF_RATING", c_def)]:
        if not col:
            missing.append(label)

    if missing:
        print("ERROR: could not find required columns:", missing)
        print("Available columns:", list(all_df.columns))
        raise SystemExit(1)

    team_id_series = all_df[c_id].astype(int)

    if c_abbr:
        abbr_series = all_df[c_abbr].astype(str).str.strip()
    else:
        # Build abbr from static map
        abbr_series = team_id_series.map(id_to_abbr).astype(str).str.strip()

    out = pd.DataFrame({
        "season": all_df["season"].astype(str),
        "team_id": team_id_series,
        "team_name": all_df[c_name].astype(str) if c_name else None,
        "team_abbr": abbr_series,
        "pace": pd.to_numeric(all_df[c_pace], errors="coerce"),
        "off_rating": pd.to_numeric(all_df[c_off], errors="coerce"),
        "def_rating": pd.to_numeric(all_df[c_def], errors="coerce"),
        "net_rating": pd.to_numeric(all_df[c_net], errors="coerce") if c_net else None,
    })

    out = out.dropna(subset=["team_abbr", "pace", "off_rating", "def_rating"]).reset_index(drop=True)
    out = out[out["team_abbr"].astype(str).str.len() > 0].reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print("Saved:", OUT)
    print("Rows:", len(out))
    print("Cols:", list(out.columns))
    print("Seasons:", out["season"].nunique())
    print(out.head(5).to_string(index=False))
