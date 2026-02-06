import os
from pathlib import Path
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

ETL_DIR = Path(os.getenv("NBA_ETL_OUTPUT_DIR") or (ROOT_DIR / "nba_etl_pkg" / "nba_etl_output"))
TEAM_BOXSCORES_CSV = ETL_DIR / "data" / "team_boxscores.csv"

OUT = DATA_DIR / "team_context.csv"


def main() -> None:
    if not TEAM_BOXSCORES_CSV.exists():
        raise FileNotFoundError(f"Missing team boxscores: {TEAM_BOXSCORES_CSV}")

    teams = pd.read_csv(TEAM_BOXSCORES_CSV, on_bad_lines="skip")
    needed = ["game_id", "season", "season_type", "team_tricode", "minutes", "pts", "fga", "fta", "tov"]
    for c in needed:
        if c not in teams.columns:
            raise KeyError(f"Missing {c} in team_boxscores.csv")

    teams["game_id"] = teams["game_id"].astype(str).str.zfill(10)
    teams["team_tricode"] = teams["team_tricode"].astype(str).str.strip()
    teams = teams.dropna(subset=["game_id", "team_tricode"]).copy()
    teams = teams.drop_duplicates(subset=["game_id", "team_tricode"], keep="last")

    opp = teams.rename(
        columns={
            "team_tricode": "opp_tricode",
            "minutes": "opp_minutes",
            "pts": "opp_pts",
            "fga": "opp_fga",
            "fta": "opp_fta",
            "tov": "opp_tov",
        }
    )

    merged = teams.merge(opp, on="game_id", suffixes=("", "_dup"))
    merged = merged[merged["team_tricode"] != merged["opp_tricode"]].copy()

    # possessions and ratings
    fga = pd.to_numeric(merged["fga"], errors="coerce")
    fta = pd.to_numeric(merged["fta"], errors="coerce")
    tov = pd.to_numeric(merged["tov"], errors="coerce")
    opp_fga = pd.to_numeric(merged["opp_fga"], errors="coerce")
    opp_fta = pd.to_numeric(merged["opp_fta"], errors="coerce")
    opp_tov = pd.to_numeric(merged["opp_tov"], errors="coerce")
    minutes = pd.to_numeric(merged["minutes"], errors="coerce")

    poss = (fga + 0.44 * fta + tov + opp_fga + 0.44 * opp_fta + opp_tov) / 2.0
    merged["pace"] = (poss * 48 / (minutes / 5)).replace([np.inf, -np.inf], np.nan)
    merged["off_rating"] = (pd.to_numeric(merged["pts"], errors="coerce") / poss * 100).replace(
        [np.inf, -np.inf], np.nan
    )
    merged["def_rating"] = (pd.to_numeric(merged["opp_pts"], errors="coerce") / poss * 100).replace(
        [np.inf, -np.inf], np.nan
    )

    grouped = (
        merged.groupby(["season", "team_tricode"], as_index=False)[["pace", "off_rating", "def_rating"]]
        .mean()
        .rename(columns={"team_tricode": "team_abbr"})
    )

    grouped = grouped.dropna(subset=["pace", "off_rating", "def_rating"]).reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(OUT, index=False)

    print("Saved:", OUT)
    print("Rows:", len(grouped))
    print("Seasons:", grouped["season"].nunique())


if __name__ == "__main__":
    main()
