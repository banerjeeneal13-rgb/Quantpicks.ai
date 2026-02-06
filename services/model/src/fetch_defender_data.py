import time
from pathlib import Path
import pandas as pd

from nba_api.stats.endpoints import leaguedashptdefend, leaguedashplayerbiostats

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
LOGS_CSV = DATA_DIR / "nba_player_logs_points_all.csv"
OUT_CSV = DATA_DIR / "defender_ratings.csv"

SLEEP_BETWEEN_CALLS = 1.0


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"Missing columns {candidates}. Have: {list(df.columns)}")


def fetch_season_defender_stats(season: str) -> pd.DataFrame:
    defend = leaguedashptdefend.LeagueDashPtDefend(
        season=season,
        season_type_all_star="Regular Season",
        per_mode_simple="Totals",
    )
    df_def = defend.get_data_frames()[0]

    bio = leaguedashplayerbiostats.LeagueDashPlayerBioStats(
        season=season,
        season_type_all_star="Regular Season",
        per_mode_simple="Totals",
    )
    df_bio = bio.get_data_frames()[0]

    col_id = pick_col(df_def, ["PLAYER_ID", "CLOSE_DEF_PERSON_ID"])
    col_name = pick_col(df_def, ["PLAYER_NAME"])
    col_team = pick_col(df_def, ["TEAM_ABBREVIATION", "TEAM_ABBR", "PLAYER_LAST_TEAM_ABBREVIATION"])
    col_fg_pct = pick_col(df_def, ["DEF_FG_PCT", "D_FG_PCT", "OPP_FG_PCT", "FG_PCT"])
    col_fga = pick_col(df_def, ["DEF_FGA", "D_FGA", "FGA"])

    df_out = df_def[[col_id, col_name, col_team, col_fg_pct, col_fga]].copy()
    df_out = df_out.rename(
        columns={
            col_id: "player_id",
            col_name: "player_name",
            col_team: "team_abbr",
            col_fg_pct: "def_fg_pct",
            col_fga: "def_fga",
        }
    )

    if "PLAYER_POSITION" not in df_def.columns:
        if "PLAYER_POSITION" not in df_bio.columns or "PLAYER_ID" not in df_bio.columns:
            raise KeyError(
                f"PLAYER_ID or PLAYER_POSITION missing from bio stats. Have: {list(df_bio.columns)}"
            )
        df_bio = df_bio[["PLAYER_ID", "PLAYER_POSITION"]].rename(
            columns={"PLAYER_ID": "player_id", "PLAYER_POSITION": "player_position"}
        )
        df_out = df_out.merge(df_bio, how="left", on="player_id")
    else:
        df_out["player_position"] = df_def["PLAYER_POSITION"]
    df_out["season"] = season
    return df_out


def main():
    if not LOGS_CSV.exists():
        raise RuntimeError(f"Missing logs csv: {LOGS_CSV}")

    logs = pd.read_csv(LOGS_CSV, usecols=["season"])
    seasons = sorted(set(logs["season"].dropna().astype(str).tolist()))
    if not seasons:
        raise RuntimeError("No seasons found in logs.")

    all_rows = []
    for season in seasons:
        print("Fetching defender stats for season:", season)
        df = fetch_season_defender_stats(season)
        all_rows.append(df)
        time.sleep(SLEEP_BETWEEN_CALLS)

    out = pd.concat(all_rows, ignore_index=True)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print("Saved:", OUT_CSV)
    print("Rows:", len(out))
    print("Seasons:", out["season"].nunique())


if __name__ == "__main__":
    main()
