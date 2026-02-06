import time
import pandas as pd

from nba_api.stats.endpoints import commonallplayers, matchupsrollup
from requests.exceptions import ReadTimeout


# -----------------------------
# 1) SETTINGS YOU CAN EDIT
# -----------------------------
SEASON = "2025-26"                 # change to "2024-25" if needed
SEASON_TYPE = "Regular Season"     # or "Playoffs"
PER_MODE_SIMPLE = "Totals"         # "Totals" or "PerGame"
SLEEP_SEC = 0.8
MAX_RETRIES = 5

OUT_OFFENSE = f"matchups_offense_active_{SEASON}_{SEASON_TYPE.replace(' ', '_')}.csv"
OUT_DEFENSE = f"matchups_defense_active_{SEASON}_{SEASON_TYPE.replace(' ', '_')}.csv"


# -----------------------------
# 2) GET ACTIVE PLAYERS
# -----------------------------
players_df = commonallplayers.CommonAllPlayers(
    league_id="00",
    season=SEASON,
    is_only_current_season=1,
).get_data_frames()[0]

active_players = players_df[players_df["ROSTERSTATUS"] == 1].copy()
active_players = active_players[["PERSON_ID", "DISPLAY_FIRST_LAST"]].reset_index(drop=True)

print(f"Active players found: {len(active_players)}")


# -----------------------------
# 3) HELPER: API CALL WITH RETRIES
# -----------------------------
def call_matchupsrollup(**kwargs) -> pd.DataFrame:
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = matchupsrollup.MatchupsRollup(**kwargs)
            return resp.get_data_frames()[0]

        except (ReadTimeout, TimeoutError) as e:
            last_err = e
            wait_s = SLEEP_SEC * attempt
            print(f"Timeout. Retry {attempt}/{MAX_RETRIES} in {wait_s:.1f}s -> {e}")
            time.sleep(wait_s)

        except Exception as e:
            last_err = e
            wait_s = SLEEP_SEC * attempt
            print(f"Error. Retry {attempt}/{MAX_RETRIES} in {wait_s:.1f}s -> {e}")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries. Last error: {last_err}")


def fetch_offense_matchups(player_id: int) -> pd.DataFrame:
    return call_matchupsrollup(
        league_id="00",
        per_mode_simple=PER_MODE_SIMPLE,
        season=SEASON,
        season_type_playoffs=SEASON_TYPE,
        off_player_id_nullable=str(player_id),
        # def_player_id_nullable left blank => all defenders
    )


def fetch_defense_matchups(player_id: int) -> pd.DataFrame:
    return call_matchupsrollup(
        league_id="00",
        per_mode_simple=PER_MODE_SIMPLE,
        season=SEASON,
        season_type_playoffs=SEASON_TYPE,
        def_player_id_nullable=str(player_id),
        # off_player_id_nullable left blank => all offensive players faced
    )


# -----------------------------
# 4) LOOP: OFFENSE CSV
# -----------------------------
offense_dfs = []

for idx, row in active_players.iterrows():
    pid = int(row["PERSON_ID"])
    name = row["DISPLAY_FIRST_LAST"]

    print(f"[OFF {idx+1}/{len(active_players)}] {name} ({pid})")

    try:
        df = fetch_offense_matchups(pid)
        df["FOCUS_PLAYER_ID"] = pid
        df["FOCUS_PLAYER_NAME"] = name
        df["FOCUS_ROLE"] = "OFFENSE"
        df["SEASON"] = SEASON
        df["SEASON_TYPE"] = SEASON_TYPE
        offense_dfs.append(df)

    except Exception as e:
        print(f"SKIP OFF: {name} ({pid}) -> {e}")

    time.sleep(SLEEP_SEC)

if not offense_dfs:
    raise RuntimeError("No OFFENSE matchup data collected.")

off_out = pd.concat(offense_dfs, ignore_index=True)
off_out.to_csv(OUT_OFFENSE, index=False)
print(f"\nSAVED OFFENSE CSV: {OUT_OFFENSE} | rows={len(off_out)}\n")


# -----------------------------
# 5) LOOP: DEFENSE CSV
# -----------------------------
defense_dfs = []

for idx, row in active_players.iterrows():
    pid = int(row["PERSON_ID"])
    name = row["DISPLAY_FIRST_LAST"]

    print(f"[DEF {idx+1}/{len(active_players)}] {name} ({pid})")

    try:
        df = fetch_defense_matchups(pid)
        df["FOCUS_PLAYER_ID"] = pid
        df["FOCUS_PLAYER_NAME"] = name
        df["FOCUS_ROLE"] = "DEFENSE"
        df["SEASON"] = SEASON
        df["SEASON_TYPE"] = SEASON_TYPE
        defense_dfs.append(df)

    except Exception as e:
        print(f"SKIP DEF: {name} ({pid}) -> {e}")

    time.sleep(SLEEP_SEC)

if not defense_dfs:
    raise RuntimeError("No DEFENSE matchup data collected.")

def_out = pd.concat(defense_dfs, ignore_index=True)
def_out.to_csv(OUT_DEFENSE, index=False)
print(f"SAVED DEFENSE CSV: {OUT_DEFENSE} | rows={len(def_out)}")

print("\nDONE.")
