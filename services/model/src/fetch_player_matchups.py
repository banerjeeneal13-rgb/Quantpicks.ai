import time
import pandas as pd
from nba_api.stats.endpoints import leaguedashptdefend

OUT_PATH = "../data/player_ptdefend_2024_25.csv"

def fetch_with_retry(max_tries=5, sleep_sec=8):
    last_err = None

    for i in range(1, max_tries + 1):
        try:
            print(f"Attempt {i}/{max_tries}...")

            endpoint = leaguedashptdefend.LeagueDashPtDefend(
                season="2024-25",
                per_mode="PerGame",
                defense_category="Overall",
                timeout=120,
            )

            df = endpoint.get_data_frames()[0]
            return df

        except Exception as e:
            last_err = e
            print("Error:", e)
            print(f"Sleeping {sleep_sec}s then retrying...")
            time.sleep(sleep_sec)

    raise RuntimeError(f"Failed after {max_tries} tries. Last error: {last_err}")

def main():
    print("Fetching NBA player tracking defense data (ptdefend) for 2024-25...")

    df = fetch_with_retry()
    df["season"] = "2024-25"

    print("Rows:", len(df))
    print("Cols:", list(df.columns))

    df.to_csv(OUT_PATH, index=False)
    print("Saved to:", OUT_PATH)

if __name__ == "__main__":
    main()
