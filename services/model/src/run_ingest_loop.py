import time
import subprocess
import sys

INTERVAL_SECONDS = 300  # 5 minutes

def main():
    print("Starting ingest loop...")
    print(f"Will run ingest_odds.py every {INTERVAL_SECONDS} seconds.\n")

    while True:
        try:
            print("Running ingest_odds.py...")
            result = subprocess.run(
                [sys.executable, "ingest_odds.py"],
                check=False
            )
            print("Finished ingest_odds.py with exit code:", result.returncode)
        except Exception as e:
            print("ERROR running ingest_odds.py:", e)

        print(f"Sleeping {INTERVAL_SECONDS} seconds...\n")
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
