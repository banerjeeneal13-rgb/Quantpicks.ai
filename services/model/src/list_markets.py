import os
import requests
from dotenv import load_dotenv

# load ../.env (the one in services/model/.env)
load_dotenv("../.env")

key = os.environ.get("ODDS_API_KEY")
if not key:
    raise SystemExit("Missing ODDS_API_KEY in services/model/.env")

url = "https://api.the-odds-api.com/v4/sports/basketball_nba/markets"
r = requests.get(url, params={"apiKey": key}, timeout=30)

print("status:", r.status_code)
print(r.text[:4000])
