import os
import requests
from dotenv import load_dotenv

load_dotenv("../.env")

key = os.environ["ODDS_API_KEY"]
sport_key = "basketball_nba"

url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
r = requests.get(url, params={"apiKey": key}, timeout=30)

print("status:", r.status_code)
print(r.text[:2000])
