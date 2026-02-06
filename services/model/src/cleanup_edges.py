import os
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

URL = os.getenv("SUPABASE_URL", "").strip()
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

if not URL or not KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in services/model/.env")

sb = create_client(URL, KEY)

def main():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_iso = cutoff.isoformat()

    # delete old rows
    res = sb.table("edges").delete().lt("starts_at", cutoff_iso).execute()
    n = len(res.data) if res.data else 0
    print("Deleted rows:", n, "cutoff:", cutoff_iso)

if __name__ == "__main__":
    main()
