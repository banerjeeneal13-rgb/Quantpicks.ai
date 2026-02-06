import os
from supabase import create_client, Client

def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not url:
        raise RuntimeError("Missing SUPABASE_URL in services/model/.env")
    if not key:
        raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY in services/model/.env")

    # Helpful debug: prints only a safe prefix
    print("Supabase URL:", url)
    print("Supabase KEY prefix:", key[:12])

    return create_client(url, key)
