import os
import re
import time
import sys
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone

import requests
import numpy as np
from dotenv import load_dotenv

from supabase_client import get_supabase
from points_predictor import get_predictor
from market_predictor import can_predict_market, get_market_predictor

# -----------------------------
# CONFIG
# -----------------------------
BASE_URL = os.getenv("SGO_BASE_URL", "https://api.sportsgameodds.com/v2").rstrip("/")
LEAGUE_ID = os.getenv("SGO_LEAGUE_ID", "NBA")
SPORT_LABEL = "NBA"
PROVIDER = "sportsgameodds"
MODEL_VERSION = "points_model_v4"

SUPPORTED_MARKETS = {
    "moneyline",
    "spread",
    "total",
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
    "player_blocks_steals",
}

SLEEP_BETWEEN_CALLS = 0.25   # be polite to API
CHUNK_SIZE = 250

# -----------------------------
# HELPERS
# -----------------------------

def norm_name(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    s = s.replace("’", "'")
    s = s.replace("'", "")
    s = s.replace(".", "")
    for suf in [" jr", " sr", " ii", " iii", " iv", " v"]:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def decimal_from_american(american: float) -> Optional[float]:
    if american is None or american == 0:
        return None
    if american > 0:
        return 1 + american / 100.0
    return 1 + 100.0 / abs(american)


def implied_prob_from_decimal(decimal_odds: float) -> Optional[float]:
    if not decimal_odds or decimal_odds <= 1.0:
        return None
    return 1.0 / decimal_odds


def no_vig_pair(over_odds: float, under_odds: float) -> Optional[Tuple[float, float]]:
    po = implied_prob_from_decimal(over_odds)
    pu = implied_prob_from_decimal(under_odds)
    if po is None or pu is None:
        return None
    s = po + pu
    if s <= 0:
        return None
    return po / s, pu / s


def ev_per_dollar(p: float, decimal_odds: float) -> Optional[float]:
    if p is None:
        return None
    d = safe_float(decimal_odds)
    if d is None or d <= 1.0:
        return None
    return float(p * d - 1.0)


def sgo_get(path: str, params: Dict[str, Any], api_key: str, timeout=30) -> Any:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    headers = {"x-api-key": api_key}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_events(api_key: str, limit: int, bookmakers: List[str] | None, event_id: str | None, deep: bool) -> List[Dict[str, Any]]:
    params = {
        "leagueID": LEAGUE_ID,
        "oddsAvailable": "true",
        "limit": limit,
    }
    if bookmakers:
        params["bookmakerID"] = ",".join(bookmakers)
    if event_id:
        params["eventID"] = event_id
    if deep:
        params["includeAltLines"] = "true"

    data = sgo_get("/events/", params=params, api_key=api_key, timeout=60)
    events = data.get("data") or data.get("events") or data
    if not isinstance(events, list):
        return []
    return events


def parse_start_time(event: Dict[str, Any]) -> Optional[str]:
    value = None
    status = event.get("status") or {}
    value = status.get("startsAt") or event.get("startTimeUTC") or event.get("start_time_utc")
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def map_market(odd_id: str, stat_id: str, bet_type_id: str, stat_entity_id: str) -> Tuple[str, str]:
    s = f"{odd_id} {stat_id}".lower()
    is_player = stat_entity_id and stat_entity_id not in {"all", "home", "away"}

    if is_player:
        if "points" in s and "rebounds" in s and "assists" in s:
            return "player_points_rebounds_assists", "pra"
        if "points" in s and "assists" in s:
            return "player_points_assists", "pa"
        if "points" in s and "rebounds" in s:
            return "player_points_rebounds", "pr"
        if "rebounds" in s and "assists" in s:
            return "player_rebounds_assists", "ra"
        if "blocks" in s and "steals" in s:
            return "player_blocks_steals", "stocks"
        if "points" in s:
            return "player_points", "points"
        if "rebounds" in s:
            return "player_rebounds", "rebounds"
        if "assists" in s:
            return "player_assists", "assists"
        if "threes" in s or "3pt" in s or "three" in s:
            return "player_threes", "threes"
        return f"unknown:{odd_id}", "unknown"

    if bet_type_id == "ml":
        return "moneyline", "points"
    if bet_type_id == "sp":
        return "spread", "points"
    if bet_type_id == "ou":
        return "total", "points"

    return f"unknown:{odd_id}", "unknown"


def extract_outcomes(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    odds_map = event.get("odds") or {}
    players = event.get("players") or {}
    rows: List[Dict[str, Any]] = []

    for odd_id, odd_obj in odds_map.items():
        stat_id = str(odd_obj.get("statID") or "")
        stat_entity_id = str(odd_obj.get("statEntityID") or "")
        bet_type_id = str(odd_obj.get("betTypeID") or "")
        side_id = str(odd_obj.get("sideID") or "")
        market_key, stat_key = map_market(odd_id, stat_id, bet_type_id, stat_entity_id)

        by_book = odd_obj.get("byBookmaker") or {}
        for book_id, book_obj in by_book.items():
            if book_obj.get("available") is False:
                continue
            odds_val = book_obj.get("odds")
            if odds_val is None:
                continue
            odds_val = str(odds_val).strip()
            american = safe_float(odds_val)
            price_decimal = decimal_from_american(american) if american is not None else None
            if price_decimal is None:
                continue

            line_val = book_obj.get("overUnder") or odd_obj.get("bookOverUnder") or odd_obj.get("fairOverUnder")
            line_val = safe_float(line_val)

            player_name = ""
            player_id = ""
            if stat_entity_id and stat_entity_id not in {"all", "home", "away"}:
                player_id = stat_entity_id
                player_name = str((players.get(stat_entity_id) or {}).get("name") or "")

            side_id_norm = side_id.strip().lower()
            if side_id_norm not in {"over", "under", "home", "away"}:
                continue

            rows.append({
                "market_key": market_key,
                "stat_key": stat_key,
                "book_id": str(book_id),
                "last_updated_at": str(book_obj.get("lastUpdatedAt") or ""),
                "odd_id": odd_id,
                "stat_id": stat_id,
                "bet_type_id": bet_type_id,
                "side_id": side_id_norm,
                "player_name": player_name,
                "player_id": player_id,
                "line": line_val,
                "price_decimal": float(price_decimal),
                "price_american": int(american) if american is not None else None,
            })

    return rows


def build_rows_from_event(event: Dict[str, Any], event_outcomes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    counters = {
        "pairs_total": 0,
        "used_model": 0,
        "used_fallback": 0,
        "rows_built": 0,
    }

    event_id = str(event.get("eventID") or event.get("id") or "")
    starts_at = parse_start_time(event)
    teams = event.get("teams") or {}
    home_team = str((teams.get("home") or {}).get("names", {}).get("long") or "")
    away_team = str((teams.get("away") or {}).get("names", {}).get("long") or "")

    rows: List[Dict[str, Any]] = []
    pred_rows: List[Dict[str, Any]] = []
    created_at = datetime.now(timezone.utc).isoformat()

    predictor_ok = True
    predictor = None
    try:
        predictor = get_predictor()
    except Exception:
        predictor_ok = False

    avg_pts_map = {}
    if predictor_ok and predictor is not None:
        try:
            cache = predictor.cache_df
            if "player_name" in cache.columns and "pts_ma_10" in cache.columns:
                for _, row in cache[["player_name", "pts_ma_10"]].dropna().iterrows():
                    name = norm_name(str(row["player_name"]))
                    if name:
                        avg_pts_map[name] = float(row["pts_ma_10"])
        except Exception:
            avg_pts_map = {}

    def maybe_fix_points_line(player: str, line: float, median_line: float | None) -> float:
        if line is None:
            return line
        # Only consider correcting clearly truncated lines (e.g. 4.5 -> 14.5).
        try:
            if median_line is not None and np.isfinite(median_line) and median_line >= 10:
                if line < 6 or (median_line - line) >= 6:
                    candidates = [line + 10.0, line + 20.0, line + 30.0]
                    candidates = [c for c in candidates if 0 <= c <= 50]
                    if candidates:
                        best = min(candidates, key=lambda c: abs(c - median_line))
                        if abs(best - median_line) + 0.5 < abs(line - median_line):
                            return best
            if predictor_ok and predictor and line < 6:
                name_key = norm_name(player)
                avg = avg_pts_map.get(name_key)
                if avg is None or not np.isfinite(avg):
                    return line
                if avg >= 12:
                    candidates = [line + 10.0, line + 20.0, line + 30.0]
                    candidates = [c for c in candidates if 0 <= c <= 50]
                    if candidates:
                        best = min(candidates, key=lambda c: abs(c - avg))
                        if abs(best - avg) + 0.5 < abs(line - avg):
                            return best
        except Exception:
            return line
        return line

    line_medians: Dict[Tuple[str, str], float] = {}
    if event_outcomes:
        tmp_lines: Dict[Tuple[str, str], List[float]] = {}
        for item in event_outcomes:
            market_key = item.get("market_key")
            if market_key != "player_points":
                continue
            player_key = norm_name(item.get("player_name") or "")
            line_val = safe_float(item.get("line"))
            if not player_key or line_val is None or not np.isfinite(line_val):
                continue
            tmp_lines.setdefault((market_key, player_key), []).append(float(line_val))
        for k, vals in tmp_lines.items():
            if not vals:
                continue
            vals_sorted = sorted(vals)
            line_medians[k] = float(vals_sorted[len(vals_sorted) // 2])

    grouped: Dict[Tuple[str, str, str, float], Dict[str, Dict[str, Any]]] = {}
    for item in event_outcomes:
        market_key = item["market_key"]
        book_id = item["book_id"]
        player_name = item.get("player_name") or ""
        player_key = norm_name(player_name)
        line = safe_float(item.get("line")) or 0.0
        if market_key == "player_points":
            median_key = (market_key, player_key)
            median_line = line_medians.get(median_key)
            line = maybe_fix_points_line(player_name, float(line), median_line)
        outcome = str(item.get("side_id") or "").strip().lower()
        if not market_key or not book_id or not outcome:
            continue
        key = (market_key, book_id, player_key, float(line))
        grouped.setdefault(key, {})[outcome] = item

    for (market_key, book_id, player_key, line_val), outcome_map in grouped.items():
        counters["pairs_total"] += 1
        over_item = outcome_map.get("over")
        under_item = outcome_map.get("under")
        home_item = outcome_map.get("home")
        away_item = outcome_map.get("away")

        p_over = None
        p_under = None
        if over_item and under_item:
            nv = no_vig_pair(float(over_item["price_decimal"]), float(under_item["price_decimal"]))
            if nv:
                p_over, p_under = nv

        p_home = None
        p_away = None
        if home_item and away_item:
            nv = no_vig_pair(float(home_item["price_decimal"]), float(away_item["price_decimal"]))
            if nv:
                p_home, p_away = nv

        for outcome_name, item in outcome_map.items():
            price = safe_float(item.get("price_decimal"))
            if price is None:
                continue

            player_name = item.get("player_name") or ""
            if outcome_name == "home":
                player_name = home_team
            elif outcome_name == "away":
                player_name = away_team
            if market_key == "total":
                player_name = "Game Total"

            p = None
            source = "no_vig_fallback"
            if market_key == "player_points" and predictor_ok:
                try:
                    dbg = predictor.debug(player_name, float(line_val))
                    p = float(dbg["p_final"]) if outcome_name == "over" else 1.0 - float(dbg["p_final"])
                    source = MODEL_VERSION
                except Exception:
                    p = None
            elif market_key.startswith("player_") and market_key != "player_points" and can_predict_market(market_key):
                try:
                    mk_pred = get_market_predictor(market_key)
                    dbg = mk_pred.debug(player_name, float(line_val))
                    p = float(dbg["p_final"]) if outcome_name == "over" else 1.0 - float(dbg["p_final"])
                    source = f"{market_key}_model_v1"
                except Exception:
                    p = None

            if p is None:
                if outcome_name == "over":
                    p = p_over
                elif outcome_name == "under":
                    p = p_under
                elif outcome_name == "home":
                    p = p_home
                elif outcome_name == "away":
                    p = p_away

            if p is None:
                p = implied_prob_from_decimal(price)

            if p is None:
                continue

            ev = ev_per_dollar(float(p), float(price))
            if ev is None:
                continue

            line_out = float(line_val) if line_val is not None else 0.0
            store_side = outcome_name
            if outcome_name == "home":
                store_side = "over"
            elif outcome_name == "away":
                store_side = "under"

            rows.append({
                "provider": PROVIDER,
                "event_id": event_id,
                "sport": SPORT_LABEL,
                "market": market_key,
                "player_name": player_name,
                "side": store_side,
                "line": line_out,
                "book": str(book_id).lower(),
                "odds": float(price),
                "p": float(p),
                "ev": float(ev),
                "starts_at": starts_at,
                "source": source,
                "created_at": created_at,
            })
            pred_rows.append({
                "provider": PROVIDER,
                "event_id": event_id,
                "sport": SPORT_LABEL,
                "market": market_key,
                "player_name": player_name,
                "side": store_side,
                "line": line_out,
                "book": str(book_id).lower(),
                "odds": float(price),
                "p_model": float(p),
                "p_raw": None,
                "source": source,
                "model_version": MODEL_VERSION if source == MODEL_VERSION else None,
                "starts_at": starts_at,
                "game_date": starts_at[:10] if starts_at else None,
                "created_at": created_at,
            })

            if source == MODEL_VERSION:
                counters["used_model"] += 1
            else:
                counters["used_fallback"] += 1

    counters["rows_built"] = len(rows)
    return rows, pred_rows, counters


def upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sb = get_supabase()

    total = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        res = sb.table("edges").upsert(
            chunk,
            on_conflict="provider,event_id,market,player_name,side,line,book"
        ).execute()

        data = getattr(res, "data", None)
        if isinstance(data, list):
            total += len(data)
        else:
            total += len(chunk)

    return total


def upsert_predictions(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sb = get_supabase()

    total = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        res = sb.table("predictions").upsert(
            chunk,
            on_conflict="provider,event_id,market,player_name,side,line,book"
        ).execute()

        data = getattr(res, "data", None)
        if isinstance(data, list):
            total += len(data)
        else:
            total += len(chunk)
    return total


def main():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    api_key = (os.getenv("SPORTS_GAME_ODDS_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing SPORTS_GAME_ODDS_API_KEY in services/model/.env")

    sb_url = (os.getenv("SUPABASE_URL") or "").strip()
    sb_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not sb_url or not sb_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in services/model/.env")

    print("Supabase URL:", sb_url)
    print("Supabase KEY prefix:", (sb_key[:12] + "...") if len(sb_key) > 12 else sb_key)

    max_events_raw = (os.getenv("MAX_EVENTS") or "").strip()
    max_events = int(max_events_raw) if max_events_raw.isdigit() else 0
    bookmakers_raw = (os.getenv("SGO_BOOKMAKERS") or "").strip()
    bookmakers = [b.strip() for b in bookmakers_raw.split(",") if b.strip()] if bookmakers_raw else None

    events = fetch_events(api_key, limit=max_events or 50, bookmakers=bookmakers, event_id=None, deep=False)
    print("Found events:", len(events))

    total_upserted = 0
    agg = {
        "used_model": 0,
        "used_fallback": 0,
        "pairs_total": 0,
        "rows_built": 0,
    }

    for idx, ev in enumerate(events, start=1):
        event_id = str(ev.get("eventID") or ev.get("id") or "")
        if not event_id:
            continue
        print(f"[{idx}/{len(events)}] Event {event_id}")

        event_outcomes = extract_outcomes(ev)
        rows, pred_rows, counts = build_rows_from_event(ev, event_outcomes)
        for k, v in counts.items():
            agg[k] = agg.get(k, 0) + int(v)

        up = upsert_rows(rows)
        up_pred = upsert_predictions(pred_rows)
        total_upserted += up

        print(
            f"  rows={len(rows)} upserted={up} preds={up_pred} | "
            f"model_used={agg['used_model']} fallback_used={agg['used_fallback']}"
        )
        time.sleep(SLEEP_BETWEEN_CALLS)

    print("Done. Total upserted:", total_upserted)
    print("Pairs total:", agg["pairs_total"])
    print("Rows built:", agg["rows_built"])
    print("Model used:", agg["used_model"])
    print("Fallback used:", agg["used_fallback"])

    if os.getenv("AUTO_RETRAIN_AFTER_INGEST", "0").strip() == "1":
        retrain_script = Path(__file__).resolve().parent / "run_daily_retrain.py"
        if retrain_script.exists():
            print("AUTO_RETRAIN_AFTER_INGEST=1 -> running daily retrain script")
            subprocess.run([sys.executable, str(retrain_script)], check=False)
        else:
            print("AUTO_RETRAIN_AFTER_INGEST=1 but run_daily_retrain.py not found; skipping.")


if __name__ == "__main__":
    main()
