# QuantPicks AI Project Context

Goal:
- Pull NBA player props odds from TheOddsAPI
- Compute fair probabilities (no-vig fallback)
- Train points model (HistGradientBoostingRegressor) using rolling stats + team context
- Convert predicted points distribution into P(over) and EV
- Store edges in Supabase
- Display top edges on Next.js site with pagination, filters, and source badges

Key scripts:
- fetch_all_players_points.py -> pulls player logs
- fetch_team_context.py -> team pace/off/def ratings
- build_points_dataset.py -> training parquet
- train_points_model.py -> outputs points_model_v4.joblib + calibration
- build_points_feature_cache.py -> latest row per player for live prediction
- upload_injuries.py -> injuries table
- ingest_odds.py -> fetch odds, compute P+EV, upsert into edges table

Edge row fields:
- provider, event_id, market, player_name, side, line, book, odds
- p, ev
- source: points_model_v4 OR no_vig_fallback OR stub_v1
