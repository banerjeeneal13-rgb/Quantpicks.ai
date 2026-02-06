NBA Modelling Feature Store (Current)

Keys
- game_id (where available), game_date_utc, team_id/abbr, opponent_team_id/abbr, player_id/name, season, season_type

Current Core Tables (existing in repo)
- player_game_box: `services/model/data/nba_player_logs_points_all.csv`
  - Columns: MIN, PTS, REB, AST, STL, BLK, TOV, FGA, FGM, FG3A, FG3M, FTA, FTM, MATCHUP, GAME_DATE
- team_context: `services/model/data/team_context.csv`
  - pace, off_rating, def_rating
- injuries (snapshot): `services/model/data/injuries_today.csv`
  - status, game_date, team_abbr, player_name
- matchups_defense: `services/model/matchups_defense_active_2025-26_Regular_Season.csv`
  - defender matchup FG%/3P%/FGA/MIN
- matchups_offense: `services/model/matchups_offense_active_2025-26_Regular_Season.csv`
  - offensive matchup FG%/3P%/FGA/MIN
- feature_cache: `services/model/data/points_feature_cache.csv`
  - normalized features used for prediction

Optional (compliant, may be missing)
- zone_stats_team_*.json (NBA zone client output)
  - shot zone buckets by team (Opponent measure)

Outputs
- points_training.parquet (model training data)
- points_model_v4.joblib (trained model artifact)
- points_feature_cache.csv (latest features for inference)
