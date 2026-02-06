Coverage Status (Current Repo)

F1_player_performance: PARTIAL
- Player game logs present (nba_player_logs_points_all.csv)
- Lacks full official NBA API logs if stats.nba.com blocked

F2_minutes_and_role: PARTIAL
- Rolling minutes in feature cache; no official starter flags

F3_team_context: PARTIAL
- team_context.csv present (pace/off/def)

F4_opponent_matchup: PARTIAL
- matchup offense/defense files present (active season only)

F5_shot_profile_and_zones: PARTIAL
- Zone client present; data missing if NBA endpoint blocked

F6_schedule_and_rest: PARTIAL
- days_rest computed from logs; no travel proxy

F7_injuries_and_availability: PARTIAL
- injuries_today.csv present; no historical snapshot store

F8_officiating_environment: MISSING
- No referee assignments or foul environment data

F9_market_lines_and_prices: PARTIAL
- Odds ingested elsewhere; not normalized into this store
