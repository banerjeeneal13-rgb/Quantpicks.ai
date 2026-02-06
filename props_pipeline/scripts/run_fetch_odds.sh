#!/usr/bin/env bash
set -euo pipefail

python -m prop_tracker fetch-odds \
  --sport basketball_nba \
  --regions us,au \
  --markets player_points,player_rebounds,player_assists \
  --bookmakers draftkings,fanduel \
  --out data/historical_props.csv
