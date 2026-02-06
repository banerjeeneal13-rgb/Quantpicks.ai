#!/usr/bin/env bash
set -euo pipefail

python -m prop_tracker evaluate \
  --sport basketball_nba \
  --props data/historical_props.csv \
  --stats data/actual_stats.csv \
  --out data/evaluation.csv
