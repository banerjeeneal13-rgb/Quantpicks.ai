$ErrorActionPreference = "Stop"

$repo = "C:\Users\baner\quantpicks-ai"
$etlDir = Join-Path $repo "nba_etl_pkg"
$modelSrc = Join-Path $repo "services\model\src"
$etlOut = Join-Path $etlDir "nba_etl_output"
$logDir = Join-Path $repo "logs"
$logPath = Join-Path $logDir "overnight_etl_retrain.log"
$maxLogBytes = 5MB
$venvPython = Join-Path $repo "services\model\.venv\Scripts\python.exe"
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) { $python = "python" }
$modelPython = $python
if (Test-Path $venvPython) { $modelPython = $venvPython }

if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}
if (Test-Path $logPath) {
  $len = (Get-Item $logPath).Length
  if ($len -gt $maxLogBytes) {
    $stamp = (Get-Date).ToString("yyyyMMdd-HHmmss")
    $rotated = Join-Path $logDir ("overnight_etl_retrain_" + $stamp + ".log")
    Move-Item $logPath $rotated -Force
  }
}
try {
  Start-Transcript -Path $logPath -Append | Out-Null
} catch {}

# Full season pull (current season start -> today)
$endDate = (Get-Date).ToString("yyyy-MM-dd")
$seasonStart = "2025-10-01"

Push-Location $etlDir

# Optional one-time backfill for 2024-25 (set ETL_BACKFILL_2024_25=1 if needed)
if ($env:ETL_BACKFILL_2024_25 -eq "1") {
  & $python -m nba_etl.cli --log-level info fetch `
    --seasons 2024-25 `
    --season-type "Regular Season" `
    --out-dir $etlOut `
    --date-start 2024-10-01 `
    --date-end 2025-06-30 `
    --chunk-days 7 `
    --auto-loop `
    --no-scoreboard-fallback `
    --cache-ttl 600 `
    --timeout 90 `
    --max-retries 4 `
    --backoff 2.0
}

# 1) Pull full current season for 2025-26
& $python -m nba_etl.cli --log-level info fetch `
  --seasons 2025-26 `
  --season-type "Regular Season" `
  --out-dir $etlOut `
  --date-start $seasonStart `
  --date-end $endDate `
  --chunk-days 7 `
  --auto-loop `
  --cache-ttl 600 `
  --timeout 60 `
  --max-retries 2 `
  --backoff 1.5
Pop-Location

try {
  Stop-Transcript | Out-Null
} catch {}

# 3) Ingest ETL boxscores into model logs + advanced features
& $modelPython (Join-Path $modelSrc "ingest_nba_etl_boxscores.py")
& $modelPython (Join-Path $modelSrc "fetch_espn_boxscores.py")

# 4) Retrain model artifacts
Push-Location $modelSrc
& $modelPython build_points_dataset.py
& $modelPython train_points_model.py
& $modelPython fetch_espn_injuries.py
& $modelPython fetch_nba_team_defense_zones.py
if ($env:ENABLE_NBA_ZONE -eq "1") {
  $year = (Get-Date).Year
  if ((Get-Date).Month -ge 8) {
    $season = "{0}-{1}" -f $year, ($year + 1).ToString().Substring(2, 2)
  } else {
    $season = "{0}-{1}" -f ($year - 1), $year.ToString().Substring(2, 2)
  }
  & $modelPython (Join-Path $modelSrc "fetch_zone_stats.py") --season $season --season_type "Regular Season" --entity team
  & $modelPython (Join-Path $modelSrc "fetch_zone_stats.py") --season $season --season_type "Regular Season" --entity player
}
if ($env:ENABLE_NBA_WITH_WITHOUT -eq "1") {
  & $modelPython fetch_nba_player_with_without.py
}
& $modelPython build_points_feature_cache.py
Pop-Location
