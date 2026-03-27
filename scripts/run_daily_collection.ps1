$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
  Run today's data collection to update:
  1. Top 3 picks ROI (grade completed games)
  2. Generate new premium picks for today based on threshold criteria
  3. Refresh the daily picks snapshot via the API

  This is a lightweight script to run on-demand or at the start of the day.
#>

$repo = "C:\Users\baner\Dropbox\Quantpicks.ai"
$modelSrc = Join-Path $repo "services\model\src"
$etlDir = Join-Path $repo "nba_etl_pkg"
$etlOut = Join-Path $etlDir "nba_etl_output"
$logDir = Join-Path $repo "logs"
$logPath = Join-Path $logDir "daily_collection.log"
$venvPython = Join-Path $repo "services\model\.venv\Scripts\python.exe"
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) { $python = "python" }
$modelPython = $python
if (Test-Path $venvPython) { $modelPython = $venvPython }

if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}

function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "[$ts] $msg"
  Write-Host $line
  Add-Content -Path $logPath -Value $line -ErrorAction SilentlyContinue
}

Log "=== Daily collection starting ==="

# ── Step 1: Pull latest data (last 2 days) ────────────────────────────────
Log "Step 1: Pulling latest NBA data..."

$end = (Get-Date).ToString("yyyy-MM-dd")
$start = (Get-Date).AddDays(-2).ToString("yyyy-MM-dd")

Push-Location $etlDir
try {
  & $python -m nba_etl.cli --log-level warning fetch `
    --seasons 2025-26 `
    --season-type "Regular Season" `
    --out-dir $etlOut `
    --date-start $start `
    --date-end $end `
    --chunk-days 2 `
    --auto-loop `
    --cache-ttl 300 `
    --timeout 20 `
    --max-retries 2 `
    --backoff 1.0
  Log "ETL fetch complete."
} catch {
  Log "ETL fetch warning: $_"
}
Pop-Location

# ── Step 2: Ingest box scores and update player logs ──────────────────────
Log "Step 2: Ingesting box scores..."
try {
  & $modelPython (Join-Path $modelSrc "ingest_nba_etl_boxscores.py")
  & $modelPython (Join-Path $modelSrc "fetch_espn_boxscores.py")
  Log "Box score ingest complete."
} catch {
  Log "Box score ingest warning: $_"
}

# ── Step 3: Update injuries and feature cache ─────────────────────────────
Log "Step 3: Updating injuries and features..."
try {
  & $modelPython (Join-Path $modelSrc "fetch_espn_injuries.py")
  & $modelPython (Join-Path $modelSrc "fetch_nba_team_defense_zones.py")
  & $modelPython (Join-Path $modelSrc "build_points_feature_cache.py")
  Log "Feature cache rebuilt."
} catch {
  Log "Feature refresh warning: $_"
}

# ── Step 4: Trigger daily picks snapshot via web API ──────────────────────
Log "Step 4: Triggering daily picks snapshot..."

# Read Supabase URL from web env for API call
$webEnv = Join-Path $repo "apps\web\.env.local"
$appUrl = "http://localhost:3000" # Default Next.js dev URL

# Try to hit the daily-picks API to create today's snapshot
try {
  $response = Invoke-RestMethod -Uri "$appUrl/api/daily-picks?type=all" -Method Get -TimeoutSec 30 -ErrorAction Stop
  $top3Count = if ($response.top3Count) { $response.top3Count } else { 0 }
  $premiumCount = if ($response.premiumCount) { $response.premiumCount } else { 0 }
  Log "Daily picks snapshot created: $top3Count top-3 picks, $premiumCount premium picks for $($response.date)"
} catch {
  Log "Could not reach web app API (is it running?): $_"
  Log "Daily picks will be snapshotted on first web app load today."
}

Log "=== Daily collection complete ==="
Log ""
