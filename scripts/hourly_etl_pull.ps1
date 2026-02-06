$ErrorActionPreference = "Stop"

$repo = "C:\Users\baner\quantpicks-ai"
$etlDir = Join-Path $repo "nba_etl_pkg"
$modelSrc = Join-Path $repo "services\model\src"
$etlOut = Join-Path $etlDir "nba_etl_output"
$logDir = Join-Path $repo "logs"
$logPath = Join-Path $logDir "hourly_etl_pull.log"
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
    $rotated = Join-Path $logDir ("hourly_etl_pull_" + $stamp + ".log")
    Move-Item $logPath $rotated -Force
  }
}
try {
  Add-Content -Path $logPath -Value ("[bootstrap] " + (Get-Date).ToString("s") + " starting hourly ETL")
} catch {}
try {
  Start-Transcript -Path $logPath -Append | Out-Null
} catch {}

# Pull last 3 days only (lightweight hourly refresh)
$end = (Get-Date).ToString("yyyy-MM-dd")
$start = (Get-Date).AddDays(-3).ToString("yyyy-MM-dd")

Push-Location $etlDir
& $python -m nba_etl.cli --log-level warning fetch `
  --seasons 2025-26 `
  --season-type "Regular Season" `
  --out-dir $etlOut `
  --date-start $start `
  --date-end $end `
  --chunk-days 3 `
  --auto-loop `
  --cache-ttl 600 `
  --timeout 20 `
  --max-retries 1 `
  --backoff 1.0
Pop-Location

# Update model logs/advanced features (no retrain hourly)
& $modelPython (Join-Path $modelSrc "ingest_nba_etl_boxscores.py")
& $modelPython (Join-Path $modelSrc "fetch_espn_boxscores.py")

# Refresh injuries + feature cache for latest availability context
& $modelPython (Join-Path $modelSrc "fetch_espn_injuries.py")
& $modelPython (Join-Path $modelSrc "fetch_nba_team_defense_zones.py")
& $modelPython (Join-Path $modelSrc "build_points_feature_cache.py")

try {
  Stop-Transcript | Out-Null
} catch {}
try {
  Add-Content -Path $logPath -Value ("[bootstrap] " + (Get-Date).ToString("s") + " finished hourly ETL")
} catch {}
