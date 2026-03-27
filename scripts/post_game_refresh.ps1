$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
  Post-game auto-refresh scheduler.

  Reads today's game times from the Supabase edges table, then schedules
  a data refresh (ETL + ROI recalc) 3 hours after each game's start time.
  This ensures results are captured and ROI is updated ASAP after games end.

  Run this at the start of each day (e.g. 8 AM via Task Scheduler).
#>

$repo = "C:\Users\baner\Dropbox\Quantpicks.ai"
$modelSrc = Join-Path $repo "services\model\src"
$etlDir = Join-Path $repo "nba_etl_pkg"
$etlOut = Join-Path $etlDir "nba_etl_output"
$logDir = Join-Path $repo "logs"
$logPath = Join-Path $logDir "post_game_refresh.log"
$venvPython = Join-Path $repo "services\model\.venv\Scripts\python.exe"
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) { $python = "python" }
$modelPython = $python
if (Test-Path $venvPython) { $modelPython = $venvPython }

# Supabase config — read from model .env
$envFile = Join-Path $repo "services\model\.env"
$supabaseUrl = ""
$supabaseKey = ""
if (Test-Path $envFile) {
  Get-Content $envFile | ForEach-Object {
    if ($_ -match "^SUPABASE_URL=(.+)$") { $supabaseUrl = $Matches[1].Trim() }
    if ($_ -match "^SUPABASE_SERVICE_ROLE_KEY=(.+)$") { $supabaseKey = $Matches[1].Trim() }
  }
}
# Fallback: try web app env
$webEnv = Join-Path $repo "apps\web\.env.local"
if ((-not $supabaseUrl) -and (Test-Path $webEnv)) {
  Get-Content $webEnv | ForEach-Object {
    if ($_ -match "^SUPABASE_URL=(.+)$") { $supabaseUrl = $Matches[1].Trim() }
    if ($_ -match "^SUPABASE_SERVICE_ROLE_KEY=(.+)$") { $supabaseKey = $Matches[1].Trim() }
  }
}

if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}

function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "[$ts] $msg"
  Write-Host $line
  Add-Content -Path $logPath -Value $line -ErrorAction SilentlyContinue
}

function Run-PostGameRefresh {
  Log "Running post-game ETL refresh..."

  $end = (Get-Date).ToString("yyyy-MM-dd")
  $start = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")

  # 1) Pull latest box scores
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
    Log "ETL fetch error: $_"
  }
  Pop-Location

  # 2) Ingest box scores + update features
  try {
    & $modelPython (Join-Path $modelSrc "ingest_nba_etl_boxscores.py")
    & $modelPython (Join-Path $modelSrc "fetch_espn_boxscores.py")
    Log "Box score ingest complete."
  } catch {
    Log "Box score ingest error: $_"
  }

  # 3) Refresh feature cache for ROI calculations
  try {
    & $modelPython (Join-Path $modelSrc "fetch_espn_injuries.py")
    & $modelPython (Join-Path $modelSrc "build_points_feature_cache.py")
    Log "Feature cache rebuilt."
  } catch {
    Log "Feature cache error: $_"
  }

  Log "Post-game refresh complete."
}

# ── Main: fetch today's game times and schedule refreshes ─────────────────

Log "=== Post-game refresh scheduler starting ==="

$today = (Get-Date).ToString("yyyy-MM-dd")

# Fetch game start times from Supabase edges table
$gameTimes = @()

if ($supabaseUrl -and $supabaseKey) {
  try {
    $headers = @{
      "apikey"        = $supabaseKey
      "Authorization" = "Bearer $supabaseKey"
    }
    # NBA games for "today" in US span from ~23:00 UTC (7PM ET) to ~07:00 UTC next day (1AM ET late West Coast)
    # In AEST (UTC+10) that's roughly 9 AM to 5 PM same day
    $todayStart = "${today}T00:00:00+10:00"
    $tomorrowStart = "${today}T20:00:00+10:00"

    $apiUrl = "$supabaseUrl/rest/v1/edges?select=starts_at&starts_at=gte.$todayStart&starts_at=lt.$tomorrowStart&limit=200"
    $response = Invoke-RestMethod -Uri $apiUrl -Headers $headers -Method Get -ErrorAction Stop

    # Extract unique game start times
    $uniqueTimes = @{}
    foreach ($row in $response) {
      if ($row.starts_at) {
        $startTime = [datetime]::Parse($row.starts_at)
        $roundedKey = $startTime.ToString("yyyy-MM-dd HH:00") # Round to hour
        if (-not $uniqueTimes.ContainsKey($roundedKey)) {
          $uniqueTimes[$roundedKey] = $startTime
        }
      }
    }
    $gameTimes = $uniqueTimes.Values | Sort-Object
    Log "Found $($gameTimes.Count) unique game time slots today."
  } catch {
    Log "Failed to fetch game times from Supabase: $_"
  }
} else {
  Log "No Supabase credentials found. Using default schedule."
}

# If we couldn't get game times, use typical NBA schedule slots in AEST
# 7 PM ET = 9 AM AEST, 7:30 PM ET = 9:30 AM, 8 PM ET = 10 AM,
# 9 PM ET = 11 AM, 10 PM ET = 12 PM, 10:30 PM PT = 3 PM AEST
if ($gameTimes.Count -eq 0) {
  $gameTimes = @(
    [datetime]::Parse("$today 09:00"),  # 7 PM ET  -> 9 AM AEST
    [datetime]::Parse("$today 09:30"),  # 7:30 PM ET -> 9:30 AM AEST
    [datetime]::Parse("$today 10:00"),  # 8 PM ET  -> 10 AM AEST
    [datetime]::Parse("$today 11:00"),  # 9 PM ET  -> 11 AM AEST
    [datetime]::Parse("$today 12:00"),  # 10 PM ET -> 12 PM AEST
    [datetime]::Parse("$today 15:00")   # 10:30 PM PT (1 AM ET) -> 3 PM AEST
  )
  Log "Using default game time slots (AEST): $($gameTimes.Count) slots."
}

# Schedule refreshes 3 hours after each game start
$refreshTimes = @()
foreach ($gameTime in $gameTimes) {
  $refreshTime = $gameTime.AddHours(3)

  # Only schedule if the refresh time is in the future
  if ($refreshTime -gt (Get-Date)) {
    $refreshTimes += $refreshTime
    Log "Scheduled refresh at $($refreshTime.ToString('HH:mm')) (game at $($gameTime.ToString('HH:mm')))"
  } else {
    Log "Skipping past refresh time: $($refreshTime.ToString('HH:mm'))"
  }
}

# Deduplicate refresh times within 30-minute windows
$deduped = @()
$lastTime = $null
foreach ($rt in ($refreshTimes | Sort-Object)) {
  if ($null -eq $lastTime -or ($rt - $lastTime).TotalMinutes -ge 30) {
    $deduped += $rt
    $lastTime = $rt
  }
}
$refreshTimes = $deduped
Log "After dedup: $($refreshTimes.Count) refresh slots."

# Wait and execute each refresh
foreach ($refreshTime in $refreshTimes) {
  $waitSeconds = [math]::Max(0, ($refreshTime - (Get-Date)).TotalSeconds)

  if ($waitSeconds -gt 0) {
    Log "Waiting $([math]::Round($waitSeconds / 60, 1)) minutes until $($refreshTime.ToString('HH:mm'))..."
    Start-Sleep -Seconds $waitSeconds
  }

  Run-PostGameRefresh
}

Log "=== Post-game refresh scheduler finished ==="
