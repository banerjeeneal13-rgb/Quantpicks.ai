# run_all.ps1
$ErrorActionPreference = "Stop"

cd $PSScriptRoot
cd ..

# activate venv
.\.venv\Scripts\Activate.ps1

cd src

Write-Host "1) Build feature cache"
python build_points_feature_cache.py

Write-Host "2) Ingest odds"
python ingest_odds.py

Write-Host "3) Cleanup old edges (24h)"
python cleanup_edges.py

Write-Host "DONE"
