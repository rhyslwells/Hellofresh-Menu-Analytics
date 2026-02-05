#!/usr/bin/env pwsh

# Append new data to existing HelloFresh database without deleting it.
# Fetches data from API for the specified date range and adds it to the existing database.
# Then reruns silver and gold layers to incorporate the new data.
#
# Usage:
#   .\append_db.ps1 -DateRange "2026-02-01", "2026-04-03"
#   .\append_db.ps1 -DateRange "2026-02-01", "2026-04-03" -SkipRecipes

param(
    [Parameter(Mandatory=$true)]
    [string[]]$DateRange,
    
    [switch]$FetchRecipes = $true
)

$ErrorActionPreference = "Stop"

# Get project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

if ($DateRange.Count -ne 2) {
    Write-Host "ERROR: DateRange must have exactly 2 values (start and end date)" -ForegroundColor Red
    exit 1
}

Write-Host "Appending new data to HelloFresh Database..." -ForegroundColor Cyan
Write-Host "Project root: $projectRoot" -ForegroundColor Gray
Write-Host ""

# Check if database exists
$dbPath = Join-Path $projectRoot "hfresh\hfresh.db"
if (-not (Test-Path $dbPath)) {
    Write-Host "ERROR: Database not found at $dbPath" -ForegroundColor Red
    Write-Host "Use rebuild_db.ps1 to create a fresh database first" -ForegroundColor Yellow
    exit 1
}
Write-Host "Existing database found: $dbPath" -ForegroundColor Green
Write-Host ""

# Activate venv if present
$venvPath = Join-Path $projectRoot "venv\Scripts\Activate.ps1"
if (Test-Path $venvPath) {
    Write-Host "Activating virtual environment..." -ForegroundColor Cyan
    & $venvPath
    Write-Host ""
}

# Build bronze args
$bronzeArgs = @("--start-date", $DateRange[0], "--end-date", $DateRange[1])

if ($FetchRecipes) {
    $bronzeArgs += "--fetch-recipes"
    Write-Host "Running bronze ingestion with per-recipe details (FULL MODE)..." -ForegroundColor Cyan
} else {
    Write-Host "Running bronze ingestion (lite mode)..." -ForegroundColor Cyan
}

Write-Host "   Date range: $($DateRange[0]) to $($DateRange[1])" -ForegroundColor Gray
Write-Host ""

# Start monitoring job
Write-Host "Starting API response monitor..." -ForegroundColor Cyan

$monitorJob = Start-Job -ScriptBlock {
    param($dbPath, $projectRoot)
    
    $sqlitePath = "sqlite3"
    $lastCount = 0
    
    while ($true) {
        try {
            $result = & $sqlitePath $dbPath "SELECT COUNT(*) FROM api_responses" 2>$null
            if ($result -and $result -ne $lastCount) {
                $lastCount = $result
                Write-Host "[Monitor] Total API Responses in Bronze: $result records" -ForegroundColor Yellow
            }
        } catch {
            # Silently skip errors during early startup
        }
        Start-Sleep -Seconds 2
    }
} -ArgumentList $dbPath, $projectRoot

Write-Host "Monitor started (PID: $($monitorJob.Id))" -ForegroundColor Gray
Write-Host ""
Write-Host "Bronze ingestion in progress..." -ForegroundColor Cyan
Write-Host "---" -ForegroundColor Gray

# Run bronze ingestion (appends to existing data)
python scripts/1_bronze.py $bronzeArgs
$bronzeExitCode = $LASTEXITCODE

# Stop monitoring job
Stop-Job -Job $monitorJob -ErrorAction SilentlyContinue
Remove-Job -Job $monitorJob -ErrorAction SilentlyContinue

Write-Host "---" -ForegroundColor Gray

if ($bronzeExitCode -ne 0) {
    Write-Host "ERROR: Bronze ingestion failed" -ForegroundColor Red
    exit 1
}

# Show final count
Write-Host "Bronze ingestion complete" -ForegroundColor Green
$finalCount = & sqlite3 $dbPath "SELECT COUNT(*) FROM api_responses"
Write-Host "   Total API responses in database: $finalCount records" -ForegroundColor Cyan
Write-Host ""

# Run silver transformation (reprocess with new data)
Write-Host "Running silver transformation (reprocessing all data)..." -ForegroundColor Cyan
python scripts/2_silver.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Silver transformation failed" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Run gold analytics (rebuild with updated data)
Write-Host "Running gold analytics layer (rebuilding analytics)..." -ForegroundColor Cyan
python scripts/3_gold_analytics.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Gold analytics failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Database append complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Database location: $dbPath" -ForegroundColor Cyan
$outputPath = Join-Path $projectRoot "hfresh\output"
Write-Host "Output charts: $outputPath" -ForegroundColor Cyan
Write-Host ""
