#!/usr/bin/env pwsh

# Rebuild the HelloFresh database from scratch, pulling as much data as possible from the API.
# Usage:
#   .\rebuild_db.ps1
#   .\rebuild_db.ps1 -DateRange "2025-09-01", "2026-04-03"

param(
    [switch]$FetchRecipes = $true,
    [string[]]$DateRange
)

$ErrorActionPreference = "Stop"

# Get project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

Write-Host "Rebuilding HelloFresh Database from scratch..." -ForegroundColor Cyan
Write-Host "Project root: $projectRoot" -ForegroundColor Gray
Write-Host ""

# Remove existing database
$dbPath = Join-Path $projectRoot "hfresh\hfresh.db"
if (Test-Path $dbPath) {
    Write-Host "Removing existing database..." -ForegroundColor Yellow
    Remove-Item $dbPath -Force
    Write-Host "   Deleted: $dbPath" -ForegroundColor Green
    Write-Host ""
}

# Activate venv if present
$venvPath = Join-Path $projectRoot "venv\Scripts\Activate.ps1"
if (Test-Path $venvPath) {
    Write-Host "Activating virtual environment..." -ForegroundColor Cyan
    & $venvPath
    Write-Host ""
}

# Initialize database
Write-Host "Initializing database schema..." -ForegroundColor Cyan
python scripts/init_sqlite.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Database initialization failed" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Build bronze args
$bronzeArgs = @()

# Always fetch recipes for rebuild to get maximum data
if ($FetchRecipes) {
    $bronzeArgs += "--fetch-recipes"
    Write-Host "Running bronze ingestion with per-recipe details (FULL MODE)..." -ForegroundColor Cyan
} else {
    Write-Host "Running bronze ingestion (lite mode)..." -ForegroundColor Cyan
}

# Add date range if specified
if ($DateRange -and $DateRange.Count -eq 2) {
    $bronzeArgs += "--start-date"
    $bronzeArgs += $DateRange[0]
    $bronzeArgs += "--end-date"
    $bronzeArgs += $DateRange[1]
    Write-Host "   Date range: $($DateRange[0]) to $($DateRange[1])" -ForegroundColor Gray
}

# Start monitoring job
Write-Host ""
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
                Write-Host "[Monitor] API Responses in Bronze: $result records" -ForegroundColor Yellow
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

# Run bronze ingestion
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
Write-Host "   Total API responses stored: $finalCount records" -ForegroundColor Cyan
Write-Host ""

# Run silver transformation
Write-Host "Running silver transformation..." -ForegroundColor Cyan
python scripts/2_silver.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Silver transformation failed" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Run gold analytics
Write-Host "Running gold analytics layer..." -ForegroundColor Cyan
python scripts/3_gold_analytics.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Gold analytics failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Database rebuild complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Database location: $dbPath" -ForegroundColor Cyan
$outputPath = Join-Path $projectRoot "hfresh\output"
Write-Host "Output charts: $outputPath" -ForegroundColor Cyan
Write-Host ""
