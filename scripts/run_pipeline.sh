#!/usr/bin/env bash
set -euo pipefail

# run_pipeline.sh [OPTIONS]
#
# Run the HelloFresh data pipeline with flexible options.
#
# Options:
#   MODE (positional, default: "lite"):
#     "lite"   = skip per-recipe details
#     "full"   = fetch per-recipe details
#
#   --week WEEK
#     Ingest specific week: ISO format (YYYY-Www) or date (YYYY-MM-DD)
#     Example: --week 2026-W05 or --week 2026-01-15
#
#   --start-date DATE and --end-date DATE
#     Ingest date range (YYYY-MM-DD format)
#     Example: --start-date 2026-01-01 --end-date 2026-01-31
#
#   --fetch-recipes
#     Fetch per-recipe details (can be combined with --week or date range)
#
# Examples:
#   ./run_pipeline.sh                           # next week, lite
#   ./run_pipeline.sh full                      # next week, full
#   ./run_pipeline.sh --week 2026-W05           # specific week, lite
#   ./run_pipeline.sh --week 2026-W05 --fetch-recipes  # specific week, full
#   ./run_pipeline.sh --start-date 2026-01-01 --end-date 2026-01-31  # date range

# Parse arguments
MODE="lite"
BRONZE_ARGS=""

# Check if first argument looks like MODE (no dashes) or is a flag
if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
  MODE="$1"
  shift
fi

# Collect remaining arguments for bronze script
while [[ $# -gt 0 ]]; do
  BRONZE_ARGS="$BRONZE_ARGS $1"
  shift
done

# Set fetch mode
if [ "$MODE" = "full" ]; then
  echo "Running with per-recipe details..."
  BRONZE_ARGS="$BRONZE_ARGS --fetch-recipes"
else
  echo "Running lite pipeline (skip per-recipe detail fetch)..."
fi

# Activate venv if present
if [ -f "venv/bin/activate" ]; then
  # shellcheck source=/dev/null
  . venv/bin/activate
fi

echo "Initializing database (idempotent)..."
python scripts/init_sqlite.py

echo "Running bronze ingestion..."
# shellcheck disable=SC2086
python scripts/1_bronze.py $BRONZE_ARGS

echo "Running silver transformation..."
python scripts/2_silver.py

echo "Running gold analytics..."
python scripts/3_gold_analytics.py

echo "Pipeline complete. Check hfresh/hfresh.db and hfresh/output/"
