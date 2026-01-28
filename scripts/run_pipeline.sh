#!/usr/bin/env bash
set -euo pipefail

# run_pipeline.sh [mode]
# mode: "full" = fetch per-recipe details (default), "lite" = skip per-recipe details
# Examples:
#   ./run_pipeline.sh lite
#   ./run_pipeline.sh full

MODE=${1:-lite}
if [ "$MODE" = "full" ]; then
  echo "Running full pipeline (will fetch per-recipe details)..."
  export FETCH_RECIPE_DETAILS=1
else
  echo "Running lite pipeline (skip per-recipe detail fetch)..."
  export FETCH_RECIPE_DETAILS=0
fi

# Activate venv if present
if [ -f "venv/bin/activate" ]; then
  # shellcheck source=/dev/null
  . venv/bin/activate
fi

echo "Initializing database (idempotent)..."
python scripts/init_sqlite.py

echo "Running bronze ingestion..."
python scripts/1_bronze.py

echo "Running silver transformation..."
python scripts/2_silver.py

echo "Running gold analytics..."
python scripts/3_gold_analytics.py

echo "Pipeline complete. Check hfresh/hfresh.db and hfresh/output/"
