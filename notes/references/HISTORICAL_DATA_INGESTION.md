# Historical Data Ingestion & Temporal Selection

## Overview

The Bronze layer ingestion script (`scripts/1_bronze.py`) now supports flexible temporal selection, enabling you to ingest HelloFresh menu data for any historical week or date range. This allows you to:

- **Rebuild the database incrementally** from historical weekly slices
- **Maintain weekly cycle execution** for production/scheduled runs
- **Coexist multiple weeks** in the same `.db` file without data corruption
- **Re-run past weeks** safely without affecting other weeks' data

## Architecture

### Idempotency Strategy

The pipeline maintains data integrity through:

1. **SCD Type 2 Tracking**: The Silver layer (normalization) uses Slowly Changing Dimension Type 2 to track entity versions with `first_seen_date` and `last_seen_date`

2. **Append-Only Bronze**: Raw API responses are stored with a `pull_date` field (the date the pull was executed)

3. **Smart Upsert Logic**: When the same entity appears in multiple pulls:
   - First occurrence: Creates new record
   - Subsequent occurrences: Updates `last_seen_date` and marks as `is_active = 1`
   - This ensures no data loss and full change tracking

### Example: Multiple Weeks Coexisting

```
Database: hfresh.db
├─ api_responses (Bronze - append-only)
│  ├─ pull_date: 2026-01-15 → menus for week of 2026-01-13
│  ├─ pull_date: 2026-01-22 → menus for week of 2026-01-20
│  ├─ pull_date: 2026-01-15 (rerun) → same menus (idempotent update)
│  └─ pull_date: 2026-02-01 → menus for week of 2026-01-27
│
└─ recipes, ingredients, menus (Silver - SCD Type 2 normalized)
   └─ All weeks' data coexist with change history
```

## Usage

### 1. Default Behavior (Next Week)

Maintains the original behavior - ingests next week's menus:

```bash
# Via shell script
./run_pipeline.sh

# Direct Python
python scripts/1_bronze.py
```

### 2. Specific Week (ISO Format)

Ingest a specific week using ISO week format `YYYY-Www`:

```bash
# Ingest week 5 of 2026 (Jan 26 - Feb 1)
python scripts/1_bronze.py --week 2026-W05

# Via shell script
./run_pipeline.sh --week 2026-W05
```

**ISO Week Notes:**
- Week 1 = first week with a Thursday in the year
- Weeks start on Monday, end on Sunday
- Week 01 might start in December of previous year

### 3. Specific Date (in Target Week)

Provide any date in the target week; the script calculates Monday-Sunday bounds:

```bash
# Any date in week of Jan 13-19, 2026
python scripts/1_bronze.py --week 2026-01-15

# Via shell script
./run_pipeline.sh --week 2026-01-15
```

### 4. Date Range

Ingest a specific range of dates (inclusive):

```bash
# Entire January 2026
python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-31

# Multi-week range
python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-02-28
```

### 5. With Per-Recipe Details

Combine temporal selection with per-recipe fetching (slower but more complete):

```bash
# Specific week with recipe details
python scripts/1_bronze.py --week 2026-W05 --fetch-recipes

# Via shell script
./run_pipeline.sh --week 2026-W05 --fetch-recipes

# Or use MODE + temporal args
./run_pipeline.sh full --week 2026-W05
```

## CLI Reference

### `1_bronze.py` Command Options

```
usage: 1_bronze.py [-h] [--week WEEK] [--start-date START_DATE] 
                    [--end-date END_DATE] [--fetch-recipes]

optional arguments:
  -h, --help              Show help message
  
  --week WEEK             Target week in ISO format (YYYY-Www) or 
                          any date in the week (YYYY-MM-DD)
                          Example: 2026-W05 or 2026-01-15
                          
  --start-date START_DATE Start date for range (YYYY-MM-DD)
                          Must be used with --end-date
                          
  --end-date END_DATE     End date for range (YYYY-MM-DD)
                          Must be used with --start-date
                          
  --fetch-recipes         Fetch per-recipe details
                          (slower, more complete data)
```

### `run_pipeline.sh` Command Options

```
usage: run_pipeline.sh [MODE] [OPTIONS]

MODE (optional, default: "lite"):
  lite    Skip per-recipe details (faster)
  full    Fetch per-recipe details (slower)

OPTIONS (optional):
  --week WEEK             Target week (see examples below)
  --start-date DATE       Start of date range
  --end-date DATE         End of date range
  --fetch-recipes         Fetch per-recipe details
```

### Examples

```bash
# Next week, lite mode (original behavior)
./run_pipeline.sh
./run_pipeline.sh lite

# Next week, full mode
./run_pipeline.sh full

# Specific week, lite mode
./run_pipeline.sh --week 2026-W05
./run_pipeline.sh --week 2026-01-15

# Specific week, full mode
./run_pipeline.sh full --week 2026-W05
./run_pipeline.sh --week 2026-W05 --fetch-recipes

# Date range, lite mode
./run_pipeline.sh --start-date 2026-01-01 --end-date 2026-01-31

# Date range, full mode
./run_pipeline.sh full --start-date 2026-01-01 --end-date 2026-02-28

# Direct Python calls
python scripts/1_bronze.py --week 2026-W05
python scripts/1_bronze.py --week 2026-W05 --fetch-recipes
python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-07
```

## Use Cases

### Rebuilding Historical Database

```bash
# Start from a known good point and rebuild incrementally
# (e.g., back to start of October 2025)

for week in W40 W41 W42 W43 W44 W45 W46 W47 W48 W49 W50 W51 W52 W01; do
  python scripts/1_bronze.py --week "2025-${week}"
  python scripts/2_silver.py
  python scripts/3_gold_analytics.py
done
```

### Backfilling Missing Weeks

```bash
# If a scheduled run failed, backfill that week
python scripts/1_bronze.py --week 2026-W08
python scripts/2_silver.py
python scripts/3_gold_analytics.py
```

### Correcting Data Quality Issues

```bash
# Re-ingest a week that had API issues
python scripts/1_bronze.py --week 2026-W10 --fetch-recipes
python scripts/2_silver.py
python scripts/3_gold_analytics.py

# The SCD Type 2 logic will update records, no data loss
```

### Scheduled Production Run (GitHub Actions)

```yaml
# .github/workflows/weekly-ingest.yml
name: Weekly Data Ingest

on:
  schedule:
    - cron: '0 2 * * 1'  # Every Monday at 2am UTC

jobs:
  ingest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run pipeline (next week)
        env:
          HELLOFRESH_API_KEY: ${{ secrets.HELLOFRESH_API_KEY }}
        run: ./run_pipeline.sh full
      
      - name: Commit changes
        run: |
          git add hfresh/hfresh.db docs/weekly_reports/
          git commit -m "Weekly data ingest - $(date +%Y-W%V)" || true
          git push
```

## Data Consistency

### Safe to Re-run?

✅ **Yes!** Re-running the same `pull_date` is safe:
- Bronze: Duplicate `api_responses` entries occur but silver layer deduplicates via entity ID
- Silver: SCD Type 2 upsert updates existing records, no data loss
- Gold: Analytics recalculated from updated silver layer

### Idempotent Operations

| Operation | Idempotent? | Result |
|-----------|-----------|--------|
| Run same week twice | ✅ Yes | Second run updates `last_seen_date` |
| Run different weeks | ✅ Yes | All weeks coexist |
| Re-run with `--fetch-recipes` | ✅ Yes | Recipe details updated |
| Run overlapping date ranges | ✅ Yes | Records merged/updated |

### What Gets Updated?

```sql
-- When re-ingesting a week:

recipes:
  - is_active remains 1
  - last_seen_date updated to new pull_date
  - All other fields updated with latest data

menus:
  - Same behavior as recipes
  - Menu-recipe links updated

reference_data (ingredients, allergens, etc.):
  - Only updated if first run (reference baseline)
  - Subsequent runs skip reference data (no re-fetching)
```

## Troubleshooting

### Error: Invalid date format

```
Error: Invalid date format: 2026/01/15. Expected YYYY-MM-DD
```

**Fix:** Use ISO format with hyphens: `2026-01-15`

### Error: Cannot use --week with --start-date/--end-date

```
Error: Cannot use --week with --start-date/--end-date. Choose one.
```

**Fix:** Use either `--week` OR `--start-date`/`--end-date`, not both

### Error: start_date must be before end_date

```
Error: start_date must be before end_date: 2026-01-31 > 2026-01-01
```

**Fix:** Ensure `--start-date` ≤ `--end-date`

### Large Date Ranges Running Slowly

If ingesting many weeks (e.g., 52 weeks), consider:

1. Run without `--fetch-recipes` first (faster baseline)
2. Add `--fetch-recipes` for critical weeks only
3. Ingest in batches with progress checks

```bash
# Ingest in monthly batches with full details
for month in 01 02 03 04 05 06; do
  python scripts/1_bronze.py \
    --start-date "2026-${month}-01" \
    --end-date "2026-${month}-28" \
    --fetch-recipes
  echo "Completed month $month"
done
```

## Technical Details

### Temporal Parsing

- **ISO Week Format** (`YYYY-Www`): Follows ISO 8601 standard
- **Date Format** (`YYYY-MM-DD`): ISO 8601 date
- **Week Bounds**: Calculated as Monday (day 0) to Sunday (day 6) at UTC
- **Default Behavior**: Uses system `datetime.now()` to calculate next week

### Database Schema Impact

```sql
-- Bronze Layer (unchanged)
api_responses:
  - pull_date (TEXT) ← used to separate pulls
  - endpoint (TEXT)
  - locale (TEXT)
  - page_number (INTEGER)
  - payload (TEXT - JSON)
  - ingestion_timestamp (TEXT)

-- Silver Layer (SCD Type 2)
recipes, menus, ingredients, etc:
  - first_seen_date (TEXT) ← when first seen
  - last_seen_date (TEXT) ← last pull_date it appeared
  - is_active (INTEGER) ← 1 if in latest pull
  - _ingestion_ts (TEXT)
```

## Future Enhancements

Potential improvements for future versions:

- [ ] Support for specifying both week AND detailed recipe fetching in shell script
- [ ] Batch ingestion with progress tracking
- [ ] Parallel processing for large date ranges
- [ ] Selective reference data updates
- [ ] Historical data export/archival
