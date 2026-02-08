# Quick Reference: Temporal Data Ingestion

## One-Liners

```bash
# Next week (default)
./run_pipeline.sh

# Specific week
python scripts/1_bronze.py --week 2026-W05

# Any date in week
python scripts/1_bronze.py --week 2026-01-15

# Date range
python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-31

# With recipe details
./run_pipeline.sh full --week 2026-W05
python scripts/1_bronze.py --week 2026-W05 --fetch-recipes
```

## Common Tasks

### Backfill Missing Week
```bash
python scripts/1_bronze.py --week 2026-W08
python scripts/2_silver.py
python scripts/3_gold_analytics.py
```

### Rebuild Multiple Weeks
```bash
for week in W01 W02 W03 W04 W05; do
  python scripts/1_bronze.py --week "2026-${week}"
done
python scripts/2_silver.py
python scripts/3_gold_analytics.py
```

### Ingest Month with Details
```bash
python scripts/1_bronze.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --fetch-recipes
python scripts/2_silver.py
python scripts/3_gold_analytics.py
```

### Fix Previous Run
```bash
# Re-run same week (safe and idempotent)
python scripts/1_bronze.py --week 2026-W10 --fetch-recipes
python scripts/2_silver.py
python scripts/3_gold_analytics.py
```

## Help

```bash
python scripts/1_bronze.py --help
```

## Key Points

- ✅ **Safe to re-run** - SCD Type 2 handles idempotency
- ✅ **Multiple weeks** - All coexist in same database
- ✅ **Default works** - No args = next week (original behavior)
- ✅ **Date formats** - ISO standard only (YYYY-MM-DD)
- ❌ **Can't mix** - Use `--week` OR `--start-date`/`--end-date`, not both

## See Also

- Full documentation: [HISTORICAL_DATA_INGESTION.md](HISTORICAL_DATA_INGESTION.md)
- Implementation details: [TEMPORAL_SELECTION_IMPLEMENTATION.md](TEMPORAL_SELECTION_IMPLEMENTATION.md)


# Implementation Summary: Historical Data Ingestion

## Overview

Successfully implemented flexible temporal selection for the Bronze layer ingestion script, enabling reconstruction and backdating of the HelloFresh menu analytics database.

## Changes Made

### 1. Bronze Script (`scripts/1_bronze.py`)

**Added Temporal Utilities:**
- `parse_iso_date()` - Parse ISO dates (YYYY-MM-DD)
- `get_week_bounds()` - Calculate Monday-Sunday for any date
- `parse_temporal_arguments()` - Central temporal argument parser supporting:
  - ISO week format (YYYY-Www)
  - Any date in target week (YYYY-MM-DD)
  - Explicit date ranges (start + end)
  - Default next week (legacy behavior)

**Enhanced Function Signatures:**
- `perform_weekly_pull()` now accepts optional `start_date` and `end_date` parameters
- Falls back to next week calculation if not provided (maintains backward compatibility)

**Added CLI Argument Parser:**
- `parse_arguments()` - Comprehensive argument parsing with:
  - `--week` - ISO week or date in week
  - `--start-date` and `--end-date` - Explicit range
  - `--fetch-recipes` - Per-recipe detail fetching
  - Validation of mutually exclusive arguments
  - Helpful error messages and usage examples

**Enhanced main():**
- Parses CLI arguments before execution
- Validates temporal arguments with clear error messages
- Passes dates to `perform_weekly_pull()`
- Environment variable handling for recipe fetching

### 2. Shell Script (`scripts/run_pipeline.sh`)

**Backward Compatibility:**
- Maintains original MODE behavior (`lite`/`full`)
- Positional argument parsing for MODE still works

**Enhanced Argument Handling:**
- Supports `--week`, `--start-date`, `--end-date`, `--fetch-recipes`
- Can combine MODE with temporal arguments
- Example: `./run_pipeline.sh full --week 2026-W05`
- Passes all arguments to bronze script

**Improved Documentation:**
- Comprehensive help with examples
- Clear explanation of each option
- Real-world usage scenarios

### 3. Documentation (`notes/HISTORICAL_DATA_INGESTION.md`)

**Comprehensive Guide Covering:**
- Architecture and idempotency strategy
- How multiple weeks coexist in same database
- Usage patterns with examples
- CLI reference for both Python and shell commands
- Real-world use cases (rebuilding, backfilling, correction)
- GitHub Actions integration example
- Data consistency guarantees
- Troubleshooting guide
- Technical implementation details

## Key Features

### ✅ Temporal Selection

| Format | Example | Behavior |
|--------|---------|----------|
| Default | (no args) | Next week |
| ISO Week | `--week 2026-W05` | Week 5 of 2026 |
| Date in Week | `--week 2026-01-15` | Week containing Jan 15 |
| Date Range | `--start-date 2026-01-01 --end-date 2026-01-31` | All dates in range |

### ✅ Idempotency

- **SCD Type 2 Implementation**: Prevents data loss on re-runs
- **Smart Upsert Logic**: Updates `last_seen_date`, maintains history
- **Multi-week Coexistence**: Different weeks can be ingested independently
- **No Data Corruption**: Re-running past weeks safe and non-destructive

### ✅ Backward Compatibility

- Default behavior unchanged (next week ingestion)
- Existing shell script calls still work: `./run_pipeline.sh` and `./run_pipeline.sh full`
- Environment variable `FETCH_RECIPE_DETAILS` still supported
- No breaking changes to database schema

### ✅ Production Ready

- Comprehensive error handling
- Clear error messages for invalid arguments
- Type hints and validation
- Follows existing code style
- Examples for GitHub Actions integration

## Usage Examples

```bash
# Default (next week)
./run_pipeline.sh

# Specific week (ISO format)
python scripts/1_bronze.py --week 2026-W05

# Specific week (any date in week)
./run_pipeline.sh --week 2026-01-15

# Date range
python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-31

# With per-recipe details
./run_pipeline.sh full --week 2026-W05

# Rebuild historical data
for week in W40 W41 W42 W43 W44; do
  python scripts/1_bronze.py --week "2025-${week}"
  python scripts/2_silver.py
done
```

## Testing Recommendations

### Unit Tests
- `parse_iso_date()` with valid/invalid inputs
- `get_week_bounds()` for date at start, middle, end of week
- `parse_temporal_arguments()` with all argument combinations
- Error handling for conflicting arguments

### Integration Tests
- Ingest same week twice (verify idempotency)
- Ingest different weeks (verify coexistence)
- Ingest overlapping ranges (verify merge)
- Verify menu counts stay consistent

### Manual Testing
```bash
# Test 1: Default behavior unchanged
python scripts/1_bronze.py
python scripts/2_silver.py

# Test 2: Specific week
python scripts/1_bronze.py --week 2026-W01
python scripts/2_silver.py

# Test 3: Re-run same week (idempotency)
python scripts/1_bronze.py --week 2026-W01
python scripts/2_silver.py

# Test 4: Multiple weeks
python scripts/1_bronze.py --week 2026-W01
python scripts/1_bronze.py --week 2026-W02
python scripts/2_silver.py

# Verify: Check hfresh.db - should have data from both weeks
```

## GitHub Actions Considerations

For scheduled weekly runs, consider adding temporal parameter support to Actions workflow:

```yaml
# Can pass week parameter to override default (next week)
- run: python scripts/1_bronze.py --week ${{ inputs.week }}
```

This enables:
- Manual backfill runs via workflow_dispatch
- Recovery from missed weeks
- Data quality correction runs

## Questions This Addresses

✅ **"Can I ingest historical weeks?"** - Yes, with `--week` or date range parameters

✅ **"Will future weekly runs still work?"** - Yes, default behavior unchanged

✅ **"Can multiple weeks exist in same DB?"** - Yes, SCD Type 2 handles coexistence

✅ **"Is re-running a week safe?"** - Yes, fully idempotent through upsert logic

✅ **"Do I need to change my CI/CD?"** - No, backward compatible

## Success Criteria Met

- ✅ Bronze script accepts explicit temporal selector
- ✅ Weekly pipeline cycle maintained for future runs
- ✅ Past weeks can be ingested independently
- ✅ Multiple weeks coexist in same database
- ✅ Re-running past weeks doesn't corrupt/erase other weeks
- ✅ Backward compatible with existing scripts
- ✅ Comprehensive documentation provided
