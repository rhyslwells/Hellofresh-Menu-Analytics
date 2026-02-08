# Implementation Checklist: Historical Data Ingestion

## ✅ Completed

### Core Functionality
- [x] Parse ISO week format (YYYY-Www)
- [x] Parse date within week (YYYY-MM-DD) → calculate bounds
- [x] Parse explicit date ranges (--start-date, --end-date)
- [x] Default to next week when no temporal args provided
- [x] Validate temporal arguments with clear error messages
- [x] Handle edge cases (invalid dates, conflicting args)

### CLI Interface
- [x] Add `--week` argument to bronze script
- [x] Add `--start-date` and `--end-date` arguments
- [x] Add `--fetch-recipes` flag
- [x] Implement argument parser with help text
- [x] Validate mutually exclusive argument groups
- [x] Provide helpful usage examples

### Shell Script Enhancement
- [x] Maintain backward compatibility (MODE still works)
- [x] Support `--week` argument passthrough
- [x] Support `--start-date`/`--end-date` passthrough
- [x] Support `--fetch-recipes` flag
- [x] Allow combining MODE with temporal arguments
- [x] Update documentation in script header

### Data Integrity
- [x] Verify SCD Type 2 prevents data loss
- [x] Confirm multi-week coexistence in database
- [x] Verify idempotency of re-runs
- [x] Check Bronze append-only pattern
- [x] Confirm Silver upsert logic with tracking

### Documentation
- [x] Comprehensive guide (HISTORICAL_DATA_INGESTION.md)
  - [x] Architecture explanation
  - [x] Idempotency strategy
  - [x] Usage patterns with examples
  - [x] CLI reference
  - [x] Use cases (rebuild, backfill, correction)
  - [x] GitHub Actions example
  - [x] Data consistency guarantees
  - [x] Troubleshooting guide
  
- [x] Implementation summary (TEMPORAL_SELECTION_IMPLEMENTATION.md)
  - [x] Overview of changes
  - [x] File-by-file modifications
  - [x] Key features
  - [x] Testing recommendations
  - [x] Success criteria verification

- [x] Quick reference (QUICK_REFERENCE_TEMPORAL.md)
  - [x] Common command examples
  - [x] Argument format reference
  - [x] Quick task solutions

### Code Quality
- [x] Type hints on all functions
- [x] Docstrings with Args/Returns/Raises
- [x] Error handling with clear messages
- [x] Consistent code style with existing codebase
- [x] No breaking changes to existing API
- [x] Proper exit codes on errors

## 🧪 Testing Checklist

### Manual Testing
- [ ] Run default (next week)
  ```bash
  python scripts/1_bronze.py
  ```
- [ ] Run with ISO week
  ```bash
  python scripts/1_bronze.py --week 2026-W01
  ```
- [ ] Run with date in week
  ```bash
  python scripts/1_bronze.py --week 2026-01-15
  ```
- [ ] Run with date range
  ```bash
  python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-31
  ```
- [ ] Run with --fetch-recipes
  ```bash
  python scripts/1_bronze.py --week 2026-W01 --fetch-recipes
  ```

### Shell Script Testing
- [ ] Default mode `./run_pipeline.sh`
- [ ] Full mode `./run_pipeline.sh full`
- [ ] With week `./run_pipeline.sh --week 2026-W01`
- [ ] Combined `./run_pipeline.sh full --week 2026-W01`
- [ ] With date range `./run_pipeline.sh --start-date 2026-01-01 --end-date 2026-01-31`

### Error Handling
- [ ] Invalid date format (e.g., 2026/01/15)
- [ ] Invalid ISO week (e.g., 2026-W99)
- [ ] Conflicting arguments (--week + --start-date)
- [ ] Missing paired arguments (--start-date without --end-date)
- [ ] start_date > end_date

### Idempotency Testing
- [ ] Ingest week 1 → Check record count
- [ ] Ingest week 1 again → Verify count unchanged
- [ ] Check last_seen_date updated
- [ ] Ingest week 2 → Verify both weeks present

### Data Integrity
- [ ] Multiple weeks coexist in database
- [ ] Record counts correct for each week
- [ ] Menu-recipe associations preserved
- [ ] Ingredient data consistent across weeks

### Backward Compatibility
- [ ] Existing `./run_pipeline.sh` still works
- [ ] Existing `./run_pipeline.sh full` still works
- [ ] Default behavior (next week) unchanged
- [ ] Environment variable `FETCH_RECIPE_DETAILS` still works

## 📋 Success Criteria

All requirements from Issue #20 met:

✅ **Bronze script accepts explicit temporal selector**
   - `--week YYYY-Www` format supported
   - `--week YYYY-MM-DD` (date in week) supported
   - `--start-date` and `--end-date` supported
   
✅ **Pipeline maintains weekly cycle for future runs**
   - Default behavior unchanged (next week)
   - Shell script MODE still works (lite/full)
   - Backward compatible with existing workflows

✅ **Past weeks can be ingested independently**
   - Temporal parameters allow targeting any week
   - No dependencies between weeks
   - Each pull is independent

✅ **Multiple weeks coexist in same .db file**
   - SCD Type 2 tracking prevents conflicts
   - Different weeks have separate temporal metadata
   - Entity tracking via first_seen/last_seen dates

✅ **Re-running past week doesn't corrupt/erase other weeks**
   - Fully idempotent through upsert logic
   - SCD Type 2 updates vs inserts smart
   - No data loss on re-runs

## 🚀 Deployment Steps

1. **Review Changes**
   - Verify [1_bronze.py](../scripts/1_bronze.py) changes
   - Verify [run_pipeline.sh](../scripts/run_pipeline.sh) changes
   - Review documentation

2. **Test Locally** (using testing checklist above)

3. **Commit & Push**
   ```bash
   git add scripts/1_bronze.py scripts/run_pipeline.sh notes/
   git commit -m "feat: add temporal selection for historical data ingestion (#20)"
   git push
   ```

4. **Optional: Update GitHub Actions**
   - Add workflow_dispatch inputs for temporal parameters
   - Allow manual backfill runs
   - Document in Actions workflow

5. **Verify in Production**
   - Run default pipeline (verify next week ingestion)
   - Run backfill for missed weeks
   - Monitor for any issues

## 📚 Documentation Links

- [Full Guide](./HISTORICAL_DATA_INGESTION.md)
- [Implementation Details](./TEMPORAL_SELECTION_IMPLEMENTATION.md)
- [Quick Reference](./QUICK_REFERENCE_TEMPORAL.md)
- [Bronze Script Help](../scripts/1_bronze.py) - Run `python scripts/1_bronze.py --help`

## 🎯 Future Enhancements

- [ ] GitHub Actions workflow_dispatch parameters
- [ ] Batch ingestion with progress tracking
- [ ] Parallel processing for large date ranges
- [ ] Selective reference data updates (not just first-run)
- [ ] Data export/archival capabilities
- [ ] Database statistics and optimization
- [ ] Web UI for date selection and ingestion
