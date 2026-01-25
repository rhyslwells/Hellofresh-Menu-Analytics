# Project Tasks & Notes

## Active Tasks

- [x] Convert 1_bronze.py from Databricks to SQLite
- [x] Convert 2_silver.py with SCD Type 2 logic
- [x] Convert 3_gold_analytics.py to SQLite SQL
- [x] Convert 6_weekly_report.py with GitHub Actions
- [x] Create GitHub Actions workflow (pipeline.yml)
- [x] Create setup documentation (GITHUB_SETUP.md, LOCAL_DEV.md)
- [ ] Test end-to-end pipeline locally
- [ ] Test GitHub Actions workflow execution
- [ ] Monitor first automated run (Friday 02:00 UTC)
- [ ] Adjust schedule/retention as needed

## Key Implementation Notes

### SCD Type 2 Pattern
Records track their lifetime in the dataset:
- `first_seen_date` - Week when first encountered
- `last_seen_date` - Most recent update (every week)
- `is_active` - TRUE if in current API data

**Logic:** UPDATE existing record's dates, INSERT if new, mark missing as inactive

### Database Structure
- **Single file:** `hfresh/hfresh.db` (14 tables)
- **Bronze (1):** api_responses (raw API payloads)
- **Silver (11):** Normalized entities + bridge relationships
- **Gold (5):** Analytics tables

### Charts Generated Weekly
1. `menu_overlap.png` - Week-over-week recipe stability
2. `recipe_survival.png` - Recipe lifespan distribution
3. `ingredient_trends.png` - Popularity rankings over time
4. `allergen_heatmap.png` - Allergen coverage by week

### API Integration
- Endpoint: HelloFresh API (requires API key)
- Rate limit: 60 requests/minute
- Pagination: Automatic with recursive retry logic
- Auth: GitHub Secret `HELLOFRESH_API_KEY`

## Known Gotchas

### SQLite Limitations
- No native ARRAY operations (use Python set operations instead)
- No MERGE statement (use UPDATE + INSERT pattern)
- JULIANDAY() for date calculations (not DATE_ADD like Spark)

### GitHub Actions
- Workflow runs Friday 02:00 UTC
- Needs repository secret configured (case-sensitive: `HELLOFRESH_API_KEY`)
- Database file persists between runs (append-only design)
- Reports auto-commit to main branch

### Local Development
- Must set environment variable: `export HELLOFRESH_API_KEY="..."`
- Create venv: `python -m venv venv && source venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- Run scripts in order: init → 1_bronze → 2_silver → 3_gold → 6_report

## Schema Validation Queries

```sql
-- Verify all tables exist
SELECT COUNT(*) FROM recipes;
SELECT COUNT(*) FROM ingredients;
SELECT COUNT(*) FROM menus;
SELECT COUNT(*) FROM weekly_menu_metrics;

-- Check SCD tracking is active
SELECT COUNT(DISTINCT recipe_id) FROM recipes WHERE is_active = 1;
SELECT DATE(MAX(last_seen_date)) FROM recipes;

-- Verify relationships
SELECT COUNT(*) FROM recipe_ingredients;
SELECT COUNT(*) FROM menu_recipes;

-- Check gold analytics
SELECT COUNT(*) FROM weekly_menu_metrics;
SELECT COUNT(DISTINCT week_start_date) FROM ingredient_trends;
```

## Performance Tips

1. **First run is slow** - API rate limiting takes ~5-10 min for bronze
2. **Subsequent runs are faster** - Only append new rows to bronze
3. **Gold layer recomputes fully** - Uses DELETE then INSERT pattern (idempotent)
4. **Charts take ~1 min** - matplotlib rendering on full history

## Backup Strategy

```bash
# Backup database
cp hfresh/hfresh.db hfresh/hfresh_backup_$(date +%Y%m%d).db

# Verify backup
sqlite3 hfresh/hfresh_backup_*.db "SELECT COUNT(*) FROM recipes;"
```

## Monitoring & Alerts

Check weekly:
- GitHub Actions successful runs (green checkmarks)
- New report + charts in `hfresh/output/`
- Report committed to main branch
- No data quality warnings in logs

Red flags:
- API key expired → Update GitHub Secret
- Database locked → Ensure no concurrent runs
- Out of memory → Increase runner specs or reduce retention
- Charts not generating → Check matplotlib installation

## Useful Commands

```bash
# Explore database
sqlite3 hfresh/hfresh.db ".schema"
sqlite3 hfresh/hfresh.db ".tables"

# Query results
sqlite3 hfresh/hfresh.db "SELECT * FROM weekly_menu_metrics LIMIT 5;"

# Check file size
ls -lh hfresh/hfresh.db

# Git operations
git log --oneline hfresh/output/reports/
git diff <commit1> <commit2> -- hfresh/output/reports/

# Run specific script
python scripts/1_bronze.py
python scripts/6_weekly_report.py
```

## Future Enhancements

- [ ] Add Slack notifications on workflow completion
- [ ] Add data quality checks (duplicate detection, nullability)
- [ ] Create GitHub Pages dashboard for reports
- [ ] Add email alerts for anomalies
- [ ] Set up dbt for complex silver/gold transformations
- [ ] Add Apache Airflow orchestration (if scaling beyond GitHub Actions)
- [ ] Export gold tables to cloud data warehouse
