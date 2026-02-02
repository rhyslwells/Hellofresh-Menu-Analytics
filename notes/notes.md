# TODOs

test

### Notes

Done:
- [x] Its not 4_weekly_report.py anymore, rename to 4_weekly_report.py and adjust repo files accordingly.
- [x] Rename the repository to something more descriptive than Databricks-Explorer.
- [x] Convert `1_bronze.py` from Databricks to SQLite
- [x] Convert `2_silver.py` with SCD Type 2 logic
- [x] Convert `3_gold_analytics.py` to SQLite SQL
- [x] Convert `4_weekly_report.py` with GitHub Actions
- [x] Add CI workflow (`.github/workflows/pipeline.yml`)
- [x] Add `GITHUB_SETUP.md` and `LOCAL_DEV.md`
- [x] Can we write some sql scripts to explore the hfresh.db file saving these to scripts\sql_queries\ for future reference?

This file contains the practical developer notes, the TODO checklist, and
the exact commands for building and running the repository locally. High-
level project information and links live in the repository `README.md`.

- You can get a better host name using namecheep (min £5 per year) instead of using the github pages URL.

SCD Type 2 pattern (implementation notes)
- Each entity row tracks lifecycle: `first_seen_date`, `last_seen_date`, `is_active`.
- Pattern: on each run, compare current API entities →
	- UPDATE existing rows' `last_seen_date` and `is_active` as needed
	- INSERT rows that are new with `first_seen_date = last_seen_date = run_week`
	- Mark rows not present in current payload as `is_active = FALSE` (and set `last_seen_date`)

Database layout (developer view)
- Single DB: `hfresh/hfresh.db` (14 tables total)
- Bronze: `api_responses` (raw JSON, append-only)
- Silver: normalized entity tables (recipes, ingredients, allergens, tags, labels, menus) and bridge tables (recipe_ingredients, recipe_allergens, recipe_tags, recipe_labels, menu_recipes)
- Gold: analytics tables (weekly_menu_metrics, recipe_survival_metrics, ingredient_trends, menu_stability_metrics, allergen_density)

Charts generated weekly
- `menu_overlap.png`, `recipe_survival.png`, `ingredient_trends.png`, `allergen_heatmap.png`

API integration
- Endpoint: HelloFresh API (API key required)
- Rate limit: 60 requests/minute (throttle calls accordingly)
- Auth: runner/local env var or GitHub Secret `HELLOFRESH_API_KEY`

Known gotchas
- SQLite has no `MERGE` and limited array/JSON functions — implement joins and set logic in Python when helpful.
- Use `julianday()` for date math when necessary.
- Database file can be locked if concurrent scripts run — avoid parallel runs against the same DB.

Local development (exact commands)
Windows (PowerShell/cmd):
```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
set HELLOFRESH_API_KEY=your_key_here
python scripts/init_sqlite.py
python scripts/1_bronze.py
python scripts/2_silver.py
python scripts/3_gold_analytics.py
python scripts/4_weekly_report.py
```

Backup procedure (git bash)
```bash
cp hfresh/hfresh.db hfresh/backups/hfresh_backup_$(date +%Y%m%d).db
sqlite3 hfresh/hfresh_backup_*.db "SELECT COUNT(*) FROM recipes;"
```

Performance notes
- First full bronze ingestion will be slow (API rate limits).
- Subsequent runs append new rows to bronze and update silver; gold recomputes are idempotent.

Monitoring checklist
- Confirm GitHub Actions run succeeded
- Check `hfresh/output/<week>/report.md` and charts
- Verify `weekly_menu_metrics` and `recipe_survival_metrics` contain expected rows