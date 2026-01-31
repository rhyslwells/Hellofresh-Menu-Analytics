# Local Development — Simple Pipeline

This file explains the minimal commands to run the pipeline locally: one-time setup and the weekly run.

Database used: `hfresh/hfresh.db`

Prerequisites
- Python 3.10+, pip, Git
- Set `HELLOFRESH_API_KEY` in your environment

One-time setup
1. Create & activate virtualenv

```bash
python -m venv venv
venv\\Scripts\\activate     # Windows
# or: source venv/bin/activate   # Linux/macOS
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Initialize SQLite schema (run once)

```bash
python scripts/init_sqlite.py
```

Expected result: `hfresh/hfresh.db` with bronze/silver/gold tables created.

Weekly run (order matters)
1. Bronze — fetch raw API responses

```bash
python scripts/1_bronze.py
```

2. Silver — normalize & SCD upserts

```bash
python scripts/2_silver.py
```

3. Gold — compute analytics

```bash
python scripts/3_gold_analytics.py
```

4. Reports — generate charts & markdown

```bash
python scripts/4_weekly_report.py
```

Quick full-run (use for automation/testing)

```bash
python scripts/init_sqlite.py && \\
python scripts/1_bronze.py && \\
python scripts/2_silver.py && \\
python scripts/3_gold_analytics.py && \\
python scripts/4_weekly_report.py
```

What to expect
- Database file: `hfresh/hfresh.db`
- Bronze: `api_responses` contains raw payloads
- Silver: `recipes`, `ingredients`, `menus`, bridge tables (e.g., `recipe_ingredients`)
- Gold: analytical tables in DB and charts in `hfresh/output/charts/`
- Reports: markdown files in `hfresh/output/reports/`

Quick verification

```bash
# Count recipes
sqlite3 hfresh/hfresh.db "SELECT COUNT(*) FROM recipes;"

# List gold metrics
sqlite3 hfresh/hfresh.db "SELECT * FROM weekly_menu_metrics LIMIT 5;"
```

Notes & troubleshooting
- If some bridge tables are empty, ensure `scripts/1_bronze.py` fetched recipe detail payloads (these are stored under endpoint `recipes`).
- If gold scripts fail with column errors, run `python scripts/init_sqlite.py` to ensure schema is up-to-date.
- `hfresh/silver_data.db` is redundant; the canonical DB path is `hfresh/hfresh.db`.

If you want, I can add a simple `Makefile` or `run_pipeline.sh` to automate these commands.
# Local Development — Simple Pipeline

This file explains the minimal commands to run the pipeline locally: one-time setup and the weekly run.

Database used: `hfresh/hfresh.db`

Prerequisites
- Python 3.10+, pip, Git
- Set `HELLOFRESH_API_KEY` in your environment

One-time setup
1. Create & activate virtualenv

```bash
python -m venv venv
venv\\Scripts\\activate     # Windows
# or: source venv/bin/activate   # Linux/macOS
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Initialize SQLite schema (run once)

```bash
python scripts/init_sqlite.py
```

Expected result: `hfresh/hfresh.db` with bronze/silver/gold tables created.

Weekly run (order matters)
1. Bronze — fetch raw API responses

```bash
python scripts/1_bronze.py
```

2. Silver — normalize & SCD upserts

```bash
python scripts/2_silver.py
```

3. Gold — compute analytics

```bash
python scripts/3_gold_analytics.py
```

4. Reports — generate charts & markdown

```bash
python scripts/4_weekly_report.py
```

Quick full-run (use for automation/testing)

```bash
python scripts/init_sqlite.py && \\
python scripts/1_bronze.py && \\
python scripts/2_silver.py && \\
python scripts/3_gold_analytics.py && \\
python scripts/4_weekly_report.py
```

What to expect
- Database file: `hfresh/hfresh.db`
- Bronze: `api_responses` contains raw payloads
- Silver: `recipes`, `ingredients`, `menus`, bridge tables (e.g., `recipe_ingredients`)
- Gold: analytical tables in DB and charts in `hfresh/output/charts/`
- Reports: markdown files in `hfresh/output/reports/`

Quick verification

```bash
# Count recipes
sqlite3 hfresh/hfresh.db "SELECT COUNT(*) FROM recipes;"

# List gold metrics
sqlite3 hfresh/hfresh.db "SELECT * FROM weekly_menu_metrics LIMIT 5;"
```

Notes & troubleshooting
- If some bridge tables are empty, ensure `scripts/1_bronze.py` fetched recipe detail payloads (these are stored under endpoint `recipes`).
- If gold scripts fail with column errors, run `python scripts/init_sqlite.py` to ensure schema is up-to-date.
- `hfresh/silver_data.db` is redundant; the canonical DB path is `hfresh/hfresh.db`.

If you want, I can add a simple `Makefile` or `run_pipeline.sh` to automate these commands.
