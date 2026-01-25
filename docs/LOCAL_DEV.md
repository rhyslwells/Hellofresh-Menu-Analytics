# Local Development Guide

This document covers running the HelloFresh pipeline locally for development and testing.

## Prerequisites

- Python 3.10 or higher
- pip or conda
- SQLite3 (usually included with Python)
- Git
- HelloFresh API key

## 1. Initial Setup

### Clone the Repository

```bash
git clone <your-repo-url>
cd Databricks-Explorer
```

### Create Virtual Environment

```bash
# Using venv
python -m venv venv

# Activate (choose one)
source venv/bin/activate          # Linux/macOS
venv\Scripts\activate             # Windows
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Configuration

### Set API Key

Before running the pipeline, set your HelloFresh API key:

```bash
# Linux/macOS (Bash)
export HELLOFRESH_API_KEY="your_api_key_here"

# macOS (Zsh)
export HELLOFRESH_API_KEY="your_api_key_here"

# Windows (PowerShell)
$env:HELLOFRESH_API_KEY = "your_api_key_here"

# Windows (Command Prompt)
set HELLOFRESH_API_KEY=your_api_key_here
```

### Verify Installation

```bash
python -c "import sqlite3, requests, pandas, matplotlib; print('All dependencies OK')"
```

## 3. Running the Pipeline

### Step 1: Initialize Database

Creates the SQLite schema (runs once or after deletion):

```bash
python scripts/init_sqlite.py
```

Output: `hfresh/hfresh.db` created with all tables.

### Step 2: Bronze Layer (API Ingestion)

Fetches raw data from HelloFresh API:

```bash
python scripts/1_bronze.py
```

This step:
- Requires: `HELLOFRESH_API_KEY` environment variable
- Fetches: Next week's menus with embedded recipes
- On first run: Also fetches reference data (ingredients, allergens, tags, labels)
- Duration: ~1-2 minutes depending on API response time

### Step 3: Silver Layer (Normalization)

Transforms raw data into normalized, SCD Type 2 tracked tables:

```bash
python scripts/2_silver.py
```

This step:
- Reads: Bronze layer (`api_responses` table)
- Writes: Silver tables (recipes, ingredients, menus, etc.)
- Handles: Upserts with SCD Type 2 logic (first_seen_date, last_seen_date, is_active)
- Duration: ~10-30 seconds

### Step 4: Gold Layer (Analytics)

Computes analytical metrics and insights:

```bash
python scripts/3_gold_analytics.py
```

This step:
- Reads: Silver layer tables
- Computes: 5 analytical tables
  - `weekly_menu_metrics`: Menu composition
  - `recipe_survival_metrics`: Recipe lifespan
  - `ingredient_trends`: Ingredient popularity
  - `menu_stability_metrics`: Week-over-week changes
  - `allergen_density`: Allergen coverage
- Duration: ~5-15 seconds

### Step 5: Report Generation

Creates markdown reports with embedded charts:

```bash
python scripts/6_weekly_report.py
```

This step:
- Reads: Gold layer tables
- Generates: Charts (PNG)
  - `menu_overlap_trends.png`
  - `recipe_survival_distribution.png`
  - `ingredient_trends.png`
  - `allergen_density_heatmap.png`
- Creates: Markdown report with insights
- Commits: Report to Git (if running in repo)
- Duration: ~10-20 seconds
- Output: `hfresh/output/reports/weekly_report_YYYY-MM-DD.md`

### Run All Steps at Once

```bash
python scripts/init_sqlite.py && \
python scripts/1_bronze.py && \
python scripts/2_silver.py && \
python scripts/3_gold_analytics.py && \
python scripts/6_weekly_report.py
```

## 4. Exploring Results

### View Database

Use any SQLite browser or command line:

```bash
sqlite3 hfresh/hfresh.db

# Common queries:
.tables                           # List all tables
SELECT COUNT(*) FROM recipes;     # Count recipes
SELECT * FROM weekly_menu_metrics LIMIT 5;  # View metrics
```

### View Generated Reports

```bash
# List available reports
ls hfresh/output/reports/

# Open in editor
code hfresh/output/reports/weekly_report_*.md
```

### View Charts

Charts are PNG files in `hfresh/output/charts/`. Open with any image viewer.

## 5. Development Workflow

### Make Changes to Scripts

Edit pipeline scripts in `scripts/`:

```
scripts/
├── 1_bronze.py          # API ingestion
├── 2_silver.py          # Data normalization
├── 3_gold_analytics.py  # Metric computation
├── 6_weekly_report.py   # Report generation
└── init_sqlite.py       # Database initialization
```

### Test Changes

After editing a script:

```bash
# Rebuild database to test from scratch
rm hfresh/hfresh.db
python scripts/init_sqlite.py

# Run your modified step
python scripts/YOUR_SCRIPT.py
```

### Debug Issues

Add logging to investigate:

```python
# In any script:
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.debug(f"Variable value: {variable}")
```

## 6. Database Management

### Backup Database

```bash
cp hfresh/hfresh.db hfresh/hfresh_backup.db
```

### Reset Database

```bash
rm hfresh/hfresh.db
python scripts/init_sqlite.py
```

### Export Data to CSV

```bash
sqlite3 hfresh/hfresh.db ".mode csv" ".output recipes.csv" "SELECT * FROM recipes;" ".quit"
```

## 7. Performance Tips

- **First run takes longer** due to API rate limiting and reference data fetch
- **Subsequent runs are faster** because reference data is cached
- **Use smaller test APIs** if available for faster iteration

## 8. Troubleshooting

### "API key not found"

```bash
# Verify environment variable is set
echo $HELLOFRESH_API_KEY  # Linux/macOS
echo %HELLOFRESH_API_KEY% # Windows CMD
```

### "Database is locked"

The database file is being used by another process:

```bash
# Kill any Python processes
pkill python  # Linux/macOS

# Or restart your terminal
```

### "Matplotlib not available"

Charts will be skipped, but pipeline continues. To enable charts:

```bash
pip install matplotlib seaborn
```

### Module not found errors

Ensure you're in the virtual environment:

```bash
which python  # Should show venv path
pip list      # Should show installed packages
```

## 9. Committing Changes

After running locally and verifying results:

```bash
# Stage changes
git add hfresh/output/reports/
git add scripts/  # If you modified code

# Commit
git commit -m "Add weekly report and pipeline updates"

# Push to GitHub
git push
```

## 10. Next Steps

- Review generated reports in `hfresh/output/reports/`
- Examine charts in `hfresh/output/charts/`
- Query the database for custom insights
- Modify transformation logic in `scripts/2_silver.py` if needed
- Add custom metrics to `scripts/3_gold_analytics.py`

## References

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [Python Pandas Guide](https://pandas.pydata.org/docs/)
- [Matplotlib Tutorial](https://matplotlib.org/stable/tutorials/index.html)
