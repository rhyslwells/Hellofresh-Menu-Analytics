# GitHub Actions Setup for HelloFresh Pipeline

This document covers setup for running the HelloFresh data pipeline using GitHub Actions with SQLite.

## Prerequisites

- GitHub repository
- GitHub Actions enabled (default for public repos)
- Python 3.10+
- SQLite3

## 1. Configure GitHub Secrets

The pipeline requires the HelloFresh API key to be stored as a GitHub Secret.

### Steps:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Create a secret named `HELLOFRESH_API_KEY`
5. Paste your HelloFresh API key as the value
6. Click **Add secret**

> **Note:** To get your API key, log in to HelloFresh and navigate to your account settings to generate an API token.

## 2. GitHub Actions Workflow

The pipeline runs automatically on a schedule defined in `.github/workflows/pipeline.yml`.

### Schedule:

- **Time:** Friday at 02:00 UTC
- **Manual Trigger:** Available via `workflow_dispatch`

### Workflow Steps:

1. **Checkout** - Clones the repository
2. **Setup Python** - Installs Python 3.10
3. **Install Dependencies** - Installs required packages from `requirements.txt`
4. **Initialize Database** - Creates SQLite schema via `init_sqlite.py`
5. **Bronze Ingestion** - Fetches API data via `1_bronze.py`
6. **Silver Transformation** - Normalizes data via `2_silver.py`
7. **Gold Analytics** - Computes metrics via `3_gold_analytics.py`
8. **Weekly Report** - Generates report via `4_weekly_report.py`
9. **Upload Artifacts** - Stores charts and reports (30-day retention)
10. **Commit Reports** - Automatically commits generated reports to the repository

## 3. Local Development

To test the pipeline locally:

### First Run:

```bash
# Initialize database
python scripts/init_sqlite.py

# Run pipeline steps in order
python scripts/1_bronze.py      # Requires HELLOFRESH_API_KEY env var
python scripts/2_silver.py
python scripts/3_gold_analytics.py
python scripts/4_weekly_report.py
```

### Set API Key:

```bash
# Linux/macOS
export HELLOFRESH_API_KEY="your_api_key"

# Windows PowerShell
$env:HELLOFRESH_API_KEY="your_api_key"

# Windows CMD
set HELLOFRESH_API_KEY=your_api_key
```

## 4. Database Structure

The pipeline creates a single SQLite database at `hfresh/hfresh.db` with:

- **Bronze Layer:** Raw API responses (`api_responses`)
- **Silver Layer:** Normalized entities with SCD Type 2 tracking
  - Dimensions: `recipes`, `ingredients`, `allergens`, `tags`, `labels`, `menus`
  - Bridges: `recipe_ingredients`, `recipe_allergens`, `recipe_tags`, `recipe_labels`, `menu_recipes`
- **Gold Layer:** Analytical tables
  - `weekly_menu_metrics`
  - `recipe_survival_metrics`
  - `ingredient_trends`
  - `menu_stability_metrics`
  - `allergen_density`

## 5. Output Files

Reports and charts are generated in:

```
hfresh/
├── output/
│   ├── charts/
│   │   ├── menu_overlap_trends.png
│   │   ├── recipe_survival_distribution.png
│   │   ├── ingredient_trends.png
│   │   └── allergen_density_heatmap.png
│   └── reports/
│       └── weekly_report_YYYY-MM-DD.md
└── hfresh.db
```

## 6. Monitoring & Troubleshooting

### View Workflow Status:

- Go to **Actions** tab in your GitHub repository
- Click the latest workflow run
- View logs for each step

### Common Issues:

| Issue | Solution |
|-------|----------|
| API key not found | Verify `HELLOFRESH_API_KEY` secret is configured |
| Database locked | Ensure previous run completed; check `hfresh.db` is not corrupted |
| Matplotlib error | Charts will be skipped gracefully; only affects output quality |
| Git push failed | Ensure token has write permissions to the repository |

## 7. Customization

### Change Schedule:

Edit `.github/workflows/pipeline.yml`, line with `cron`:

```yaml
cron: '0 2 * * 5'  # Current: Friday 02:00 UTC
# Examples:
# '0 9 * * MON'    - Monday 09:00 UTC
# '0 */6 * * *'    - Every 6 hours
```

### Adjust Artifact Retention:

In `.github/workflows/pipeline.yml`:

```yaml
retention-days: 30  # Change to desired days
```

## 8. Accessing Reports

Reports are committed to the repository and visible in:

1. **Git History:** `git log --oneline` shows report commits
2. **File Browser:** Navigate to `hfresh/output/reports/` in GitHub
3. **Artifacts:** Download from workflow run's artifact section (for 30 days)

## References

- [HelloFresh API Documentation](https://www.hellofresh.com/plans)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
