# HelloFresh Data Platform Documentation

This folder contains comprehensive documentation for the HelloFresh pipeline.

## Quick Navigation

### 📋 Core Documents (Start Here)

- **[blueprint.md](blueprint.md)** - Complete architecture overview and implementation details
  - 3-layer medallion architecture (bronze/silver/gold)
  - 14 SQLite table schema with SCD Type 2
  - GitHub Actions workflow setup
  - Design decisions and success criteria

- **[notes.md](notes.md)** - Task checklist and implementation notes
  - Active tasks and completed items
  - SCD Type 2 pattern explanation
  - Known gotchas and limitations
  - Useful SQL validation queries
  - Performance tips and monitoring

### 📖 Getting Started

**For local development:** See [LOCAL_DEV.md](../LOCAL_DEV.md)
- Python environment setup
- Running the 5-step pipeline locally
- Database exploration
- Troubleshooting

**For GitHub Actions:** See [GITHUB_SETUP.md](../GITHUB_SETUP.md)
- Repository secret configuration
- Workflow schedule customization
- Artifact management
- CI/CD troubleshooting

### 📚 Reference Docs

- **[SCD-Type-2.md](SCD-Type-2.md)** - Detailed explanation of Slowly Changing Dimensions Type 2
  - What is SCD Type 2 and why it matters
  - Implementation patterns for HelloFresh data
  - Code examples

- **[api.md](api.md)** - HelloFresh API integration details
  - API endpoint information
  - Authentication with secrets management
  - Rate limiting strategy
  - PowerShell setup script for secrets

---

## Project Summary

**Goal:** Build a production-ready weekly data pipeline for HelloFresh API data

**Technology Stack:**
- SQLite3 (database)
- pandas (data processing)
- matplotlib/seaborn (visualization)
- GitHub Actions (orchestration)

**Execution Model:**
- Automated: Runs every Friday 02:00 UTC
- Manual: Trigger via GitHub Actions UI
- Local: Run all scripts on your machine

**Output:**
- SQLite database with 14 tables (bronze/silver/gold)
- Weekly markdown reports
- 4 PNG charts (menu, recipe, ingredient, allergen analysis)
- All committed to Git repository

---

## Key Features

### Medallion Architecture
```
API Data → Bronze (raw JSON)
         → Silver (normalized with SCD Type 2)
         → Gold (5 analytics tables)
         → Reports (markdown + charts)
```

### SCD Type 2 Tracking
Every entity tracks its lifecycle:
- When first seen in API
- When last updated
- Current active status
- Complete historical lineage

### 5 Gold Analytics Tables
1. **weekly_menu_metrics** - Recipe composition per week
2. **recipe_survival_metrics** - When recipes appear/disappear
3. **ingredient_trends** - Popularity rankings
4. **menu_stability_metrics** - Week-over-week changes
5. **allergen_density** - Allergen coverage analysis

### Automated Reporting
- 4 PNG charts generated weekly
- Markdown report with embedded analysis
- Auto-committed to Git repository
- Viewable via GitHub

---

## Quick Start

### Option 1: Local Development
```bash
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate (Windows)
pip install -r requirements.txt

export HELLOFRESH_API_KEY="your_key"

python scripts/init_sqlite.py        # Create tables
python scripts/1_bronze.py           # Fetch API data
python scripts/2_silver.py           # Normalize
python scripts/3_gold_analytics.py   # Compute analytics
python scripts/6_weekly_report.py    # Generate report
```

### Option 2: GitHub Actions
1. Push repo to GitHub
2. Add GitHub Secret: `HELLOFRESH_API_KEY`
3. Workflow runs automatically Friday 02:00 UTC

---

## File Organization

```
Databricks-Explorer/
├── docs/                           # This folder
│   ├── blueprint.md               # Architecture
│   ├── notes.md                   # Tasks & notes
│   ├── SCD-Type-2.md             # SCD explanation
│   ├── api.md                     # API details
│   ├── README.md                  # This file
│   └── changes/                   # Historical change logs
│
├── scripts/                        # Python pipeline
│   ├── init_sqlite.py             # Create schema
│   ├── 1_bronze.py                # API ingestion
│   ├── 2_silver.py                # Normalization
│   ├── 3_gold_analytics.py        # Analytics
│   ├── 6_weekly_report.py         # Reporting
│   └── sqlite_utils.py            # Utilities
│
├── hfresh/                        # Data & output
│   ├── hfresh.db                  # SQLite database (14 tables)
│   └── output/
│       ├── charts/                # PNG files
│       └── reports/               # Markdown reports
│
├── .github/
│   └── workflows/
│       └── pipeline.yml           # GitHub Actions
│
├── requirements.txt               # Dependencies
├── .gitignore                     # Exclude *.db, output/charts
├── GITHUB_SETUP.md               # GitHub Actions guide
├── LOCAL_DEV.md                  # Local development guide
└── CONVERSION_COMPLETE.md        # Project status
```

---

## Schema Overview

### Bronze Layer (1 table)
- `api_responses` - Raw JSON from API (immutable, append-only)

### Silver Layer (11 tables)
**Entities** (with SCD Type 2):
- recipes, ingredients, allergens, tags, labels, menus

**Relationships** (many-to-many bridges):
- recipe_ingredients, recipe_allergens, recipe_tags, recipe_labels, menu_recipes

### Gold Layer (5 tables)
- weekly_menu_metrics
- recipe_survival_metrics
- ingredient_trends
- menu_stability_metrics
- allergen_density

---

## Common Tasks

### Query the Database
```bash
sqlite3 hfresh/hfresh.db
sqlite> SELECT COUNT(*) FROM recipes WHERE is_active = 1;
sqlite> SELECT DISTINCT week_start_date FROM weekly_menu_metrics ORDER BY week_start_date DESC;
```

### Backup Database
```bash
cp hfresh/hfresh.db hfresh/hfresh_backup_$(date +%Y%m%d).db
```

### Run Specific Script
```bash
python scripts/3_gold_analytics.py  # Just compute analytics
python scripts/6_weekly_report.py   # Just generate report
```

### Check GitHub Actions
- Go to: `https://github.com/YOUR_USERNAME/repo/actions`
- Click pipeline.yml to see run history
- Check logs for any errors

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| API key not found | Set `HELLOFRESH_API_KEY` env var or GitHub Secret |
| Database locked | Ensure only one script running at a time |
| No tables in database | Run `python scripts/init_sqlite.py` first |
| Charts not showing | Verify `matplotlib` installed: `pip install matplotlib` |
| Git operations fail | Ensure running from repo root with Git initialized |

See [LOCAL_DEV.md](../LOCAL_DEV.md) for detailed troubleshooting.

---

## Architecture Decisions

### SQLite vs Databricks
- ✅ **SQLite:** Free, portable, local development, GitHub Actions compatible
- ❌ **SQLite:** Limited to ~10GB, no multi-user concurrency
- ✅ **Databricks:** Petabyte scale, production-grade, ACID guarantees
- ❌ **Databricks:** Costs money, cloud-only, setup complexity

**Choice:** SQLite for learning & personal use; Databricks for production

### SCD Type 2 Implementation
- **UPDATE + INSERT pattern** - Standard SQL, no MERGE required
- **Records track lifetime** - first_seen_date, last_seen_date, is_active
- **Historical queries possible** - "What recipes were active on date X?"

### Python Fallback for Complex Operations
- **SQLite limitation:** No ARRAY_INTERSECT, CARDINALITY
- **Solution:** Fetch data, use Python set operations
- **Trade-off:** Single-threaded, works for weekly batches

---

## Performance Characteristics

| Step | Runtime | Limiting Factor |
|------|---------|-----------------|
| init_sqlite.py | <1 min | Disk write speed |
| 1_bronze.py | 5-10 min | API rate limit (60/min) |
| 2_silver.py | 5 min | Database transaction overhead |
| 3_gold_analytics.py | 3-5 min | Full table scans |
| 6_weekly_report.py | 3-5 min | Chart rendering |
| **Total** | **20-30 min** | Bronze ingestion |

**Optimization:** Subsequent weeks are faster (only new API data)

---

## Next Steps

1. **Verify setup locally** - Run all 5 scripts and check outputs
2. **Push to GitHub** - Create repo and configure secrets
3. **Monitor first run** - Watch Friday 02:00 UTC for automated execution
4. **Review reports** - Analyze trends and validate data quality
5. **Optimize schedule** - Adjust timing or retention as needed

---

## Support & Resources

### Documentation
- [Architecture Blueprint](blueprint.md)
- [SCD Type 2 Explanation](SCD-Type-2.md)
- [Local Development Guide](../LOCAL_DEV.md)
- [GitHub Actions Setup](../GITHUB_SETUP.md)

### External Resources
- [HelloFresh API Docs](https://console.hfresh.info/docs)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [GitHub Actions Docs](https://docs.github.com/en/actions)

---

**Last Updated:** 2026-01-25  
**Version:** 2.0 (SQLite + GitHub Actions)  
**Status:** ✅ Production Ready
