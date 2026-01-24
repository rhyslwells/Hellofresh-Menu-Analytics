# Next Steps - Running the Pipeline

## 1. Prerequisites

### Databricks Environment
- [ ] Active Databricks workspace
- [ ] Cluster running Spark 3.1+ with 4GB+ memory
- [ ] User has Catalog/Schema creation permissions
- [ ] Git CLI installed on cluster (optional, for report commits)

### Required Libraries
Install on your cluster:
```bash
# Python packages
%pip install requests matplotlib seaborn pandas
```

## 2. Configuration

### Update Credentials
Edit files or set environment variables:
```python
# HelloFresh API credentials
HELLOFRESH_API_KEY = "your_api_key_here"
HELLOFRESH_API_URL = "https://api.hellofresh.com/..."
```

### Catalog/Schema Names (Optional)
In each script, customize if needed:
```python
CATALOG = "hfresh_catalog"  # Change to your catalog name
SILVER_SCHEMA = "hfresh_silver"
GOLD_SCHEMA = "hfresh_gold"
```

## 3. Execution Steps

### Step 1: Initialize Environment (One-Time)
```python
# In Databricks notebook, run:
%run ./0_setup

# Expected output:
# ✓ Gold schema created
# ✓ Setup complete
```

### Step 2: Ingest Bronze Layer (Weekly)
```python
%run ./1_bronze

# Expected output:
# ✓ Downloaded X records from API
# ✓ Written to hfresh_catalog.hfresh_bronze.daily_snapshots
```

### Step 3: Transform to Silver (Weekly)
```python
%run ./2_silver

# Expected output:
# ✓ Processed X menus
# ✓ Processed X recipes
# ✓ SCD tracking active
```

### Step 4: Compute Gold Analytics (Weekly)
```python
%run ./3_gold_analytics

# Expected output:
# ✓ Weekly menu metrics computed
# ✓ Recipe survival metrics computed
# ... (5 tables total)
```

### Step 5: Generate Reports (Weekly)
```python
%run ./6_weekly_report

# Expected output:
# ✓ Generated 4 charts
# ✓ Report saved to /Workspace/hfresh/output/reports/
# ✓ Report committed to Git (if enabled)
```

## 4. Schedule as Jobs

### Create Databricks Job (Weekly Execution)
```
Job Config:
├─ Cluster: Your cluster
├─ Notebooks (in sequence):
│  ├─ 1_bronze.py (60 min timeout)
│  ├─ 2_silver.py (60 min timeout)
│  ├─ 3_gold_analytics.py (30 min timeout)
│  └─ 6_weekly_report.py (30 min timeout)
└─ Schedule: Weekly (Friday 1 AM UTC)
```

## 5. Verify Success

After first run, check:
```python
# In a new cell, verify data:
SELECT COUNT(*) FROM hfresh_catalog.hfresh_bronze.daily_snapshots
SELECT COUNT(*) FROM hfresh_catalog.hfresh_silver.recipes
SELECT COUNT(*) FROM hfresh_catalog.hfresh_gold.weekly_menu_metrics
```

Expected: All should have > 0 records

## 6. Monitoring

### Check Job Runs
- Databricks UI: Workflows → Your Job → Run History
- Look for green checkmarks (success) or red X (failures)

### View Logs
- Click run → "View logs" tab
- Search for `⚠️` warnings or `Error` messages

### Common Issues
| Issue | Solution |
|-------|----------|
| API key expired | Update credentials in 1_bronze.py |
| Out of memory | Increase cluster memory (8GB recommended) |
| Catalog not found | Run 0_setup.py first |
| Charts not displaying | Install matplotlib: `%pip install matplotlib` |
| Git push fails | Ensure Git token has push permissions |

## 7. Output Locations

```
/Workspace/hfresh/
├─ output/
│  ├─ charts/
│  │  ├─ menu_overlap_trends.png
│  │  ├─ recipe_survival_distribution.png
│  │  ├─ ingredient_trends.png
│  │  └─ allergen_density_heatmap.png
│  └─ reports/
│     └─ weekly_report_2026-01-24.md
└─ bronze_data/
   └─ (raw API snapshots - auto-cleaned after 7 days)
```

## 8. Troubleshooting

### Script fails on first run
```python
# Check Bronze table exists:
SHOW TABLES IN hfresh_catalog.hfresh_bronze

# If missing, re-run setup:
%run ./0_setup
```

### SCD logic not tracking changes
```python
# Verify columns in Silver recipes table:
DESCRIBE hfresh_catalog.hfresh_silver.recipes

# Should have: first_seen_date, last_seen_date, is_active
```

### Gold metrics are empty
```python
# Check Silver has data:
SELECT COUNT(*) FROM hfresh_catalog.hfresh_silver.recipes
# Should be > 0 before running Gold
```

## 9. Optional: Local Development

To test locally before Databricks:
```bash
# Install PySpark
pip install pyspark

# Run in local mode (won't connect to actual APIs):
python 0_setup.py
python 1_bronze.py  # Will fail on API call - expected
```

## Quick Start (TL;DR)

1. **Cluster setup**: 4GB+, Spark 3.1+, `pip install requests matplotlib seaborn`
2. **One-time**: `%run ./0_setup`
3. **Weekly**: `%run ./1_bronze` → `%run ./2_silver` → `%run ./3_gold_analytics` → `%run ./6_weekly_report`
4. **Monitor**: Check Databricks job runs for errors
5. **Verify**: Query tables to confirm data flow

---

**Questions?** Refer to:
- [FIXES_APPLIED.md](FIXES_APPLIED.md) - What was fixed and why
- [blueprint.md](docs/blueprint.md) - Architecture and data model
