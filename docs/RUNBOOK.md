# Runbook — How to run the HelloFresh pipeline (Databricks)

This runbook lists the minimal steps to run the pipeline end-to-end on Databricks.
Keep secrets out of the repo — use Databricks Secrets (see `scripts/SETUP_SECRETS.md`).

Prerequisites
- Databricks workspace and user with permissions to create clusters and secret scopes
- Databricks CLI or UI access
- Git repo checked out in workspace under a path you control (optional)

Quick sequence
1. Provision cluster
2. Create secret scope + store API key
3. Install libs on cluster
4. Run `scripts/0_setup.py`
5. Run `scripts/1_bronze.py`
6. Run `scripts/2_silver.py`
7. Run `scripts/3_gold_analytics.py`
8. Run `scripts/6_weekly_report.py`
9. Verify results and schedule as job

Commands & details

1) Provision Databricks cluster
- Spark 3.1+ recommended
- Attach a driver with Python and ~8GB RAM (adjust for data size)
- Install Python libraries (or use init script):

```bash
# In a notebook cell on the cluster or cluster init script
%pip install requests matplotlib seaborn pandas
```

2) Create secret scope and store API key
- Recommended: use scope named `hfresh_secrets` and key `api_key`

```bash
# Using Databricks CLI
databricks secrets create-scope --scope hfresh_secrets
databricks secrets put --scope hfresh_secrets --key api_key
# Or use dbutils in a notebook:
# dbutils.secrets.createScope("hfresh_secrets")
# dbutils.secrets.put(scope="hfresh_secrets", key="api_key", value="<PASTE_KEY>")
```

3) Install required packages on the cluster
- See step 1
- Verify in a notebook:
```python
import requests
import matplotlib
import seaborn
```

4) One-time setup: create catalogs/schemas
```python
# In a Databricks notebook attached to the cluster
%run ./scripts/0_setup
```
Expected: console shows created catalogs/schemas and success messages.

5) Ingest Bronze (weekly snapshot)
```python
%run ./scripts/1_bronze
```
Notes:
- `1_bronze.py` reads API key from Databricks Secrets (`hfresh_secrets:api_key`) and falls back to `HELLOFRESH_API_KEY` env var for local dev
- Check Delta table: `SELECT COUNT(*) FROM hfresh_catalog.hfresh_bronze.api_responses`

6) Transform Bronze → Silver
```python
%run ./scripts/2_silver
```
Notes:
- SCD Type 2 upserts are used (MERGE)
- Verify `hfresh_catalog.hfresh_silver.recipes` has `first_seen_date`, `last_seen_date`, `is_active`

7) Compute Gold analytics
```python
%run ./scripts/3_gold_analytics
```
Notes:
- Each analytical table is cleared and re-populated (idempotent)
- Verify counts using SELECT on gold tables

8) Generate weekly report (charts + markdown)
```python
%run ./scripts/6_weekly_report
```
Notes:
- In Databricks, reports are written to `/Workspace/hfresh/output/reports/`
- Charts to `/Workspace/hfresh/output/charts/`
- Git commit is disabled in Databricks by default; for local runs, `6_weekly_report.py` will attempt to commit if `GIT_REPO_PATH` is configured

9) Verify & schedule
- Quick checks:
```sql
SELECT COUNT(*) FROM hfresh_catalog.hfresh_bronze.api_responses;
SELECT COUNT(*) FROM hfresh_catalog.hfresh_silver.recipes;
SELECT COUNT(*) FROM hfresh_catalog.hfresh_gold.weekly_menu_metrics;
```
- Create a Databricks Job (workflow) chaining the notebooks/scripts above in order. Schedule weekly.

Troubleshooting & notes
- If API calls fail, confirm secret exists:
```python
# In Databricks cell
dbutils.secrets.get(scope="hfresh_secrets", key="api_key")
```
- If MERGE fails, check schema mismatch or column names in silver tables
- Charts require `matplotlib`; if missing, the report will still generate markdown without images

Local development
- You can run small parts locally, but the pipeline expects Delta tables in Databricks.
- For local testing, set `HELLOFRESH_API_KEY` env var and run parts of `1_bronze.py` that use requests

Files of interest
- `scripts/0_setup.py` — creates catalog/schemas
- `scripts/1_bronze.py` — ingestion (uses secrets)
- `scripts/2_silver.py` — normalization (SCD Type 2)
- `scripts/3_gold_analytics.py` — analytics SQL
- `scripts/6_weekly_report.py` — charts & markdown
- `scripts/queries.sql` — ad-hoc query library
- `scripts/SETUP_SECRETS.md` — secret setup guide

Notebooks vs Python scripts
---------------------------
Do I need to convert the `.py` scripts to `.ipynb` notebooks?

- Short answer: No — not strictly required. Databricks can run Python scripts or notebooks.
- Recommended: convert to notebooks if you plan to use `notebook_task` in Jobs or prefer interactive runs. Keep them as Python files if you prefer CI-driven, script-based execution (use `spark_python_task` or package as wheel).

How to choose:
- Use notebooks when you want: interactive development, `%run` cells, and easy exploration in the workspace.
- Use Python files when you want: git-friendly script workflows, CI/CD, or to run as `spark_python_task` or packaged wheel.

Quick conversion options:
- Import Python files into the workspace as notebooks (Databricks CLI):

```bash
databricks workspace import --language PYTHON --format SOURCE scripts/1_bronze.py /Repos/<your-path>/scripts/1_bronze --overwrite
```

- Convert locally to an IPython notebook (optional) using `jupytext`:

```bash
pip install jupytext
jupytext --to notebook scripts/1_bronze.py
```

Artifacts I added
-----------------
- `scripts/cluster-init-install-packages.sh` — cluster init script to install required Python packages (uploaded to your repo). Place it on DBFS and reference it in cluster init scripts.
- `scripts/databricks_job.json` — Job template to import the pipeline workflow (adjust notebook paths and cluster settings before import).

Upload init script to DBFS & create the Job
-----------------------------------------
1) Upload the init script to DBFS (example):

```bash
# Upload local script to DBFS
databricks fs cp scripts/cluster-init-install-packages.sh dbfs:/databricks/init/cluster-init-install-packages.sh --overwrite
```

2) Import the Job JSON to create the workflow:

```bash
# Create job from JSON template
databricks jobs create --json-file scripts/databricks_job.json
```

Notes on the Job JSON
- The provided `databricks_job.json` uses `notebook_task` entries. If you prefer to run Python files directly, either import the `.py` files as notebooks, or modify the JSON to use `spark_python_task` with a Python file stored in DBFS or a wheel.

Next steps I completed for you
-----------------------------
- Created `scripts/cluster-init-install-packages.sh`
- Created `scripts/databricks_job.json`
- Marked the corresponding todo items completed in the project TODO list

What I can do next (pick one)
- Upload the init script to DBFS and return the exact CLI command I used
- Generate a Jobs API `curl` command (with example auth headers) to create the job
- Convert the Python scripts into workspace notebooks and update the job JSON with those notebook paths

---
Generated: 2026-01-25
