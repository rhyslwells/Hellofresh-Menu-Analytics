# Workflow Dispatch & Manual Backfill Runs

## What is `workflow_dispatch`?

`workflow_dispatch` allows manually triggering a GitHub Actions workflow from the UI instead of relying only on schedules or push events. This enables backfill and debugging.

## How to trigger

1. Go to **Actions** tab → **HelloFresh Pipeline**
2. Click **Run workflow** button
3. Optionally enter the `week` parameter (YYYY-MM-DD format)
4. Click **Run workflow**

## The week parameter

- **Optional**: Defaults to next week if not provided
- **Format**: `YYYY-MM-DD` (e.g., `2026-01-15`)
- **Use case**: Backfill historical data or re-process a specific week

## Implementation

```yaml
on:
  workflow_dispatch:
    inputs:
      week:
        description: 'Week to backfill (YYYY-MM-DD format, optional)'
        required: false
        type: string
```

Each script checks if `--week` is passed and uses it; otherwise defaults to current logic:

```bash
if [ -z "${{ inputs.week }}" ]; then
  uv run python scripts/1_bronze.py
else
  uv run python scripts/1_bronze.py --week "${{ inputs.week }}"
fi
```

## Script support

Ensure your scripts accept the `--week` parameter:

```python
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--week', type=str, default=None, help='Week to process')
args = parser.parse_args()
```

Using "Replace String in File"


```

```
Done! ✅ Added a backup step that:
- Runs before any data processing
- Only backs up if the DB exists (safe for first run)
- Creates timestamped backups: `hfresh-YYYY-MM-DD-HHMMSS.db`
- Stores in backups

This protects against any unexpected data issues during backfill runs via `workflow_dispatch`.

Made changes.


Let me check the scripts to understand the current behavior: 

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/1_bronze.py#1-1), lines 1 to 80

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/init_sqlite.py#1-1), lines 1 to 50

Let me check more of the bronze script to understand the --week parameter handling: 

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/1_bronze.py#80-80), lines 80 to 150

Let me check the main execution part: 

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/1_bronze.py#550-550), lines 550 to 597

Perfect! Now I understand the behavior. Here's my analysis:

## Analysis

**1. Last week of data:**
- ✅ Default behavior: Fetches next week (when run)
- ✅ Can specify `--week` in YYYY-MM-DD format to fetch specific weeks
- The `get_week_bounds()` function calculates Monday-Sunday for that week

**2. Database behavior:**
- ✅ **APPENDS**, does NOT overwrite
- Uses `INSERT` statements (immutable bronze layer)
- Bronze table has `ingestion_timestamp` to track when data was pulled
- No schema reset - tables use `CREATE TABLE IF NOT EXISTS`

**3. Backup strategy:**
- ✅ **Excellent idea** - especially important for `workflow_dispatch` backfill runs
- Should backup before Bronze layer runs (to preserve previous data)
- Store backups in backups

## My recommendations

Add backup step in the workflow:
1. Backup before Bronze ingestion (safer for backfill scenarios)
2. Use timestamp in filename: `hfresh-YYYY-MM-DD-HHMMSS.db`
3. Optionally clean old backups (keep last N)

Here's what I'll add to the workflow:

```yaml
- name: Backup database
  run: |
    if [ -f hfresh/hfresh.db ]; then
      mkdir -p hfresh/backups
      cp hfresh/hfresh.db hfresh/backups/hfresh-$(date +%Y-%m-%d-%H%M%S).db
      echo "Database backed up"
    fi
```

Want me to add this to the pipeline?