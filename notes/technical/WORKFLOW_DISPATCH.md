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
