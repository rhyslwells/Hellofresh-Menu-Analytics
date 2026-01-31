# HelloFresh weekly pipeline

https://rhyslwells.github.io/Hellofresh-Menu-Analytics/

Lightweight weekly data pipeline that ingests HelloFresh API data,
stores it in a single SQLite file, applies SCD Type 2 normalization,
computes analytics and generates markdown reports + charts.

Key outputs:
- SQLite DB with bronze/silver/gold layers
- Weekly markdown report and 4 PNG charts

Quick links:
- Architecture: [docs/blueprint.md](docs/blueprint.md)
- SCD reference: [docs/SCD-Type-2.md](docs/SCD-Type-2.md)
- API integration: [docs/api.md](docs/api.md)
- Build, run, and todos: [docs/notes.md](docs/notes.md)
- Local dev instructions: [LOCAL_DEV.md](LOCAL_DEV.md)
- GitHub Actions setup: [GITHUB_SETUP.md](GITHUB_SETUP.md)

Repository layout (high level):
- `scripts/` — pipeline steps (`init_sqlite.py`, `1_bronze.py`, `2_silver.py`, `3_gold_analytics.py`, `4_weekly_report.py`)
- `hfresh/` — `hfresh.db` and `output/` (charts + reports)
- `docs/` — design docs and implementation notes

Getting started:
See [docs/notes.md](docs/notes.md) or [LOCAL_DEV.md](LOCAL_DEV.md) for exact
environment and run commands (local setup, API key, and per-script usage).

Status:
- Designed for weekly automation (Friday 02:00 UTC) via GitHub Actions
- Idempotent gold-step outputs; bronze is append-only

If you want I can: run the local setup checklist, or move any remaining
implementation details into `docs/notes.md`.
