# HelloFresh Menu Analytics

Lightweight weekly data pipeline: ingest HelloFresh API, normalise with SCD Type 2, and generate analyses & charts in a dashboard and weekly reports.

**Outputs:** SQLite DB (bronze/silver/gold) & weekly reports with visualisations

**View live:** https://rhyslwells.github.io/Hellofresh-Menu-Analytics/

## Quick Start

1. [Run the pipeline](notes/technical/LOCAL_DEV.md)

## Repository Structure

- `scripts/` — pipeline stages (bronze → gold)
- `hfresh/` — database and outputs
- `notes/` — architecture, design, and reference

## Learn More

- [System architecture](notes/references/blueprint.md)
- [Entity-relationship diagram](notes/references/er-diagram.md)
- [SCD Type 2 reference](notes/references/SCD-Type-2.md)
- [GitHub Actions setup](notes/technical/GITHUB_SETUP.md)

**Automation:** Weekly (Friday 02:00 UTC via GitHub Actions)
