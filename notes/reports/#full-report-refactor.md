# Main Page Refactor

## Objective

Transform the HelloFresh Menu Analytics dashboard from static weekly reports to a dynamic, interactive main page with exploratory data analysis capabilities using all historical data.

## Key Goals

1. **Create dashboard update script** (`scripts/5_dashboard.py`)
   - Generates interactive charts from all historical data
   - Runs after gold layer is built
   - Regenerates `docs/index.html` with embedded Plotly charts

2. **Shift main page from tables to charts**
   - Interactive visualizations instead of static tables
   - Multi-view analysis across all data
   - Keep tables in weekly reports for archives only

3. **Refactor for code reusability**
   - Extract shared functions into `scripts/report_utils.py`
   - Avoid duplication between `4_weekly_report.py` and `5_dashboard.py`
   - Use `3_gold_analytics.py` and `2_silver.py` as data references

4. **Integrate into pipeline**
   - Update `.github/workflows/pipeline.yml` to run `5_dashboard.py` after gold layer
   - Weekly reports still generate on schedule
   - Dashboard updates with each pipeline run

## Current State

- Weekly reports: `4_weekly_report.py` generates static snapshots
- Main page: `docs/index.html` (basic HTML, limited interactivity)
- Charts: Only in weekly reports
- Data source: Gold layer in SQLite

## Vision: Main Page (Dashboard) vs Weekly Reports

### Main Page (`docs/index.html`) - DYNAMIC DASHBOARD

**Scope**: All historical data, exploratory analysis  
**Purpose**: Interactive dashboard for data exploration  
**Update Trigger**: After each pipeline run

**Chart Deliverables**:

1. **Ingredient Trends Over Time** - Line/bar chart showing ingredient usage across all weeks
2. **Menu Stability Metrics** - Chart showing overlap % and recipe churn over time
3. **Allergen Patterns** - Temporal heatmap of allergen density across all weeks
4. **Recipe Difficulty Distribution** - Chart of recipes by difficulty level
5. **Summary Metrics Panel** - Key aggregates (total recipes, avg difficulty, etc.)

*Sliders will be added in Phase 2 once all charts are working with full data*

### Weekly Reports (`4_weekly_report.py`) - STATIC ARCHIVES

**Scope**: Single week snapshot  
**Purpose**: Time-based archives of data state  
**Update Trigger**: Weekly schedule (kept as-is)

**Contents**:
- Menu stability table (that week only)
- Top 5 recipes by difficulty
- Trending ingredients chart (week-specific)
- Allergen heatmap (week-specific)

---

## Implementation Strategy

### Phase 1: Dashboard Script & Main Page Charts (PRIORITY)

**Step 1: Create Shared Utilities Module**
- [ ] Create `scripts/report_utils.py`
- [ ] Extract database functions (queries, connections)
- [ ] Extract chart generation functions (reusable between scripts)
- [ ] Extract HTML building utilities

**Step 2: Create Dashboard Script (`scripts/5_dashboard.py`)**
- [ ] Build `5_dashboard.py` using `3_gold_analytics.py` and `2_silver.py` as data references
- [ ] Queries for full historical data (not week-specific)
- [ ] Import shared functions from `report_utils.py`

**Step 3: Implement Charts (All Historical Data)**
- [ ] Chart 1: Ingredient trends (line/bar showing usage across all weeks)
- [ ] Chart 2: Menu stability metrics (timeline of overlap % and churn)
- [ ] Chart 3: Allergen patterns (heatmap with all weeks)
- [ ] Chart 4: Recipe difficulty distribution (bar/histogram)
- [ ] Summary metrics panel (aggregates)

**Step 4: Generate Main Page HTML**
- [ ] Build `docs/index.html` with embedded Plotly charts
- [ ] Include summary section
- [ ] Style consistently with existing reports

**Step 5: Integrate into Pipeline**
- [ ] Update `.github/workflows/pipeline.yml` to run `5_dashboard.py` after `3_gold_analytics.py`
- [ ] Add dashboard generation step to workflow

### Phase 2: Interactive Features (Later)

- [ ] Add date range sliders to main page charts
- [ ] Implement client-side filtering if needed
- [ ] Performance optimization

### Phase 3: Weekly Reports Simplification (After Dashboard Stable)

- [ ] Remove duplicated chart code from `4_weekly_report.py`
- [ ] Keep tables for week-specific snapshots
- [ ] Optimize generation time

---

## Decisions & Technical Notes

| Decision | Rationale |
|----------|-----------|
| Dashboard script location | `scripts/5_dashboard.py` - alongside other pipeline scripts |
| Shared code location | `scripts/report_utils.py` - avoids duplication |
| Data references | Use `3_gold_analytics.py` and `2_silver.py` structure |
| HTML generation | Generate `docs/index.html` on each pipeline run |
| Chart selection | Use initiative based on available data and interesting insights |
| Slider implementation | **Defer to Phase 2** - focus on getting all-data charts working first |
| Pipeline integration | Update `.github/workflows/pipeline.yml` to run dashboard after gold layer |
| Weekly report updates | Continue weekly schedule, keep table + chart structure, no changes yet |

## Data Sources

- **Gold Layer Tables**: Use queries from `3_gold_analytics.py` as starting point
- **Silver Layer**: Reference `2_silver.py` for data transformation patterns
- **Historical Data**: Query full dataset, no date filters (except per-week granularity where appropriate)

## Success Criteria

- [ ] Main page displays 4-5 interactive charts from all historical data
- [ ] Charts embed Plotly visualizations in `docs/index.html`
- [ ] Code shared between `4_weekly_report.py` and `5_dashboard.py` via `report_utils.py`
- [ ] Dashboard regenerates automatically on pipeline run
- [ ] Dashboard displays meaningful insights about menu evolution, ingredients, and allergens