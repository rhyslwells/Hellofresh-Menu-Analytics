# Main Page Refactor Goals

## Overview

Transform the HelloFresh Menu Analytics dashboard from static weekly reports to a dynamic, interactive main page with exploratory data analysis capabilities.

## Current State

- Weekly reports generated in `4_weekly_report.py` (static snapshots)
- Main page `docs/index.html` (basic, limited interactivity)
- Charts embedded in weekly reports only

## Vision: Separation of Concerns

### Main Page (`docs/index.html`) - DYNAMIC & INTERACTIVE

**Purpose**: Exploratory data analysis dashboard with full dataset  
**Features**:

- High-level summary charts (all historical data)
- Interactive sliders for temporal selection
- Multi-view analysis capability
- Real-time data exploration

**Deliverables**:

- Chart 1: Ingredient trends over time (with date range slider)
- Chart 2: Menu stability metrics (timeline view)
- Chart 3: Allergen patterns (temporal heatmap with slider)
- Chart 4: Recipe difficulty distribution (with filters)
- Summary metrics panel (updated dynamically)

### Weekly Reports (`4_weekly_report.py`) - STATIC SNAPSHOTS

**Purpose**: Time-based archives of data state  
**Features**:

- Focus on single week in isolation
- Keep current table + chart structure
- Simpler, faster generation
- Stored in `docs/weekly_reports/`

**Keep As Is**:

- Menu stability table for that week
- Top 5 recipes (with difficulty)
- Trending ingredients & allergen heatmap (week-specific)

---

## Implementation Strategy

### Phase 1: Main Page Enhancement

- [ ] Refactor main page to use Plotly for interactive charts
- [ ] Implement date range slider(s) for temporal filtering
- [ ] Add chart data generation functions to support all-data views
- [ ] Style and layout main dashboard

### Phase 2: Simplify Weekly Reports

- [ ] Reduce `4_weekly_report.py` scope (remove chart generation if moved to main)
- [ ] Keep essential snapshot generation
- [ ] Optimize for performance

### Phase 3: Data Architecture

- [ ] Create separate query functions for:
  - Main page (full historical data queries)
  - Weekly reports (single-week queries)

---

## Questions to Confirm

1. Should weekly reports auto-generate weekly, or on-demand?
2. Do you want main page to update when new data arrives?
3. How far back should main page charts show? (all history or last N months?)
