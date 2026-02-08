# Plotly Migration Summary - 4_weekly_report.py

## Overview
Successfully migrated `4_weekly_report.py` from Matplotlib to Plotly with HTML-embedded interactive charts.

## Changes Made

### 1. **Dependencies Updated**
- **Removed**: `matplotlib`, `seaborn`
- **Added**: `plotly` (graph_objects, express, subplots)
- **Kept**: `pandas` (required for both)
- **Removed**: `markdown` module (no longer needed)

### 2. **Configuration Changes**
- Removed `CHARTS_DIR` variable (no separate chart files needed)
- Charts are now embedded directly in HTML

### 3. **Chart Generation Functions - Converted to Plotly**

All four chart functions now return HTML strings instead of saving PNG files:

#### `generate_menu_overlap_chart()`
- **Before**: Saved PNG using matplotlib subplots (2 charts stacked)
- **After**: Returns interactive Plotly HTML with:
  - Line chart with area fill for overlap percentage
  - Grouped bar chart for added vs removed recipes
  - Hover tooltips and data labels

#### `generate_recipe_survival_chart()`
- **Before**: Saved PNG histogram using matplotlib
- **After**: Returns interactive histogram with:
  - Overlaid histograms for active vs churned recipes
  - Color-coded by status
  - Interactive legend

#### `generate_ingredient_trends_chart()`
- **Before**: Saved PNG line plot using pandas/matplotlib
- **After**: Returns interactive Plotly line chart with:
  - Multiple ingredients as separate traces
  - Markers and line styling
  - Legend for ingredient names

#### `generate_allergen_density_chart()`
- **Before**: Saved PNG heatmap using seaborn
- **After**: Returns interactive Plotly heatmap with:
  - Color scale (RdYlGn_r)
  - Hover information with percentages
  - Week and allergen labels

### 4. **Report Generation - Complete Refactor**

#### Old: `generate_markdown_report()` → New: `generate_html_report()`
- Now generates HTML directly instead of markdown
- Embeds all 4 charts inline
- Includes all data tables with proper HTML formatting
- Returns complete HTML content

#### Old: `save_report_to_file()` → New: Enhanced Version
- Takes HTML content instead of markdown
- Includes comprehensive CSS styling:
  - Professional color scheme (#2E86AB primary, #A23B72 accent)
  - Responsive layout with max-width container
  - Interactive table styling with hover effects
  - Gradient backgrounds for sections
  - Box shadows and borders for depth
- Includes Plotly CDN script tag
- Professional HTML template with metadata section
- Executive summary in styled box
- Data quality notes section
- Footer with generation timestamp

### 5. **Main Function Updates**
- Updated to call `generate_html_report()` instead of `generate_markdown_report()`
- Updated to call `save_report_to_file()` with HTML content
- Added check for HAS_PLOTLY with helpful installation message
- Updated progress messages to reflect new workflow

## Output

### File Structure
```
docs/
  weekly_reports/
    YYYY-MM-DD-report.html  (single file with embedded charts)
```

### HTML Report Features
- **Single File**: No separate chart files needed
- **Interactive Charts**: 
  - Zoom, pan, and hover on charts
  - Toggle legend items
  - Download as PNG from chart toolbar
- **Professional Styling**:
  - Clean, modern design
  - Mobile-responsive
  - Color-coded sections
- **Complete Data**:
  - Executive summary
  - Weekly metrics tables
  - Recipe rankings
  - Ingredient trends
  - Allergen analysis
  - Cuisine distribution
  - Recipe lifecycle status

## Usage

```bash
# From command line
python scripts/4_weekly_report.py

# From GitHub Actions (same as before)
python scripts/4_weekly_report.py
```

## Requirements

Install Plotly and Pandas:
```bash
pip install plotly pandas
```

Or update requirements file:
```
plotly>=5.0.0
pandas>=1.0.0
```

## Benefits

✅ **Interactive Charts**: Users can zoom, pan, and hover for details
✅ **Single HTML File**: Easier to share and store (no multiple PNG files)
✅ **Professional Styling**: Modern, polished appearance
✅ **Responsive Design**: Works on desktop and mobile
✅ **No External Dependencies**: Charts embedded via CDN
✅ **Better Interactivity**: Plotly's built-in features
✅ **Easier Maintenance**: All-in-one HTML file
✅ **Smaller File Size**: HTML with CDN is smaller than multiple PNGs

## Backward Compatibility

- Reports save to same location: `docs/weekly_reports/`
- Same filename format: `YYYY-MM-DD-report.html`
- Git commit process unchanged
- Database queries remain identical
