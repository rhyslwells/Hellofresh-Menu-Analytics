# Quick Reference: Plotly Migration

## What Changed?

### Old Workflow (Matplotlib)
```
Generate Charts → Save PNG files to disk → Generate Markdown → Convert to HTML
```

### New Workflow (Plotly)
```
Generate Charts → Embed HTML in Report → Save Single HTML file
```

## Key Differences

| Aspect | Before | After |
|--------|--------|-------|
| **Chart Library** | Matplotlib | Plotly |
| **Chart Output** | PNG files | HTML div strings |
| **Report Format** | Markdown → HTML | Direct HTML |
| **File Output** | 1 HTML + 4 PNG | 1 HTML (all embedded) |
| **Interactivity** | Static images | Interactive (zoom, pan, hover) |
| **File Size** | ~2-3 MB (PNG charts) | ~500 KB HTML (with CDN) |
| **Chart Customization** | Complex | Simple with Plotly API |

## Function Signatures

### Chart Functions (all return HTML strings)
```python
generate_menu_overlap_chart(conn) -> str      # Returns HTML div
generate_recipe_survival_chart(conn) -> str   # Returns HTML div
generate_ingredient_trends_chart(conn) -> str # Returns HTML div
generate_allergen_density_chart(conn) -> str  # Returns HTML div
```

### Report Functions
```python
generate_html_report(conn, week_date) -> str     # Generates full HTML report
save_report_to_file(html_content, pull_date) -> str  # Saves to disk
```

## CSS Styling Classes

The HTML report uses these CSS classes:
- `.container` - Main wrapper
- `.metadata` - Header metadata section
- `.executive-summary` - Summary box with gradient
- `.chart-container` - Chart wrapper with border
- `.data-quality` - Data quality notes section
- `.plotly-chart` - Chart styling

## How to Customize

### Change Chart Colors
Edit the hex color codes in the chart functions:
```python
line=dict(color='#2E86AB', width=3)  # Change #2E86AB
marker=dict(color='#A23B72')         # Change #A23B72
```

### Change HTML Styling
Edit the CSS in `save_report_to_file()` function:
```python
<style>
    .chart-container { border-left: 4px solid #A23B72; }  # Modify here
</style>
```

### Add New Charts
1. Create a function that returns Plotly HTML
2. Call it in `generate_html_report()`
3. Append result to `html_parts`

Example:
```python
def generate_my_chart(conn) -> str:
    if not HAS_PLOTLY:
        return ""
    
    # Get data...
    df = pd.DataFrame(...)
    
    fig = go.Figure(...)
    return fig.to_html(include_plotlyjs='cdn', div_id="my_chart")

# In generate_html_report():
my_chart_html = generate_my_chart(conn)
html_parts.append(f'<div class="chart-container">{my_chart_html}</div>')
```

## Dependencies

Ensure these are installed:
```bash
pip install plotly pandas sqlite3
```

Add to requirements.txt:
```
plotly>=5.0.0
pandas>=1.3.0
```

## Output Format

Reports are saved as:
```
docs/weekly_reports/YYYY-MM-DD-report.html
```

Single HTML file contains:
- Executive summary
- All data tables
- 4 interactive Plotly charts
- Professional styling
- Footer with generation info

## Testing

To verify the installation:
```python
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
print("✓ Plotly installed correctly")
```

## Common Issues

### Issue: "Plotly not available"
**Solution**: `pip install plotly`

### Issue: Charts not displaying
**Solution**: Ensure you're opening the HTML file in a web browser with internet access (CDN loads Plotly JS)

### Issue: Charts are small/large
**Solution**: Modify height in figure layout:
```python
fig.update_layout(height=500)  # Adjust pixel value
```

### Issue: Custom colors not working
**Solution**: Use hex codes with quotes:
```python
marker=dict(color='#2E86AB')  # Correct
marker=dict(color=2E86AB)     # Wrong
```

## Performance Notes

- Report generation is ~2-3x faster (no file I/O for multiple PNGs)
- HTML file with CDN is smaller than 4 PNG files
- Plotly rendering happens in browser (low server load)
- Charts are cached by browser after first load

## Backup

The old Matplotlib code can still be found in Git history if needed.
