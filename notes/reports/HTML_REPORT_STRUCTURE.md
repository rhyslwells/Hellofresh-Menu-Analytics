# Generated HTML Report Structure

## File Location
```
docs/weekly_reports/2026-01-01-report.html
```

## HTML Structure

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HelloFresh Report - 2026-01-01</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        /* 200+ lines of professional CSS styling */
        /* Colors: #2E86AB (primary), #A23B72 (accent) */
        /* Responsive layout with gradients and shadows */
    </style>
</head>
<body>
    <div class="container">
        
        <!-- HEADER SECTION -->
        <h1>HelloFresh Data Analysis Report</h1>
        <div class="metadata">
            <p><strong>Generated:</strong> 2026-01-01 14:30</p>
            <p><strong>Analysis Period:</strong> Week of 2026-01-01</p>
            <p><strong>Data Source:</strong> SQLite Gold Layer</p>
        </div>
        
        <!-- EXECUTIVE SUMMARY -->
        <h2>Executive Summary</h2>
        <div class="executive-summary">
            <ul>
                <li>✓ <strong>Total Recipes This Week:</strong> 42</li>
                <li>✓ <strong>New Recipes Introduced:</strong> 8</li>
                <li>✓ <strong>Returning Recipes:</strong> 34</li>
                <li>✓ <strong>Average Difficulty:</strong> 3.5</li>
                <li>✓ <strong>Average Prep Time:</strong> 28 minutes</li>
            </ul>
        </div>
        
        <!-- METRICS TABLE -->
        <h3>Weekly Metrics Table</h3>
        <table>
            <thead>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>Total Recipes</td><td>42</td></tr>
                <tr><td>Unique Recipes</td><td>38</td></tr>
                <tr><td>New Recipes</td><td>8</td></tr>
                <tr><td>Returning Recipes</td><td>34</td></tr>
                <tr><td>Avg Difficulty</td><td>3.5</td></tr>
                <tr><td>Avg Prep Time (min)</td><td>28</td></tr>
            </tbody>
        </table>
        
        <!-- SECTION 1: MENU EVOLUTION -->
        <h2>1. Menu Evolution</h2>
        <div class="chart-container">
            <!-- Plotly Interactive Chart (HTML embedded here) -->
            <div id="menu_overlap_chart">
                <!-- Plotly renders interactive chart with:
                     - Line chart with area fill (overlap %)
                     - Grouped bar chart (added vs removed)
                     - Hover tooltips
                     - Zoom, pan, legend controls
                -->
            </div>
        </div>
        
        <h3>Recent Menu Stability</h3>
        <table>
            <thead>
                <tr>
                    <th>Week</th>
                    <th>Overlap %</th>
                    <th>Added</th>
                    <th>Removed</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>2025-12-24</td><td>78.5</td><td>9</td><td>3</td></tr>
                <tr><td>2025-12-31</td><td>81.2</td><td>7</td><td>2</td></tr>
                <tr><td>2026-01-07</td><td>75.3</td><td>12</td><td>5</td></tr>
            </tbody>
        </table>
        
        <!-- SECTION 2: RECIPE LIFECYCLE -->
        <h2>2. Recipe Lifecycle Analysis</h2>
        <div class="chart-container">
            <!-- Plotly Histogram Chart -->
            <div id="recipe_survival_chart">
                <!-- Interactive histogram:
                     - Active vs Churned distributions
                     - Color-coded bars
                     - Hover count information
                -->
            </div>
        </div>
        
        <h3>Top 10 Active Recipes (Current Week)</h3>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Recipe Name</th>
                    <th>Difficulty</th>
                    <th>Prep Time</th>
                    <th>Appearances</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>Grilled Salmon</td><td>3</td><td>25</td><td>8</td></tr>
                <tr><td>2</td><td>Pasta Carbonara</td><td>2</td><td>20</td><td>7</td></tr>
                <!-- ... more recipes ... -->
            </tbody>
        </table>
        
        <!-- SECTION 3: INGREDIENT TRENDS -->
        <h2>3. Ingredient Trends</h2>
        <div class="chart-container">
            <!-- Plotly Line Chart -->
            <div id="ingredient_trends_chart">
                <!-- Interactive multi-line chart:
                     - Top 8 ingredients as separate traces
                     - Color-coded lines
                     - Legend for ingredient selection
                     - Hover with values
                -->
            </div>
        </div>
        
        <h3>Top Trending Ingredients (Latest Week)</h3>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Ingredient</th>
                    <th>Recipes</th>
                    <th>Popularity</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>Salmon</td><td>12</td><td>#1</td></tr>
                <tr><td>2</td><td>Garlic</td><td>11</td><td>#2</td></tr>
                <!-- ... more ingredients ... -->
            </tbody>
        </table>
        
        <h3>Most Used Ingredients (All Time)</h3>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Ingredient</th>
                    <th>In Recipes</th>
                    <th>Menu Appearances</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>Olive Oil</td><td>89</td><td>245</td></tr>
                <tr><td>2</td><td>Garlic</td><td>76</td><td>198</td></tr>
                <!-- ... more ingredients ... -->
            </tbody>
        </table>
        
        <!-- SECTION 4: ALLERGEN ANALYSIS -->
        <h2>4. Allergen Analysis</h2>
        <div class="chart-container">
            <!-- Plotly Heatmap -->
            <div id="allergen_density_chart">
                <!-- Interactive heatmap:
                     - Allergens as rows
                     - Weeks as columns
                     - Color intensity = percentage
                     - Hover with exact values
                -->
            </div>
        </div>
        
        <!-- SECTION 5: CUISINE DISTRIBUTION -->
        <h2>5. Cuisine Distribution</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Cuisine</th>
                    <th>Recipes</th>
                    <th>Menu Appearances</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>1</td><td>Mediterranean</td><td>15</td><td>42</td></tr>
                <tr><td>2</td><td>Asian</td><td>12</td><td>35</td></tr>
                <!-- ... more cuisines ... -->
            </tbody>
        </table>
        
        <!-- SECTION 6: RECIPE LIFECYCLE STATUS -->
        <h2>6. Recipe Lifecycle Status</h2>
        <table>
            <thead>
                <tr>
                    <th>Status</th>
                    <th>Count</th>
                    <th>Avg Days Active</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>Active</td><td>245</td><td>157</td></tr>
                <tr><td>Inactive</td><td>198</td><td>89</td></tr>
            </tbody>
        </table>
        
        <!-- DATA QUALITY SECTION -->
        <div class="data-quality">
            <h3>Data Quality Notes</h3>
            <ul>
                <li><strong>Report Generated:</strong> 2026-01-01T14:30:45.123456</li>
                <li><strong>Week Start Date:</strong> 2026-01-01</li>
                <li><strong>Data Source:</strong> SQLite Database (hfresh/hfresh.db)</li>
            </ul>
        </div>
        
        <!-- FOOTER -->
        <footer>
            <p>This report was generated automatically by the HelloFresh Data Platform.</p>
            <p>Generated on 2026-01-01 14:30:45</p>
        </footer>
        
    </div>
</body>
</html>
```

## CSS Classes and Styling

### Main Container
- `.container` - Max width 1200px, white background, centered with shadow

### Typography
- `h1` - #2E86AB color, 2.5em size, bottom border
- `h2` - #2E86AB color, 1.8em size, bottom border
- `h3` - Dark gray, 1.3em size

### Components
- `.metadata` - Light blue background, left border accent
- `.executive-summary` - Purple gradient, white text, checkmarks
- `.chart-container` - Light gray background, left border accent
- `.data-quality` - Light green background, left border accent

### Tables
- Header: #2E86AB background, white text
- Alternating rows: White and light gray
- Hover: Light gray highlight
- Borders: Light gray (#ddd)

## Interactive Features (Plotly)

All charts include:
- ✓ **Zoom**: Click and drag to zoom
- ✓ **Pan**: Hold shift and drag to pan
- ✓ **Hover**: Tooltip with exact values
- ✓ **Legend**: Click to toggle series visibility
- ✓ **Download**: Camera icon to save as PNG
- ✓ **Reset**: Home icon to reset view
- ✓ **Box Select**: Select and zoom to selection

## File Size

- **Typical size**: 400-800 KB (depending on data)
- **CDN load**: Plotly library ~3.5 MB (cached by browser)
- **Total delivery**: Single HTML file
- **Compression**: Gzip compression reduces 30-40%

## Browser Compatibility

- ✓ Chrome 90+
- ✓ Firefox 88+
- ✓ Safari 14+
- ✓ Edge 90+
- ✓ Mobile browsers (responsive design)

## Sharing and Distribution

The HTML file can be:
- ✓ Emailed directly
- ✓ Hosted on web server
- ✓ Viewed locally in browser
- ✓ Uploaded to documentation sites
- ✓ Archived in version control
