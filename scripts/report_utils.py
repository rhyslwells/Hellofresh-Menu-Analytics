"""
Shared Report Utilities

Purpose
-------
Common functions shared between weekly reports (4_weekly_report.py) and 
dashboard (5_dashboard.py) to avoid code duplication.

Functions
---------
Database Functions
  - get_db_connection(): Get SQLite connection
  
Query Functions
  - get_week_start_date(date_str): Get Monday of week
  - get_latest_week(conn): Get most recent week from gold metrics
  
Chart Generation
  - build_html_page(): Build complete HTML page with charts
  - wrap_chart_in_div(): Wrap Plotly chart in container
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

# Data visualization
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import pandas as pd
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


# ======================
# Configuration
# ======================

DB_PATH = Path("hfresh/hfresh.db")
PROJECT_ROOT = Path.cwd()


# ======================
# Database Connection
# ======================

def get_db_connection() -> sqlite3.Connection:
    """Get SQLite database connection with row factory."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ======================
# Date Utilities
# ======================

def get_week_start_date(date_str: str) -> str:
    """Get the Monday (week start) of the week containing the given date."""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    # Monday is 0, Sunday is 6
    days_since_monday = date.weekday()
    week_start = date - timedelta(days=days_since_monday)
    return week_start.strftime("%Y-%m-%d")


def get_latest_week(conn: sqlite3.Connection) -> str:
    """Get the most recent week from Gold metrics."""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(week_start_date) FROM weekly_menu_metrics")
    result = cursor.fetchone()
    
    if result and result[0]:
        return result[0]
    return datetime.now().strftime("%Y-%m-%d")


# ======================
# HTML Building
# ======================

def get_base_styles() -> str:
    """Return base CSS styles for reports and dashboard."""
    return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            padding: 40px;
        }
        h1 {
            color: #2E86AB;
            margin-bottom: 15px;
            font-size: 2.5em;
            border-bottom: 3px solid #2E86AB;
            padding-bottom: 15px;
        }
        h2 {
            color: #2E86AB;
            margin-top: 40px;
            margin-bottom: 15px;
            font-size: 1.8em;
            padding-bottom: 10px;
            border-bottom: 1px solid #ddd;
        }
        h3 {
            color: #333;
            margin-top: 25px;
            margin-bottom: 10px;
            font-size: 1.3em;
        }
        .metadata {
            background: #f0f4f8;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
            border-left: 4px solid #2E86AB;
        }
        .metadata p {
            margin: 5px 0;
            color: #555;
        }
        .metadata strong {
            color: #2E86AB;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
            background: white;
            border-radius: 5px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        }
        th, td {
            border: 1px solid #ddd;
            padding: 12px 15px;
            text-align: left;
        }
        th {
            background-color: #2E86AB;
            color: white;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #f0f0f0;
        }
        .chart-container {
            margin: 30px 0;
            padding: 20px;
            background: #f9f9f9;
            border-radius: 5px;
            border-left: 4px solid #A23B72;
        }
        .chart-container h3 {
            color: #A23B72;
            margin-top: 0;
        }
        .executive-summary {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 5px;
            margin: 20px 0;
        }
        .executive-summary ul {
            list-style: none;
            padding-left: 0;
        }
        .executive-summary li {
            padding: 8px 0;
            font-size: 1.1em;
        }
        .executive-summary li:before {
            content: "✓ ";
            margin-right: 10px;
            font-weight: bold;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 5px;
            text-align: center;
        }
        .metric-card .value {
            font-size: 2.5em;
            font-weight: bold;
            margin: 10px 0;
        }
        .metric-card .label {
            font-size: 0.95em;
            opacity: 0.9;
        }
        .data-quality {
            background: #e8f5e9;
            padding: 15px;
            border-radius: 5px;
            margin-top: 30px;
            border-left: 4px solid #4caf50;
        }
        .data-quality ul {
            list-style: none;
            padding-left: 0;
        }
        .data-quality li {
            padding: 5px 0;
            color: #2e7d32;
        }
        .data-quality li:before {
            content: "✓ ";
            margin-right: 8px;
            font-weight: bold;
        }
        .intro-section {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 35px;
            border-radius: 8px;
            margin: 30px 0;
        }
        .intro-section p {
            font-size: 1.1em;
            line-height: 1.8;
            margin-bottom: 18px;
        }
        .btn {
            display: inline-block;
            padding: 10px 20px;
            background: #2E86AB;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            transition: all 0.2s;
            font-weight: 600;
            margin-top: 15px;
        }
        .btn:hover {
            background: #1f5a78;
            box-shadow: 0 4px 12px rgba(46, 134, 171, 0.3);
        }
    """


def wrap_chart_in_div(chart_html: str, title: str = "") -> str:
    """Wrap a Plotly chart HTML in a styled container div."""
    html_parts = []
    html_parts.append('<div class="chart-container">')
    if title:
        html_parts.append(f'<h3>{title}</h3>')
    html_parts.append(chart_html)
    html_parts.append('</div>')
    return "\n".join(html_parts)


def build_html_page(
    title: str,
    subtitle: str,
    content_sections: List[Dict[str, str]],
    metadata: Optional[Dict[str, str]] = None,
    additional_styles: str = "",
) -> str:
    """Build a complete HTML page with embedded charts and content.
    
    Parameters
    ----------
    title : str
        Main page title (h1)
    subtitle : str
        Page subtitle
    content_sections : List[Dict[str, str]]
        List of sections with keys: 'title' (optional), 'content' (html string)
    metadata : Optional[Dict[str, str]]
        Metadata to display (created_at, last_updated, etc)
    additional_styles : str
        Additional CSS to add to the page
    
    Returns
    -------
    str
        Complete HTML page as string
    """
    html_parts = []
    
    # HTML header
    html_parts.append("<!DOCTYPE html>")
    html_parts.append("<html>")
    html_parts.append("<head>")
    html_parts.append('    <meta charset="UTF-8">')
    html_parts.append('    <meta name="viewport" content="width=device-width, initial-scale=1.0">')
    html_parts.append(f"    <title>{title}</title>")
    html_parts.append("    <script src=\"https://cdn.plot.ly/plotly-latest.min.js\"></script>")
    html_parts.append("    <style>")
    html_parts.append(get_base_styles())
    if additional_styles:
        html_parts.append(additional_styles)
    html_parts.append("    </style>")
    html_parts.append("</head>")
    html_parts.append("<body>")
    
    # Container start
    html_parts.append('<div class="container">')
    
    # Title and subtitle
    html_parts.append(f"    <h1>{title}</h1>")
    if subtitle:
        html_parts.append(f'    <p style="font-size: 1.2em; color: #666; margin-bottom: 20px;">{subtitle}</p>')
    
    # Metadata section
    if metadata:
        html_parts.append('<div class="metadata">')
        for key, value in metadata.items():
            html_parts.append(f"        <p><strong>{key}:</strong> {value}</p>")
        html_parts.append('</div>')
    
    # Content sections
    for section in content_sections:
        if 'title' in section and section['title']:
            html_parts.append(f"    <h2>{section['title']}</h2>")
        if 'content' in section:
            html_parts.append(f"    {section['content']}")
    
    # Container end
    html_parts.append('</div>')
    
    # Footer
    html_parts.append(f"    <script>console.log('Page generated at {datetime.now(timezone.utc).isoformat()}');</script>")
    html_parts.append("</body>")
    html_parts.append("</html>")
    
    return "\n".join(html_parts)


# ======================
# Data Queries
# ======================

def get_all_historical_weeks(conn: sqlite3.Connection) -> List[str]:
    """Get all available weeks from historical data, ordered chronologically."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT week_start_date
        FROM weekly_menu_metrics
        ORDER BY week_start_date ASC
    """)
    return [row[0] for row in cursor.fetchall()]


def get_summary_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Get key summary metrics from all historical data."""
    cursor = conn.cursor()
    
    metrics = {}
    
    # Total recipes in database
    cursor.execute("SELECT COUNT(*) FROM recipes WHERE is_active = 1")
    metrics['total_recipes'] = cursor.fetchone()[0] or 0
    
    # Average difficulty
    cursor.execute("""
        SELECT ROUND(AVG(CAST(difficulty AS REAL)), 2)
        FROM recipes
        WHERE is_active = 1
    """)
    metrics['avg_difficulty'] = cursor.fetchone()[0] or 0
    
    # Total unique ingredients
    cursor.execute("SELECT COUNT(*) FROM ingredients WHERE is_active = 1")
    metrics['total_ingredients'] = cursor.fetchone()[0] or 0
    
    # Total weeks of data
    cursor.execute("SELECT COUNT(DISTINCT week_start_date) FROM weekly_menu_metrics")
    metrics['total_weeks'] = cursor.fetchone()[0] or 0
    
    # Average recipes per week
    cursor.execute("""
        SELECT ROUND(AVG(CAST(total_recipes AS REAL)), 1)
        FROM weekly_menu_metrics
    """)
    metrics['avg_recipes_per_week'] = cursor.fetchone()[0] or 0
    
    return metrics


# ======================
# Chart Styling Helpers
# ======================

def _get_standard_layout(**kwargs) -> Dict[str, Any]:
    """Get standard chart layout with common styling."""
    standard = {
        'plot_bgcolor': 'white',
        'paper_bgcolor': '#f9f9f9',
        'hovermode': 'x unified',
    }
    standard.update(kwargs)
    return standard


def _apply_grid_styling(fig, x_grid=False, y_grid=True):
    """Apply standard grid styling to figure axes."""
    fig.update_xaxes(showgrid=x_grid, gridcolor='lightgray' if x_grid else None)
    fig.update_yaxes(showgrid=y_grid, gridwidth=1 if y_grid else None, gridcolor='lightgray' if y_grid else None)
    return fig


# ======================
# Color Constants
# ======================

COLOR_PRIMARY = '#2E86AB'
COLOR_ACCENT = '#A23B72'
COLOR_SUCCESS = '#28A745'
COLOR_DANGER = '#DC3545'


# ======================
# Data Checking Helpers
# ======================

def check_data_available(conn: sqlite3.Connection) -> Dict[str, int]:
    """Check how much data is available for charting."""
    cursor = conn.cursor()
    
    data_check = {}
    
    cursor.execute("SELECT COUNT(DISTINCT week_start_date) FROM menu_stability_metrics")
    data_check['stability_weeks'] = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM ingredient_trends")
    data_check['ingredient_trends'] = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM allergen_density")
    data_check['allergen_density'] = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(DISTINCT week_start_date) FROM weekly_menu_metrics")
    data_check['weeks_in_metrics'] = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM recipes WHERE is_active = 1")
    data_check['active_recipes'] = cursor.fetchone()[0] or 0
    
    return data_check


# ======================
# Chart Generation Functions
# ======================

def generate_ingredient_trends_chart(conn: sqlite3.Connection) -> str:
    """Chart 1: Ingredient trends over time - line chart showing ingredient usage across all weeks."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating ingredient trends chart (all historical data)...")
    
    cursor = conn.cursor()
    
    # Get top 8 ingredients by average usage across all weeks
    cursor.execute("""
        SELECT ingredient_name
        FROM ingredient_trends
        GROUP BY ingredient_name
        HAVING AVG(recipe_count) > 0
        ORDER BY AVG(recipe_count) DESC
        LIMIT 8
    """)
    
    top_ingredients = [row[0] for row in cursor.fetchall()]
    
    if not top_ingredients:
        print("    ⚠️  No ingredient trends data available")
        return ""
    
    # Get weekly data for top ingredients
    placeholders = ", ".join(["?" for _ in top_ingredients])
    cursor.execute(f"""
        SELECT 
            week_start_date,
            ingredient_name,
            recipe_count
        FROM ingredient_trends
        WHERE ingredient_name IN ({placeholders})
        ORDER BY week_start_date ASC, ingredient_name
    """, top_ingredients)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly ingredient data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'ingredient_name', 'recipe_count'])
    
    fig = px.line(
        df,
        x='week_start_date',
        y='recipe_count',
        color='ingredient_name',
        title='Ingredient Usage Trends Over Time (Top 8)',
        labels={'week_start_date': 'Week', 'recipe_count': 'Recipe Count', 'ingredient_name': 'Ingredient'},
        height=450
    )
    
    fig.update_layout(_get_standard_layout(
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated with {len(df)} data points from {df['week_start_date'].nunique()} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="ingredient_trends_chart")


def generate_menu_stability_chart(conn: sqlite3.Connection) -> str:
    """Chart 2: Menu stability metrics - timeline chart showing overlap % and churn over time."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating menu stability metrics chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            overlap_with_prev_week * 100 as overlap_pct,
            (CASE WHEN overlap_with_prev_week IS NULL THEN NULL 
                  ELSE (1 - overlap_with_prev_week) * 100 END) as churn_pct,
            recipes_added,
            recipes_removed
        FROM menu_stability_metrics
        WHERE week_start_date IS NOT NULL
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No menu stability data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'overlap_pct', 'churn_pct', 'recipes_added', 'recipes_removed'])
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Scatter(
            x=df['week_start_date'],
            y=df['overlap_pct'],
            name='Menu Overlap %',
            mode='lines+markers',
            line=dict(color=COLOR_PRIMARY, width=2),
            marker=dict(size=6),
        ),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['recipes_added'],
            name='Recipes Added',
            marker=dict(color=COLOR_SUCCESS),
            opacity=0.6,
        ),
        secondary_y=True,
    )
    
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['recipes_removed'],
            name='Recipes Removed',
            marker=dict(color=COLOR_DANGER),
            opacity=0.6,
        ),
        secondary_y=True,
    )
    
    fig.update_layout(_get_standard_layout(
        title='Menu Stability Metrics Over Time',
        height=450,
        xaxis_title='Week',
        barmode='group',
    ))
    fig.update_yaxes(title_text="Overlap %", secondary_y=False, showgrid=True, gridcolor='lightgray')
    fig.update_yaxes(title_text="Recipes Added/Removed", secondary_y=True)
    fig.update_xaxes(showgrid=False)
    
    print(f"    ✓ Generated stability metrics for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="menu_stability_chart")


def generate_allergen_patterns_chart(conn: sqlite3.Connection) -> str:
    """Chart 3: Allergen patterns - temporal heatmap of allergen density across all weeks."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating allergen patterns heatmap...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            allergen_name,
            ROUND(percentage_of_menu, 2) as allergen_density
        FROM allergen_density
        ORDER BY week_start_date ASC, allergen_name
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No allergen density data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'allergen_name', 'allergen_density'])
    
    pivot_df = df.pivot(index='allergen_name', columns='week_start_date', values='allergen_density')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='YlOrRd',
        colorbar=dict(title="Density"),
        hovertemplate='<b>%{y}</b><br>Week: %{x}<br>Density: %{z:.3f}<extra></extra>',
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Allergen Density Patterns Over Time',
        xaxis_title='Week',
        yaxis_title='Allergen Type',
        height=400,
    ))
    
    print(f"    ✓ Generated heatmap for {pivot_df.shape[0]} allergens across {pivot_df.shape[1]} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="allergen_patterns_chart")


def generate_recipe_difficulty_chart(conn: sqlite3.Connection) -> str:
    """Chart 4: Recipe difficulty distribution - bar chart of recipes by difficulty level."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating recipe difficulty distribution chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            difficulty,
            COUNT(*) as recipe_count
        FROM recipes
        WHERE is_active = 1
        GROUP BY difficulty
        ORDER BY difficulty ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No recipe difficulty data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['difficulty', 'recipe_count'])
    
    fig = px.bar(
        df,
        x='difficulty',
        y='recipe_count',
        title='Recipe Distribution by Difficulty Level',
        labels={'difficulty': 'Difficulty Level', 'recipe_count': 'Number of Recipes'},
        height=400,
        color='difficulty',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig.update_layout(_get_standard_layout(showlegend=False))
    _apply_grid_styling(fig, x_grid=False, y_grid=True)
    
    print(f"    ✓ Generated distribution for {df['recipe_count'].sum()} total recipes")
    
    return fig.to_html(include_plotlyjs=False, div_id="difficulty_chart")


def generate_menu_evolution_chart(conn: sqlite3.Connection) -> str:
    """Chart 5: Menu evolution - area chart showing total recipes and new recipes per week."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating menu evolution chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            total_recipes,
            new_recipes,
            returning_recipes
        FROM weekly_menu_metrics
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No menu evolution data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'total_recipes', 'new_recipes', 'returning_recipes'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['returning_recipes'],
        name='Returning Recipes',
        mode='lines',
        line=dict(color=COLOR_PRIMARY),
        stackgroup='one',
        fillcolor=COLOR_PRIMARY,
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['new_recipes'],
        name='New Recipes',
        mode='lines',
        line=dict(color=COLOR_ACCENT),
        stackgroup='one',
        fillcolor=COLOR_ACCENT,
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Menu Evolution: New vs Returning Recipes',
        xaxis_title='Week',
        yaxis_title='Number of Recipes',
        height=400,
    ))
    _apply_grid_styling(fig, x_grid=False, y_grid=True)
    
    print(f"    ✓ Generated menu evolution for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="menu_evolution_chart")


def generate_weekly_difficulty_chart(conn: sqlite3.Connection) -> str:
    """Chart: Weekly average recipe difficulty - line chart showing difficulty trends per week."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating weekly recipe difficulty chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            avg_difficulty
        FROM weekly_menu_metrics
        WHERE avg_difficulty IS NOT NULL
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly difficulty data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'avg_difficulty'])
    
    fig = px.line(
        df,
        x='week_start_date',
        y='avg_difficulty',
        title='Average Recipe Difficulty Per Week',
        labels={'week_start_date': 'Week', 'avg_difficulty': 'Average Difficulty'},
        height=450,
        markers=True
    )
    
    fig.update_traces(
        line=dict(color=COLOR_ACCENT, width=3),
        marker=dict(size=8)
    )
    
    fig.update_layout(_get_standard_layout(
        showlegend=False,
        yaxis=dict(range=[0, 5]),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated weekly difficulty chart for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="weekly_difficulty_chart")


def generate_recipe_tags_chart(conn: sqlite3.Connection) -> str:
    """Chart: Recipe tags analytics - stacked bar chart showing tag distribution per week."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating recipe tags analytics chart...")
    
    cursor = conn.cursor()
    
    # Get top 8 tags by average usage
    cursor.execute("""
        SELECT tag_name
        FROM recipe_tags_analytics
        GROUP BY tag_name
        HAVING AVG(recipe_count) > 0
        ORDER BY AVG(recipe_count) DESC
        LIMIT 8
    """)
    
    top_tags = [row[0] for row in cursor.fetchall()]
    
    if not top_tags:
        print("    ⚠️  No recipe tags data available")
        return ""
    
    # Get weekly data for top tags
    placeholders = ", ".join(["?" for _ in top_tags])
    cursor.execute(f"""
        SELECT 
            week_start_date,
            tag_name,
            percentage_of_menu
        FROM recipe_tags_analytics
        WHERE tag_name IN ({placeholders})
        ORDER BY week_start_date ASC, tag_name
    """, top_tags)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly tag data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'tag_name', 'percentage_of_menu'])
    
    fig = px.area(
        df,
        x='week_start_date',
        y='percentage_of_menu',
        color='tag_name',
        title='Recipe Tags Distribution Over Time (Top 8 Tags)',
        labels={'week_start_date': 'Week', 'percentage_of_menu': 'Percentage of Menu', 'tag_name': 'Tag'},
        height=450,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    
    fig.update_layout(_get_standard_layout(
        hovermode='x unified',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated with {len(df)} data points from {df['week_start_date'].nunique()} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="recipe_tags_chart")


def generate_ingredient_complexity_chart(conn: sqlite3.Connection) -> str:
    """Chart: Ingredient complexity metrics - line chart showing ingredient count trends per week."""
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating ingredient complexity chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            avg_ingredients_per_recipe,
            min_ingredients,
            max_ingredients
        FROM ingredient_complexity_metrics
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No ingredient complexity data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'avg_ingredients_per_recipe', 'min_ingredients', 'max_ingredients'])
    
    fig = go.Figure()
    
    # Add average line
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['avg_ingredients_per_recipe'],
        name='Average Ingredients per Recipe',
        mode='lines+markers',
        line=dict(color=COLOR_PRIMARY, width=3),
        marker=dict(size=7),
    ))
    
    # Add range as shaded area (max - min)
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['max_ingredients'],
        fill=None,
        mode='lines',
        line_color='rgba(0,0,0,0)',
        showlegend=False,
        name='Max',
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['min_ingredients'],
        fill='tonexty',
        mode='lines',
        line_color='rgba(0,0,0,0)',
        name='Min-Max Range',
        fillcolor='rgba(46, 134, 171, 0.2)',
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Ingredient Complexity Metrics Over Time',
        xaxis_title='Week',
        yaxis_title='Number of Ingredients',
        hovermode='x unified',
        height=450,
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated ingredient complexity data for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="ingredient_complexity_chart")


def generate_summary_metrics_panel(conn: sqlite3.Connection) -> str:
    """Generate summary metrics cards showing key aggregates."""
    print("  → Generating summary metrics panel...")
    
    metrics = get_summary_metrics(conn)
    
    html_parts = []
    html_parts.append('<div class="metrics-grid">')
    
    cards = [
        ('total_recipes', 'Total Recipes', 'recipes'),
        ('total_ingredients', 'Unique Ingredients', 'ingredients'),
        ('total_weeks', 'Weeks of Data', 'weeks'),
        ('avg_recipes_per_week', 'Avg Recipes/Week', 'recipes'),
    ]
    
    for key, label, unit in cards:
        value = metrics.get(key, 0)
        html_parts.append(f'''
    <div class="metric-card">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        <div class="label">{unit}</div>
    </div>
        ''')
    
    html_parts.append('</div>')
    
    print("    ✓ Generated 4 metric cards")
    
    return "\n".join(html_parts)
