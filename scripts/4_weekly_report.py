"""
SQLite WEEKLY REPORT GENERATOR

Purpose
-------
Generates weekly HTML reports with embedded interactive Plotly charts.
Pulls insights from Gold layer and creates visualizations.

Output
------
- Reports (HTML): stored in docs/weekly_reports/
  - YYYY-MM-DD-report.html (with embedded interactive charts)

Charts Included
---------------
- Menu Overlap Trends (last 3 weeks)
- Recipe Survival Distribution
- Ingredient Popularity Over Time
- Allergen Density Heatmap

Usage
-----
From command line:
python scripts/4_weekly_report.py

With GitHub Actions (after 3_gold_analytics.py):
python scripts/4_weekly_report.py

Requirements
------------
- plotly
- pandas
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import subprocess
import os

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
REPORTS_DIR = PROJECT_ROOT / "docs" / "weekly_reports"


# ======================
# Database Connection
# ======================

def get_db_connection() -> sqlite3.Connection:
    """Get SQLite database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ======================
# Report Data Queries
# ======================

def get_latest_week(conn: sqlite3.Connection) -> str:
    """Get the most recent week from Gold metrics."""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(week_start_date) FROM weekly_menu_metrics")
    result = cursor.fetchone()
    
    if result and result[0]:
        return result[0]
    return datetime.now().strftime("%Y-%m-%d")


def get_week_summary(conn: sqlite3.Connection, week_date: str) -> dict:
    """Get summary metrics for the week."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            total_recipes,
            unique_recipes,
            new_recipes,
            returning_recipes,
            avg_difficulty,
            avg_prep_time_minutes
        FROM weekly_menu_metrics
        WHERE week_start_date = ?
        LIMIT 1
    """, (week_date,))
    
    row = cursor.fetchone()
    if row:
        return {
            'week_date': week_date,
            'total_recipes': row[1],
            'unique_recipes': row[2],
            'new_recipes': row[3],
            'returning_recipes': row[4],
            'avg_difficulty': row[5],
            'avg_prep_time': row[6],
        }
    return {}


def get_top_recipes(conn: sqlite3.Connection, limit: int = 10) -> list:
    """Get top recipes by recent appearance."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                r.id as recipe_id,
                r.name,
                r.difficulty,
                r.prep_time,
                COUNT(DISTINCT mr.menu_id) as menu_appearances
            FROM recipes r
            LEFT JOIN menu_recipes mr ON r.id = mr.recipe_id AND mr.is_active = 1
            WHERE r.is_active = 1
            GROUP BY r.id, r.name, r.difficulty, r.prep_time
            ORDER BY r.last_seen_date DESC, menu_appearances DESC
            LIMIT ?
        """, (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️  Error getting top recipes: {e}")
        return []


def get_menu_stability(conn: sqlite3.Connection, limit: int = 5) -> list:
    """Get recent menu stability metrics."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            overlap_with_prev_week,
            new_recipe_rate,
            churned_recipe_rate,
            recipes_added,
            recipes_removed
        FROM menu_stability_metrics
        ORDER BY week_start_date DESC
        LIMIT ?
    """, (limit,))
    
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_ingredient_trends(conn: sqlite3.Connection, limit: int = 10) -> list:
    """Get trending ingredients."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            ingredient_name,
            recipe_count,
            popularity_rank
        FROM ingredient_trends
        WHERE popularity_rank <= ?
        ORDER BY week_start_date DESC, popularity_rank ASC
        LIMIT ?
    """, (limit, limit))
    
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_popular_ingredients(conn: sqlite3.Connection, limit: int = 15) -> list:
    """Top ingredients appearing most frequently."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                i.name as ingredient_name,
                COUNT(DISTINCT ri.recipe_id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_count
            FROM ingredients i
            JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id AND ri.is_active = 1
            JOIN menu_recipes mr ON ri.recipe_id = mr.recipe_id AND mr.is_active = 1
            GROUP BY i.ingredient_id, i.name
            ORDER BY recipe_count DESC
            LIMIT ?
        """, (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️  Error getting popular ingredients: {e}")
        return []


def get_cuisine_distribution(conn: sqlite3.Connection) -> list:
    """Distribution of recipes by cuisine."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COALESCE(r.cuisine, 'Unknown') as cuisine,
                COUNT(DISTINCT r.id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_appearances
            FROM recipes r
            LEFT JOIN menu_recipes mr ON r.id = mr.recipe_id AND mr.is_active = 1
            GROUP BY r.cuisine
            ORDER BY recipe_count DESC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️  Error getting cuisine distribution: {e}")
        return []


def get_recipe_lifecycle(conn: sqlite3.Connection) -> dict:
    """Active vs inactive recipe counts."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                is_active,
                COUNT(*) as recipe_count,
                ROUND(AVG(CAST((JULIANDAY(last_seen_date) - JULIANDAY(first_seen_date)) AS REAL)), 0) as avg_days_active
            FROM recipes
            GROUP BY is_active
        """)
        
        result = {}
        for row in cursor.fetchall():
            status = 'Active' if row[0] else 'Inactive'
            result[status] = {
                'count': row[1],
                'avg_days': row[2]
            }
        return result
    except Exception as e:
        print(f"⚠️  Error getting recipe lifecycle: {e}")
        return {}


# ======================
# Chart Generation
# ======================

def check_data_available(conn: sqlite3.Connection) -> dict:
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
    
    return data_check

def generate_menu_overlap_chart(conn: sqlite3.Connection) -> str:
    """Chart 1: Menu overlap trends (last 3 weeks). Returns HTML div."""
    if not HAS_PLOTLY:
        print("⚠️  Plotly not available, skipping chart generation")
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            overlap_with_prev_week,
            recipes_added,
            recipes_removed
        FROM menu_stability_metrics
        WHERE overlap_with_prev_week IS NOT NULL
        ORDER BY week_start_date DESC
        LIMIT 3
    """)
    
    rows = cursor.fetchall()
    if not rows:
        print("  ⚠️  No menu stability data, skipping chart")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'overlap', 'added', 'removed'])
    df = df.sort_values('week_start_date')
    
    print(f"  → Generated menu overlap chart with {len(df)} weeks of data")
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Menu Overlap Trend (Last 3 Weeks)', 'Recipes Added vs Removed'),
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]],
        vertical_spacing=0.15
    )
    
    # Top chart: Overlap percentage
    fig.add_trace(
        go.Scatter(
            x=df['week_start_date'],
            y=df['overlap'],
            mode='lines+markers+text',
            name='Overlap %',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=12),
            fill='tozeroy',
            fillcolor='rgba(46, 134, 171, 0.2)',
            text=[f'{y:.1f}%' for y in df['overlap']],
            textposition='top center',
            hovertemplate='<b>%{x}</b><br>Overlap: %{y:.1f}%<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Bottom chart: Added vs Removed
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['added'],
            name='Added',
            marker=dict(color='#A23B72'),
            text=df['added'],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Added: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['removed'],
            name='Removed',
            marker=dict(color='#F18F01'),
            text=df['removed'],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Removed: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_yaxes(title_text="Overlap Percentage (%)", row=1, col=1, range=[0, 105])
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_xaxes(title_text="Week", row=2, col=1)
    
    fig.update_layout(
        height=700,
        showlegend=True,
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    print(f"  ✓ Menu overlap chart generated")
    return fig.to_html(include_plotlyjs='cdn', div_id="menu_overlap_chart")


def generate_recipe_survival_chart(conn: sqlite3.Connection) -> str:
    """Chart 2: Recipe survival distribution. Returns HTML div."""
    if not HAS_PLOTLY:
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            total_weeks_active,
            is_currently_active
        FROM recipe_survival_metrics
        WHERE total_weeks_active > 0
    """)
    
    rows = cursor.fetchall()
    if not rows:
        print("  ⚠️  No recipe survival data, skipping chart")
        return ""
    
    df = pd.DataFrame(rows, columns=['total_weeks_active', 'is_currently_active'])
    
    active = df[df['is_currently_active'] == 1]['total_weeks_active']
    inactive = df[df['is_currently_active'] == 0]['total_weeks_active']
    
    print(f"  → Generated recipe survival chart with {len(active)} active and {len(inactive)} inactive recipes")
    
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=active,
        name='Currently Active',
        marker=dict(color='#2E86AB'),
        nbinsx=20,
        hovertemplate='<b>Active Recipes</b><br>Weeks Active: %{x}<br>Count: %{y}<extra></extra>'
    ))
    
    fig.add_trace(go.Histogram(
        x=inactive,
        name='Churned',
        marker=dict(color='#A23B72'),
        nbinsx=20,
        hovertemplate='<b>Churned Recipes</b><br>Weeks Active: %{x}<br>Count: %{y}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Recipe Survival Distribution',
        xaxis_title='Weeks Active',
        yaxis_title='Number of Recipes',
        barmode='overlay',
        height=500,
        showlegend=True,
        plot_bgcolor='white',
        paper_bgcolor='white',
        hovermode='x unified'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    print(f"  ✓ Recipe survival chart generated")
    return fig.to_html(include_plotlyjs='cdn', div_id="recipe_survival_chart")


def generate_ingredient_trends_chart(conn: sqlite3.Connection) -> str:
    """Chart 3: Top ingredients over last 3 weeks. Returns HTML div."""
    if not HAS_PLOTLY:
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            ingredient_name,
            recipe_count
        FROM ingredient_trends
        WHERE recipe_count > 0 AND popularity_rank <= 8
        ORDER BY week_start_date DESC, recipe_count DESC
        LIMIT 24
    """)
    
    df = pd.DataFrame(cursor.fetchall(), columns=['week_start_date', 'ingredient_name', 'recipe_count'])
    
    if df.empty:
        print("  ⚠️  No ingredient trends data, skipping chart")
        return ""
    
    df = df.sort_values('week_start_date')
    
    print(f"  → Generated ingredient trends chart")
    
    fig = px.line(
        df,
        x='week_start_date',
        y='recipe_count',
        color='ingredient_name',
        markers=True,
        title='Top 8 Ingredients Over Last 3 Weeks',
        labels={'week_start_date': 'Week Start Date', 'recipe_count': 'Recipe Count', 'ingredient_name': 'Ingredient'},
        height=500
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        hovermode='x unified'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    for trace in fig.data:
        trace.update(
            mode='lines+markers',
            line=dict(width=3),
            marker=dict(size=10)
        )
    
    print(f"  ✓ Ingredient trends chart generated")
    return fig.to_html(include_plotlyjs='cdn', div_id="ingredient_trends_chart")


def generate_allergen_density_chart(conn: sqlite3.Connection) -> str:
    """Chart 4: Allergen density heatmap (last 3 weeks). Returns HTML div."""
    if not HAS_PLOTLY:
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            allergen_name,
            percentage_of_menu
        FROM allergen_density
        WHERE allergen_name IS NOT NULL
        ORDER BY week_start_date DESC
        LIMIT 90
    """)
    
    df = pd.DataFrame(cursor.fetchall(), columns=['week_start_date', 'allergen_name', 'percentage_of_menu'])
    
    if df.empty:
        print("  ⚠️  No allergen density data, skipping chart")
        return ""
    
    df = df.sort_values('week_start_date')
    pivot_df = df.pivot_table(index='allergen_name', columns='week_start_date', values='percentage_of_menu', fill_value=0)
    
    print(f"  → Generated allergen density heatmap")
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='RdYlGn_r',
        colorbar=dict(title='% of Menu'),
        hovertemplate='<b>%{y}</b><br>Week: %{x}<br>Percentage: %{z:.1f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title='Allergen Density Heatmap - Last 3 Weeks (%)',
        xaxis_title='Week Start Date',
        yaxis_title='Allergen',
        height=500,
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    print(f"  ✓ Allergen density chart generated")
    return fig.to_html(include_plotlyjs='cdn', div_id="allergen_density_chart")

def save_report_to_file(html_content: str, pull_date: str) -> str:
    """Save HTML report with embedded charts."""
    filename = f"{pull_date}-report.html"
    
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        filepath = REPORTS_DIR / filename
        
        # Wrap in HTML template with CSS styling
        full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HelloFresh Report - {pull_date}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            padding: 40px;
        }}
        h1 {{
            color: #2E86AB;
            margin-bottom: 10px;
            font-size: 2.5em;
            border-bottom: 3px solid #2E86AB;
            padding-bottom: 15px;
        }}
        h2 {{
            color: #2E86AB;
            margin-top: 40px;
            margin-bottom: 15px;
            font-size: 1.8em;
            padding-bottom: 10px;
            border-bottom: 1px solid #ddd;
        }}
        h3 {{
            color: #333;
            margin-top: 25px;
            margin-bottom: 10px;
            font-size: 1.3em;
        }}
        .metadata {{
            background: #f0f4f8;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
            border-left: 4px solid #2E86AB;
        }}
        .metadata p {{
            margin: 5px 0;
            color: #555;
        }}
        .metadata strong {{
            color: #2E86AB;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
            background: white;
            border-radius: 5px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px 15px;
            text-align: left;
        }}
        th {{
            background-color: #2E86AB;
            color: white;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f0f0f0;
        }}
        .chart-container {{
            margin: 30px 0;
            padding: 20px;
            background: #f9f9f9;
            border-radius: 5px;
            border-left: 4px solid #A23B72;
        }}
        .chart-container h3 {{
            color: #A23B72;
            margin-top: 0;
        }}
        .executive-summary {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .executive-summary ul {{
            list-style: none;
            padding-left: 0;
        }}
        .executive-summary li {{
            padding: 8px 0;
            font-size: 1.1em;
        }}
        .executive-summary li:before {{
            content: "✓ ";
            margin-right: 10px;
            font-weight: bold;
        }}
        .data-quality {{
            background: #e8f5e9;
            padding: 15px;
            border-radius: 5px;
            margin-top: 30px;
            border-left: 4px solid #4caf50;
        }}
        .data-quality ul {{
            list-style: none;
            padding-left: 0;
        }}
        .data-quality li {{
            padding: 5px 0;
            color: #2e7d32;
        }}
        footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
            font-size: 0.9em;
        }}
        .plotly-chart {{
            width: 100%;
            height: auto;
        }}
    </style>
</head>
<body>
    <div class="container">
        {html_content}
        <footer>
            <p>This report was generated automatically by the HelloFresh Data Platform.</p>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </div>
</body>
</html>"""
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(full_html)
        print(f"✓ Report saved to {filepath}")
        return str(filepath)
    except Exception as e:
        print(f"⚠️  Error saving report: {e}")
        return None


# ======================
# HTML Report Generation
# ======================

def generate_html_report(conn: sqlite3.Connection, week_date: str) -> str:
    """Generate HTML report with embedded charts and data tables."""
    
    print("Gathering report data...")
    summary = get_week_summary(conn, week_date)
    top_recipes = get_top_recipes(conn, limit=10)
    stability = get_menu_stability(conn, limit=5)
    ingredient_trends = get_ingredient_trends(conn, limit=10)
    popular_ings = get_popular_ingredients(conn, limit=10)
    cuisines = get_cuisine_distribution(conn)
    lifecycle = get_recipe_lifecycle(conn)
    
    print("Generating charts...")
    menu_overlap_html = generate_menu_overlap_chart(conn)
    recipe_survival_html = generate_recipe_survival_chart(conn)
    ingredient_trends_html = generate_ingredient_trends_chart(conn)
    allergen_density_html = generate_allergen_density_chart(conn)
    
    # Build HTML content
    html_parts = []
    
    # Header
    html_parts.append(f"<h1>HelloFresh Data Analysis Report</h1>")
    html_parts.append(f"""<div class="metadata">
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <p><strong>Analysis Period:</strong> Week of {week_date}</p>
        <p><strong>Data Source:</strong> SQLite Gold Layer</p>
    </div>""")
    
    # Executive Summary
    html_parts.append("<h2>Executive Summary</h2>")
    if summary:
        html_parts.append(f"""<div class="executive-summary">
            <ul>
                <li><strong>Total Recipes This Week:</strong> {summary.get('total_recipes', 0)}</li>
                <li><strong>New Recipes Introduced:</strong> {summary.get('new_recipes', 0)}</li>
                <li><strong>Returning Recipes:</strong> {summary.get('returning_recipes', 0)}</li>
                <li><strong>Average Difficulty:</strong> {summary.get('avg_difficulty', 'N/A')}</li>
                <li><strong>Average Prep Time:</strong> {summary.get('avg_prep_time', 'N/A')} minutes</li>
            </ul>
        </div>""")
    
    # Weekly Metrics Table
    html_parts.append("<h3>Weekly Metrics Table</h3>")
    html_parts.append("<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>")
    if summary:
        html_parts.append(f"<tr><td>Total Recipes</td><td>{summary.get('total_recipes', 0)}</td></tr>")
        html_parts.append(f"<tr><td>Unique Recipes</td><td>{summary.get('unique_recipes', 0)}</td></tr>")
        html_parts.append(f"<tr><td>New Recipes</td><td>{summary.get('new_recipes', 0)}</td></tr>")
        html_parts.append(f"<tr><td>Returning Recipes</td><td>{summary.get('returning_recipes', 0)}</td></tr>")
        html_parts.append(f"<tr><td>Avg Difficulty</td><td>{summary.get('avg_difficulty', 'N/A')}</td></tr>")
        html_parts.append(f"<tr><td>Avg Prep Time (min)</td><td>{summary.get('avg_prep_time', 'N/A')}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Menu Evolution
    html_parts.append("<h2>1. Menu Evolution</h2>")
    if menu_overlap_html:
        html_parts.append(f'<div class="chart-container">{menu_overlap_html}</div>')
    
    # Menu Stability Table
    html_parts.append("<h3>Recent Menu Stability</h3>")
    html_parts.append("<table><thead><tr><th>Week</th><th>Overlap %</th><th>Added</th><th>Removed</th></tr></thead><tbody>")
    if stability:
        for week in stability[:5]:
            overlap = week.get('overlap_with_prev_week') or 'N/A'
            html_parts.append(f"<tr><td>{week.get('week_start_date')}</td><td>{overlap}</td><td>{week.get('recipes_added', 0)}</td><td>{week.get('recipes_removed', 0)}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Recipe Lifecycle
    html_parts.append("<h2>2. Recipe Lifecycle Analysis</h2>")
    if recipe_survival_html:
        html_parts.append(f'<div class="chart-container">{recipe_survival_html}</div>')
    
    html_parts.append("<h3>Top 10 Active Recipes (Current Week)</h3>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Recipe Name</th><th>Difficulty</th><th>Prep Time</th><th>Appearances</th></tr></thead><tbody>")
    if top_recipes:
        for i, recipe in enumerate(top_recipes[:10], 1):
            name = recipe.get('name', 'Unknown')
            difficulty = recipe.get('difficulty', 'N/A')
            prep = recipe.get('prep_time', 'N/A')
            apps = recipe.get('menu_appearances', 0)
            html_parts.append(f"<tr><td>{i}</td><td>{name}</td><td>{difficulty}</td><td>{prep}</td><td>{apps}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Ingredient Trends
    html_parts.append("<h2>3. Ingredient Trends</h2>")
    if ingredient_trends_html:
        html_parts.append(f'<div class="chart-container">{ingredient_trends_html}</div>')
    
    html_parts.append("<h3>Top Trending Ingredients (Latest Week)</h3>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Ingredient</th><th>Recipes</th><th>Popularity</th></tr></thead><tbody>")
    if ingredient_trends:
        for i, ing in enumerate(ingredient_trends[:10], 1):
            name = ing.get('ingredient_name', 'Unknown')
            count = ing.get('recipe_count', 0)
            rank = ing.get('popularity_rank', 'N/A')
            html_parts.append(f"<tr><td>{i}</td><td>{name}</td><td>{count}</td><td>#{rank}</td></tr>")
    html_parts.append("</tbody></table>")
    
    html_parts.append("<h3>Most Used Ingredients (All Time)</h3>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Ingredient</th><th>In Recipes</th><th>Menu Appearances</th></tr></thead><tbody>")
    if popular_ings:
        for i, ing in enumerate(popular_ings[:10], 1):
            name = ing.get('ingredient_name', 'Unknown')
            recipes = ing.get('recipe_count', 0)
            menus = ing.get('menu_count', 0)
            html_parts.append(f"<tr><td>{i}</td><td>{name}</td><td>{recipes}</td><td>{menus}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Allergen Analysis
    html_parts.append("<h2>4. Allergen Analysis</h2>")
    if allergen_density_html:
        html_parts.append(f'<div class="chart-container">{allergen_density_html}</div>')
    
    # Cuisine Distribution
    html_parts.append("<h2>5. Cuisine Distribution</h2>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Cuisine</th><th>Recipes</th><th>Menu Appearances</th></tr></thead><tbody>")
    if cuisines:
        for i, cuisine in enumerate(cuisines[:10], 1):
            cuisine_name = cuisine.get('cuisine', 'Unknown')
            count = cuisine.get('recipe_count', 0)
            apps = cuisine.get('menu_appearances', 0)
            html_parts.append(f"<tr><td>{i}</td><td>{cuisine_name}</td><td>{count}</td><td>{apps}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Recipe Lifecycle Status
    html_parts.append("<h2>6. Recipe Lifecycle Status</h2>")
    html_parts.append("<table><thead><tr><th>Status</th><th>Count</th><th>Avg Days Active</th></tr></thead><tbody>")
    if lifecycle:
        for status, data in lifecycle.items():
            count = data.get('count', 0)
            days = data.get('avg_days', 0)
            html_parts.append(f"<tr><td>{status}</td><td>{count}</td><td>{days}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Data Quality Notes
    html_parts.append("""<div class="data-quality">
        <h3>Data Quality Notes</h3>
        <ul>
            <li><strong>Report Generated:</strong> {}</li>
            <li><strong>Week Start Date:</strong> {}</li>
            <li><strong>Data Source:</strong> SQLite Database (hfresh/hfresh.db)</li>
        </ul>
    </div>""".format(datetime.now().isoformat(), week_date))
    
    return "\n".join(html_parts)


def commit_report_to_git(week_date: str) -> bool:
    """Commit report to Git repository."""
    try:
        os.chdir(PROJECT_ROOT)
        
        # Git operations
        report_file = f"docs/weekly_reports/{week_date}-report.html"
        subprocess.run(["git", "add", report_file], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "hfresh-pipeline"], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "hfresh@github.local"], check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"Weekly report {week_date}"],
            check=True,
            capture_output=True
        )
        subprocess.run(["git", "push"], check=True, capture_output=True)
        
        print(f"✓ Report committed to Git")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Git commit failed: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Error during Git operations: {e}")
        return False


# ======================
# Main Execution
# ======================

def main():
    """Generate weekly report with embedded charts and Git commit."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Weekly Report Generation                                ║
    ║  Gold Layer → HTML with Embedded Plotly Charts           ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = get_db_connection()
    
    # Get latest week
    week_date = get_latest_week(conn)
    print(f"\nGenerating report for week: {week_date}\n")
    
    # Check data availability
    print("Checking data availability...")
    data_check = check_data_available(conn)
    print(f"  Data check: {data_check}\n")
    
    if not HAS_PLOTLY:
        print("⚠️  Plotly not available, skipping report generation")
        print("   Install with: pip install plotly pandas")
        conn.close()
        return
    
    # Generate HTML report with embedded charts
    print("Generating HTML report with embedded charts...")
    html_content = generate_html_report(conn, week_date)
    report_path = save_report_to_file(html_content, week_date)
    
    # Commit to Git
    if report_path:
        print("\nCommitting to Git...")
        commit_report_to_git(week_date)
    
    print(f"\n{'='*60}")
    print("✓ Report generation complete!")
    print(f"{'='*60}\n")
    
    conn.close()


if __name__ == "__main__":
    main()