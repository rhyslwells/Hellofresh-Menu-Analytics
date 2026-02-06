"""
SQLite WEEKLY REPORT GENERATOR

Purpose
-------
Generates clean, focused weekly HTML reports with embedded interactive Plotly charts.
Pulls insights from Gold layer and creates visualizations focused on menu evolution,
recipe performance, ingredient trends, and allergen patterns.

Output
------
- Reports (HTML): stored in docs/weekly_reports/
  - YYYY-MM-DD-report.html (with embedded interactive charts)

Charts & Tables Included
------------------------
- Menu Stability Table (week-over-week changes)
- Top 5 Most Difficult Recipes Table
- Top 5 Trending Ingredients Chart + Most Used Ingredients Table
- Allergen Density Heatmap

Usage
-----
From command line:
python scripts/4_weekly_report.py

With specific date:
python scripts/4_weekly_report.py --date 2026-02-07

With GitHub Actions (after 3_gold_analytics.py):
python scripts/4_weekly_report.py

Requirements
------------
- plotly
- pandas
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import os
import json
import argparse

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


def get_top_recipes(conn: sqlite3.Connection, limit: int = 5) -> list:
    """Get top recipes by difficulty (hardest first)."""
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
            ORDER BY r.difficulty DESC
            LIMIT ?
        """, (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️  Error getting top recipes: {e}")
        return []


def get_menu_stability(conn: sqlite3.Connection, week_date: str, limit: int = 5) -> list:
    """Get menu stability metrics for weeks up to and including week_date."""
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
        WHERE week_start_date <= ?
        ORDER BY week_start_date DESC
        LIMIT ?
    """, (week_date, limit))
    
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_ingredient_trends(conn: sqlite3.Connection, week_date: str, limit: int = 5) -> list:
    """Get trending ingredients for the specified week."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            ingredient_name,
            recipe_count,
            popularity_rank
        FROM ingredient_trends
        WHERE week_start_date = ? AND popularity_rank <= ?
        ORDER BY popularity_rank ASC
        LIMIT ?
    """, (week_date, limit, limit))
    
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_popular_ingredients(conn: sqlite3.Connection, limit: int = 5) -> list:
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

def generate_ingredient_trends_chart(conn: sqlite3.Connection, week_date: str) -> str:
    """Chart: Top 5 trending ingredients for the specified week. Returns HTML div."""
    if not HAS_PLOTLY:
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            ingredient_name,
            recipe_count
        FROM ingredient_trends
        WHERE week_start_date = ? AND popularity_rank <= 5
        ORDER BY popularity_rank ASC
    """, (week_date,))
    
    rows = cursor.fetchall()
    if not rows:
        print("  ⚠️  No ingredient trends data, skipping chart")
        return ""
    
    df = pd.DataFrame(rows, columns=['ingredient_name', 'recipe_count'])
    
    print(f"  → Generated ingredient trends chart with {len(df)} ingredients")
    
    fig = px.bar(
        df,
        x='recipe_count',
        y='ingredient_name',
        orientation='h',
        title='Top 5 Trending Ingredients This Week',
        labels={'recipe_count': 'Recipe Count', 'ingredient_name': 'Ingredient'},
        height=400
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        hovermode='y',
        xaxis_title='Number of Recipes',
        yaxis_title='Ingredient'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    print(f"  ✓ Ingredient trends chart generated")
    return fig.to_html(include_plotlyjs='cdn', div_id="ingredient_trends_chart")


def generate_allergen_density_chart(conn: sqlite3.Connection, week_date: str) -> str:
    """Chart: Allergen density heatmap (last 3-4 weeks). Returns HTML div."""
    if not HAS_PLOTLY:
        return ""
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            week_start_date,
            allergen_name,
            percentage_of_menu
        FROM allergen_density
        WHERE allergen_name IS NOT NULL AND week_start_date <= ?
        ORDER BY week_start_date DESC
        LIMIT 20
    """, (week_date,))
    
    rows = cursor.fetchall()
    if not rows:
        print("  ⚠️  No allergen density data, skipping chart")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'allergen_name', 'percentage_of_menu'])
    df = df.sort_values('week_start_date')
    
    pivot_df = df.pivot_table(index='allergen_name', columns='week_start_date', values='percentage_of_menu', fill_value=0)
    
    print(f"  → Generated allergen density heatmap with {len(pivot_df.columns)} weeks")
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='RdYlGn_r',
        colorbar=dict(title='% of Menu'),
        hovertemplate='<b>%{y}</b><br>Week: %{x}<br>Percentage: %{z:.1f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title='Allergen Density Heatmap',
        xaxis_title='Week Start Date',
        yaxis_title='Allergen',
        height=400,
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
    """Generate clean, focused HTML report with charts and tables."""
    
    print("Gathering report data...")
    summary = get_week_summary(conn, week_date)
    top_recipes = get_top_recipes(conn, limit=5)
    stability = get_menu_stability(conn, week_date, limit=5)
    ingredient_trends_data = get_ingredient_trends(conn, week_date, limit=5)
    popular_ings = get_popular_ingredients(conn, limit=5)
    
    print("Generating charts...")
    ingredient_trends_html = generate_ingredient_trends_chart(conn, week_date)
    allergen_density_html = generate_allergen_density_chart(conn, week_date)
    
    # Build HTML content
    html_parts = []
    
    # Header
    html_parts.append(f"<h1>HelloFresh Weekly Report</h1>")
    html_parts.append(f"""<div class="metadata">
        <p><strong>Week of:</strong> {week_date}</p>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <p><strong>Total Recipes:</strong> {summary.get('total_recipes', 'N/A')} | <strong>New:</strong> {summary.get('new_recipes', 'N/A')} | <strong>Returning:</strong> {summary.get('returning_recipes', 'N/A')}</p>
    </div>""")
    
    # Menu Evolution
    html_parts.append("<h2>1. Menu Evolution</h2>")
    html_parts.append("<h3>Recent Menu Stability</h3>")
    html_parts.append("<table><thead><tr><th>Week</th><th>Overlap %</th><th>Added</th><th>Removed</th></tr></thead><tbody>")
    if stability:
        for week in stability:
            overlap = week.get('overlap_with_prev_week') or 'N/A'
            html_parts.append(f"<tr><td>{week.get('week_start_date')}</td><td>{overlap}</td><td>{week.get('recipes_added', 0)}</td><td>{week.get('recipes_removed', 0)}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Recipe Performance
    html_parts.append("<h2>2. Recipe Performance</h2>")
    html_parts.append("<h3>Top 5 Most Difficult Active Recipes</h3>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Recipe Name</th><th>Difficulty</th><th>Prep Time (min)</th></tr></thead><tbody>")
    if top_recipes:
        for i, recipe in enumerate(top_recipes[:5], 1):
            name = recipe.get('name', 'Unknown')
            difficulty = recipe.get('difficulty', 'N/A')
            prep = recipe.get('prep_time', 'N/A')
            html_parts.append(f"<tr><td>{i}</td><td>{name}</td><td>{difficulty}</td><td>{prep}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Ingredient Insights
    html_parts.append("<h2>3. Ingredient Insights</h2>")
    if ingredient_trends_html:
        html_parts.append(f'<div class="chart-container">{ingredient_trends_html}</div>')
    
    html_parts.append("<h3>Top 5 Most Used Ingredients</h3>")
    html_parts.append("<table><thead><tr><th>Rank</th><th>Ingredient</th><th>In Recipes</th><th>Menu Appearances</th></tr></thead><tbody>")
    if popular_ings:
        for i, ing in enumerate(popular_ings[:5], 1):
            name = ing.get('ingredient_name', 'Unknown')
            recipes = ing.get('recipe_count', 0)
            menus = ing.get('menu_count', 0)
            html_parts.append(f"<tr><td>{i}</td><td>{name}</td><td>{recipes}</td><td>{menus}</td></tr>")
    html_parts.append("</tbody></table>")
    
    # Allergen Patterns
    html_parts.append("<h2>4. Allergen Patterns</h2>")
    if allergen_density_html:
        html_parts.append(f'<div class="chart-container">{allergen_density_html}</div>')
    
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


def update_reports_json(week_date: str) -> bool:
    """Update reports.json metadata file with new report.
    
    Ensures reports are always ordered by date in descending order (latest first).
    """
    try:
        reports_json_path = REPORTS_DIR / "reports.json"
        
        # Format the date for display (e.g., "January 1, 2026")
        date_obj = datetime.strptime(week_date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%B %d, %Y")
        
        # Create new report entry
        new_report = {
            "name": f"{week_date}-report.html",
            "date": formatted_date
        }
        
        # Load existing reports or create new list
        reports = []
        if reports_json_path.exists():
            try:
                with open(reports_json_path, 'r', encoding='utf-8') as f:
                    reports = json.load(f)
            except Exception as e:
                print(f"  ⚠️  Could not read existing reports.json: {e}")
                reports = []
        
        # Check if this report already exists
        existing_names = [r.get('name') for r in reports]
        if new_report['name'] not in existing_names:
            reports.append(new_report)
        
        # Sort reports by date in descending order (latest first)
        # Extract date from filename (YYYY-MM-DD format)
        reports.sort(
            key=lambda r: r.get('name', '').split('-report')[0],
            reverse=True
        )
        
        # Write back to file with pretty formatting
        with open(reports_json_path, 'w', encoding='utf-8') as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Updated reports.json with {new_report['name']} (descending date order)")
        return True
    except Exception as e:
        print(f"⚠️  Error updating reports.json: {e}")
        return False


# ======================
# Main Execution
# ======================

def main():
    """Generate weekly report with embedded charts and Git commit."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Generate weekly report for a specific date or latest week'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Report date in YYYY-MM-DD format (optional, defaults to latest week in database)'
    )
    args = parser.parse_args()
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Weekly Report Generation                                ║
    ║  Gold Layer → HTML with Embedded Plotly Charts           ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = get_db_connection()
    
    # Get week date - use provided date if given, otherwise get latest
    if args.date:
        # Calculate the Monday of the week containing the provided date
        week_date = get_week_start_date(args.date)
        print(f"\nGenerating report for week of: {week_date} (provided date: {args.date})\n")
    else:
        week_date = get_latest_week(conn)
        print(f"\nGenerating report for latest week: {week_date}\n")
    
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
    
    # Update reports.json metadata
    if report_path:
        print("\nUpdating reports metadata...")
        update_reports_json(week_date)
    
    # Commit to Git
    if report_path:
        print("Committing to Git...")
        commit_report_to_git(week_date)
    
    print(f"\n{'='*60}")
    print("✓ Report generation complete!")
    print(f"{'='*60}\n")
    
    conn.close()


if __name__ == "__main__":
    main()