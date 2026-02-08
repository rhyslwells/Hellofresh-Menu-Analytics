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
