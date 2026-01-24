"""
DATABRICKS WEEKLY REPORT GENERATOR

Purpose
-------
Generates weekly markdown reports with embedded charts after each transformation.
Pulls insights from Gold layer and creates visualizations.

Output
------
- Charts (PNG): stored in /Workspace/hfresh/output/charts/
  - menu_overlap_trends.png
  - recipe_survival_distribution.png
  - ingredient_trends.png
  - allergen_density_heatmap.png
- Reports (Markdown): stored in /Workspace/hfresh/output/reports/
  - weekly_report_YYYY-MM-DD.md
- Git integration: Commits reports to repository

Usage
-----
In Databricks notebook:
%run ./6_weekly_report

Or parameterized:
dbutils.notebook.run("6_weekly_report", 60, {"pull_date": "2026-01-24"})
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import json
import os
import subprocess

# Data visualization
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import seaborn as sns
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Databricks
try:
    from databricks.sdk.service.files import GetResponse
    spark = SparkSession.builder.appName("hfresh_weekly_report").getOrCreate()
    IN_DATABRICKS = True
except:
    IN_DATABRICKS = False


# ======================
# Configuration
# ======================

CATALOG = "hfresh_catalog"
GOLD_SCHEMA = "hfresh_gold"
SILVER_SCHEMA = "hfresh_silver"

# Gold tables
GOLD_WEEKLY_METRICS = f"{CATALOG}.{GOLD_SCHEMA}.weekly_menu_metrics"
GOLD_RECIPE_SURVIVAL = f"{CATALOG}.{GOLD_SCHEMA}.recipe_survival_metrics"
GOLD_INGREDIENT_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.ingredient_trends"
GOLD_MENU_STABILITY = f"{CATALOG}.{GOLD_SCHEMA}.menu_stability_metrics"
GOLD_ALLERGEN_DENSITY = f"{CATALOG}.{GOLD_SCHEMA}.allergen_density"

# Silver tables for report details
SILVER_RECIPES = f"{CATALOG}.{SILVER_SCHEMA}.recipes"
SILVER_MENUS = f"{CATALOG}.{SILVER_SCHEMA}.menus"

# Output directories (Databricks workspace paths)
WORKSPACE_ROOT = "/Workspace/hfresh"
CHARTS_DIR = f"{WORKSPACE_ROOT}/output/charts"
REPORTS_DIR = f"{WORKSPACE_ROOT}/output/reports"
GIT_REPO_PATH = "/Workspace/hfresh"  # Git repo root


# ======================
# Report Data Queries
# ======================

def get_latest_week(spark: SparkSession) -> str:
    """Get the most recent week from Gold metrics."""
    result = spark.sql(f"""
        SELECT MAX(week_start_date) FROM {GOLD_WEEKLY_METRICS}
    """).collect()
    
    if result and result[0][0]:
        return str(result[0][0])
    return datetime.now().strftime("%Y-%m-%d")


def get_week_summary(spark: SparkSession, week_date: str) -> dict:
    """Get summary metrics for the week."""
    df = spark.sql(f"""
        SELECT 
            week_start_date,
            total_recipes,
            unique_recipes,
            new_recipes,
            returning_recipes,
            avg_difficulty,
            avg_prep_time_minutes
        FROM {GOLD_WEEKLY_METRICS}
        WHERE week_start_date = '{week_date}'
        LIMIT 1
    """)
    
    if not df.rdd.isEmpty():
        row = df.collect()[0].asDict()
        return {
            'week_date': week_date,
            'total_recipes': row.get('total_recipes', 0),
            'unique_recipes': row.get('unique_recipes', 0),
            'new_recipes': row.get('new_recipes', 0),
            'returning_recipes': row.get('returning_recipes', 0),
            'avg_difficulty': row.get('avg_difficulty', 0),
            'avg_prep_time': row.get('avg_prep_time_minutes', 0),
        }
    return {}


def get_top_recipes(spark: SparkSession, limit: int = 10) -> list:
    """Get top recipes by recent appearance."""
    df = spark.sql(f"""
        SELECT 
            r.recipe_id,
            r.name,
            r.difficulty,
            r.prep_time,
            COUNT(DISTINCT mr.menu_id) as menu_appearances
        FROM {SILVER_RECIPES} r
        LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.menu_recipes mr 
            ON r.recipe_id = mr.recipe_id AND mr.is_active = TRUE
        WHERE r.is_active = TRUE
        GROUP BY r.recipe_id, r.name, r.difficulty, r.prep_time
        ORDER BY r.last_seen_date DESC, menu_appearances DESC
        LIMIT {limit}
    """)
    
    return [row.asDict() for row in df.collect()]


def get_menu_stability(spark: SparkSession, limit: int = 5) -> list:
    """Get recent menu stability metrics."""
    df = spark.sql(f"""
        SELECT 
            week_start_date,
            overlap_with_prev_week,
            new_recipe_rate,
            churned_recipe_rate,
            recipes_added,
            recipes_removed
        FROM {GOLD_MENU_STABILITY}
        ORDER BY week_start_date DESC
        LIMIT {limit}
    """)
    
    return [row.asDict() for row in df.collect()]


def get_ingredient_trends(spark: SparkSession, limit: int = 10) -> list:
    """Get trending ingredients."""
    df = spark.sql(f"""
        SELECT 
            ingredient_name,
            recipe_count,
            popularity_rank
        FROM {GOLD_INGREDIENT_TRENDS}
        WHERE popularity_rank <= {limit}
        ORDER BY week_start_date DESC, popularity_rank ASC
        LIMIT {limit}
    """)
    
    return [row.asDict() for row in df.collect()]


# ======================
# Report Generation
# ======================

def generate_markdown_report(conn: sqlite3.Connection) -> str:
    """Generate a markdown-formatted weekly report."""
    pull_date = get_latest_pull_date(conn)
    previous_pull = get_previous_pull_date(conn, pull_date)
    
    menu_summary = get_menu_summary(conn, pull_date)
    new_recipes = get_new_recipes(conn, pull_date)
    removed_recipes = get_removed_recipes(conn, previous_pull, pull_date) if previous_pull else []
    top_ingredients = get_top_ingredients_this_week(conn, pull_date)
    cuisines = get_cuisine_breakdown(conn, pull_date)
    
    # Build report
    lines = []
    lines.append(f"# HelloFresh Weekly Report")
    lines.append(f"")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Data Pull Date:** {pull_date}")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    
    # Menu Summary
    lines.append(f"## 📅 This Week's Menu")
    lines.append(f"")
    if menu_summary:
        lines.append(f"- **Week:** {menu_summary.get('year_week', 'N/A')}")
        lines.append(f"- **Start Date:** {menu_summary.get('start_date', 'N/A')}")
        lines.append(f"- **Menus Available:** {menu_summary.get('menu_count', 0)}")
        lines.append(f"- **Total Recipes:** {menu_summary.get('recipe_count', 0)}")
    else:
        lines.append(f"*No menu data available for this week*")
    lines.append(f"")
    
    # New Recipes
    lines.append(f"## 🆕 New Recipes This Week")
    lines.append(f"")
    if new_recipes:
        lines.append(f"**{len(new_recipes)} new recipes** added to the catalog:")
        lines.append(f"")
        for recipe in new_recipes[:15]:  # Limit to top 15
            cuisine = recipe[1] or "Unknown"
            difficulty = f"Difficulty {recipe[2]}" if recipe[2] else "Difficulty N/A"
            ingredients = f"{recipe[3]} ingredients"
            lines.append(f"- **{recipe[0]}** ({cuisine}, {difficulty}, {ingredients})")
        
        if len(new_recipes) > 15:
            lines.append(f"- *...and {len(new_recipes) - 15} more*")
    else:
        lines.append(f"*No new recipes this week*")
    lines.append(f"")
    
    # Removed Recipes
    if previous_pull:
        lines.append(f"## 👋 Recipes Removed Since Last Week")
        lines.append(f"")
        if removed_recipes:
            lines.append(f"**{len(removed_recipes)} recipes** removed from menus:")
            lines.append(f"")
            for recipe in removed_recipes[:10]:
                cuisine = recipe[1] or "Unknown"
                appearances = recipe[2]
                lines.append(f"- **{recipe[0]}** ({cuisine}, appeared in {appearances} menus)")
            
            if len(removed_recipes) > 10:
                lines.append(f"- *...and {len(removed_recipes) - 10} more*")
        else:
            lines.append(f"*No recipes removed*")
        lines.append(f"")
    
    # Top Ingredients
    lines.append(f"## 🥕 Top Ingredients This Week")
    lines.append(f"")
    if top_ingredients:
        for i, (ingredient, count) in enumerate(top_ingredients, 1):
            lines.append(f"{i}. **{ingredient}** ({count} recipes)")
    else:
        lines.append(f"*No ingredient data available*")
    lines.append(f"")
    
    # Cuisine Breakdown
    lines.append(f"## 🌍 Cuisine Distribution")
    lines.append(f"")
    if cuisines:
        for cuisine, count in cuisines:
            lines.append(f"- **{cuisine}**: {count} recipes")
    else:
        lines.append(f"*No cuisine data available*")
    lines.append(f"")
    
    lines.append(f"---")
    lines.append(f"")
    lines.append(f"*Report generated automatically by HelloFresh Data Pipeline*")
    
    return "\n".join(lines)


def generate_terminal_report(conn: sqlite3.Connection) -> None:
    """Print a formatted report to the terminal."""
    pull_date = get_latest_pull_date(conn)
    menu_summary = get_menu_summary(conn, pull_date)
    new_recipes = get_new_recipes(conn, pull_date)
    top_ingredients = get_top_ingredients_this_week(conn, pull_date)
    
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Weekly Report                                ║
    ║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^56} ║
    ╚══════════════════════════════════════════════════════════╝
    
    📅 THIS WEEK'S MENU
    {'='*60}
    Week:           {menu_summary.get('year_week', 'N/A')}
    Start Date:     {menu_summary.get('start_date', 'N/A')}
    Menus:          {menu_summary.get('menu_count', 0)}
    Total Recipes:  {menu_summary.get('recipe_count', 0)}
    
    🆕 NEW RECIPES
    {'='*60}
    {len(new_recipes)} new recipes added this week
    """)
    
    if new_recipes:
        for recipe in new_recipes[:5]:
            print(f"    • {recipe[0][:50]}")
        if len(new_recipes) > 5:
            print(f"    ...and {len(new_recipes) - 5} more")
    
    print(f"""
    🥕 TOP INGREDIENTS
    {'='*60}""")
    
    if top_ingredients:
        for i, (ingredient, count) in enumerate(top_ingredients[:5], 1):
            print(f"    {i}. {ingredient:30} ({count} recipes)")
    
    print(f"""
    {'='*60}
    Report saved to: {REPORTS_DIR.absolute()}
    """)


def save_report(content: str, pull_date: str) -> Path:
    """Save markdown report to file."""
    filename = f"weekly_report_{pull_date}.md"
    filepath = REPORTS_DIR / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return filepath


# ======================
# Main Execution
# ======================

def main():
    """Generate and save weekly report."""
    conn = sqlite3.connect(SILVER_DB)
    
    # Generate reports
    markdown_content = generate_markdown_report(conn)
    pull_date = get_latest_pull_date(conn)
    
    # Save markdown
    report_path = save_report(markdown_content, pull_date)
    
    # Print to terminal
    generate_terminal_report(conn)
    
    print(f"\n✓ Markdown report: {report_path.name}\n")
    
    conn.close()


if __name__ == "__main__":
    main()