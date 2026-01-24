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
# Chart Generation
# ======================

def generate_menu_overlap_chart(spark: SparkSession, output_path: str) -> None:
    """Chart 1: Menu overlap trends over time."""
    if not HAS_MATPLOTLIB:
        print("⚠️  Matplotlib not available, skipping chart generation")
        return
    
    df = spark.sql(f"""
        SELECT 
            week_start_date,
            overlap_with_prev_week
        FROM {GOLD_MENU_STABILITY}
        WHERE overlap_with_prev_week IS NOT NULL
        ORDER BY week_start_date
    """).toPandas()
    
    if df.empty:
        return
    
    plt.figure(figsize=(12, 6))
    plt.plot(df['week_start_date'], df['overlap_with_prev_week'], marker='o', linewidth=2, markersize=8)
    plt.title('Menu Overlap Trends (Week-over-Week)', fontsize=14, fontweight='bold')
    plt.xlabel('Week Start Date')
    plt.ylabel('Overlap Percentage (%)')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"✓ Menu overlap chart saved")


def generate_recipe_survival_chart(spark: SparkSession, output_path: str) -> None:
    """Chart 2: Recipe survival distribution."""
    if not HAS_MATPLOTLIB:
        return
    
    df = spark.sql(f"""
        SELECT 
            total_weeks_active,
            is_currently_active
        FROM {GOLD_RECIPE_SURVIVAL}
        WHERE total_weeks_active > 0
    """).toPandas()
    
    if df.empty:
        return
    
    active = df[df['is_currently_active'] == True]['total_weeks_active']
    inactive = df[df['is_currently_active'] == False]['total_weeks_active']
    
    plt.figure(figsize=(12, 6))
    plt.hist([active, inactive], label=['Currently Active', 'Churned'], bins=15, alpha=0.7)
    plt.title('Recipe Survival Distribution', fontsize=14, fontweight='bold')
    plt.xlabel('Weeks Active')
    plt.ylabel('Number of Recipes')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"✓ Recipe survival chart saved")


def generate_ingredient_trends_chart(spark: SparkSession, output_path: str) -> None:
    """Chart 3: Top ingredients over time."""
    if not HAS_MATPLOTLIB:
        return
    
    df = spark.sql(f"""
        SELECT 
            week_start_date,
            ingredient_name,
            recipe_count,
            ROW_NUMBER() OVER (PARTITION BY week_start_date ORDER BY recipe_count DESC) as rn
        FROM {GOLD_INGREDIENT_TRENDS}
        WHERE recipe_count > 0
    """).filter(F.col("rn") <= 5).toPandas()
    
    if df.empty:
        return
    
    pivot_df = df.pivot(index='week_start_date', columns='ingredient_name', values='recipe_count').fillna(0)
    
    plt.figure(figsize=(14, 6))
    pivot_df.plot(ax=plt.gca(), marker='o')
    plt.title('Top Ingredients Over Time', fontsize=14, fontweight='bold')
    plt.xlabel('Week Start Date')
    plt.ylabel('Recipe Count')
    plt.legend(title='Ingredient', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Ingredient trends chart saved")


def generate_allergen_density_chart(spark: SparkSession, output_path: str) -> None:
    """Chart 4: Allergen density heatmap."""
    if not HAS_MATPLOTLIB:
        return
    
    df = spark.sql(f"""
        SELECT 
            week_start_date,
            allergen_name,
            percentage_of_menu
        FROM {GOLD_ALLERGEN_DENSITY}
        WHERE allergen_name IS NOT NULL
        ORDER BY week_start_date DESC, percentage_of_menu DESC
        LIMIT 100
    """).toPandas()
    
    if df.empty:
        return
    
    pivot_df = df.pivot(index='allergen_name', columns='week_start_date', values='percentage_of_menu').fillna(0)
    
    plt.figure(figsize=(14, 8))
    sns.heatmap(pivot_df, annot=True, fmt='.1f', cmap='YlOrRd', cbar_kws={'label': '% of Menu'})
    plt.title('Allergen Density Heatmap (%)', fontsize=14, fontweight='bold')
    plt.xlabel('Week Start Date')
    plt.ylabel('Allergen')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"✓ Allergen density chart saved")


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