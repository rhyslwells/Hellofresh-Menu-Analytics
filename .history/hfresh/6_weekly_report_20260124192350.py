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
from pathlib import Path

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
    dbutils
    IN_DATABRICKS = True
except NameError:
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

# Output directories
if IN_DATABRICKS:
    # Databricks workspace paths
    WORKSPACE_ROOT = "/Workspace/hfresh"
    CHARTS_DIR = f"{WORKSPACE_ROOT}/output/charts"
    REPORTS_DIR = f"{WORKSPACE_ROOT}/output/reports"
    GIT_REPO_PATH = None  # Git not available in Databricks compute
else:
    # Local filesystem paths
    WORKSPACE_ROOT = os.path.join(os.path.dirname(__file__), "..")
    CHARTS_DIR = os.path.join(WORKSPACE_ROOT, "hfresh", "output", "charts")
    REPORTS_DIR = os.path.join(WORKSPACE_ROOT, "hfresh", "output", "reports")
    GIT_REPO_PATH = os.path.join(WORKSPACE_ROOT, "hfresh")


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
    try:
        df = spark.sql(f"""
            SELECT 
                r.id as recipe_id,
                r.name,
                r.difficulty,
                r.prep_time,
                COUNT(DISTINCT mr.menu_id) as menu_appearances
            FROM {SILVER_RECIPES} r
            LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.menu_recipes mr 
                ON r.id = mr.recipe_id AND mr.is_active = TRUE
            WHERE r.is_active = TRUE
            GROUP BY r.id, r.name, r.difficulty, r.prep_time
            ORDER BY r.last_seen_date DESC, menu_appearances DESC
            LIMIT {limit}
        """)
        
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"⚠️  Error getting top recipes: {e}")
        return []


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
# Exploratory Insights (from 4_explore.py)
# ======================

def get_popular_ingredients(spark: SparkSession, limit: int = 15) -> list:
    """Top ingredients appearing most frequently."""
    try:
        df = spark.sql(f"""
            SELECT 
                i.name as ingredient_name,
                COUNT(DISTINCT ri.recipe_id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_count
            FROM {SILVER_INGREDIENTS} i
            JOIN {CATALOG}.{SILVER_SCHEMA}.recipe_ingredients ri ON i.id = ri.ingredient_id AND ri.is_active = TRUE
            JOIN {SILVER_MENU_RECIPES} mr ON ri.recipe_id = mr.recipe_id AND mr.is_active = TRUE
            GROUP BY i.id, i.name
            ORDER BY recipe_count DESC
            LIMIT {limit}
        """)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"⚠️  Error getting popular ingredients: {e}")
        return []


def get_cuisine_distribution(spark: SparkSession) -> list:
    """Distribution of recipes by cuisine."""
    try:
        df = spark.sql(f"""
            SELECT 
                COALESCE(r.cuisine, 'Unknown') as cuisine,
                COUNT(DISTINCT r.id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_appearances
            FROM {SILVER_RECIPES} r
            LEFT JOIN {SILVER_MENU_RECIPES} mr ON r.id = mr.recipe_id AND mr.is_active = TRUE
            GROUP BY r.cuisine
            ORDER BY recipe_count DESC
        """)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"⚠️  Error getting cuisine distribution: {e}")
        return []


def get_recipe_lifecycle(spark: SparkSession) -> dict:
    """Active vs inactive recipe counts."""
    try:
        df = spark.sql(f"""
            SELECT 
                is_active,
                COUNT(*) as recipe_count,
                ROUND(AVG(DATEDIFF(last_seen_date, first_seen_date)), 0) as avg_days_active
            FROM {SILVER_RECIPES}
            GROUP BY is_active
        """)
        result = {}
        for row in df.collect():
            status = 'Active' if row['is_active'] else 'Inactive'
            result[status] = {
                'count': row['recipe_count'],
                'avg_days': row['avg_days_active']
            }
        return result
    except Exception as e:
        print(f"⚠️  Error getting recipe lifecycle: {e}")
        return {}


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


def save_report_to_file(content: str, pull_date: str) -> str:
    """Save markdown report to file (Databricks or local)."""
    filename = f"weekly_report_{pull_date}.md"
    
    try:
        if IN_DATABRICKS:
            # Use dbutils for Databricks workspace
            filepath = f"{REPORTS_DIR}/{filename}"
            dbutils.fs.put(filepath, content, overwrite=True)
            print(f"✓ Report saved to {filepath}")
            return filepath
        else:
            # Use local filesystem
            reports_path = Path(REPORTS_DIR)
            reports_path.mkdir(parents=True, exist_ok=True)
            filepath = reports_path / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Report saved to {filepath}")
            return str(filepath)
    except Exception as e:
        print(f"⚠️  Error saving report: {e}")
        return None


# ======================
# Markdown Report Generation
# ======================

def generate_markdown_report(spark: SparkSession, week_date: str) -> str:
    """Generate markdown report with embedded chart references."""
    
    summary = get_week_summary(spark, week_date)
    top_recipes = get_top_recipes(spark, limit=10)
    stability = get_menu_stability(spark, limit=1)
    ingredient_trends = get_ingredient_trends(spark, limit=10)
    
    lines = []
    lines.append("# HelloFresh Data Analysis Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Analysis Period:** Week of {week_date}")
    lines.append(f"**Data Source:** Databricks Gold Layer")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Executive Summary
    lines.append("## Executive Summary")
    lines.append("")
    if summary:
        lines.append(f"- **Total Recipes This Week:** {summary.get('total_recipes', 0)}")
        lines.append(f"- **New Recipes Introduced:** {summary.get('new_recipes', 0)}")
        lines.append(f"- **Returning Recipes:** {summary.get('returning_recipes', 0)}")
        lines.append(f"- **Average Difficulty:** {summary.get('avg_difficulty', 'N/A')}")
        lines.append(f"- **Average Prep Time:** {summary.get('avg_prep_time', 'N/A')} minutes")
    lines.append("")
    
    # Menu Evolution
    lines.append("## 1. Menu Evolution")
    lines.append("")
    lines.append("![Menu Overlap Trends](../charts/menu_overlap_trends.png)")
    lines.append("")
    if stability:
        first_week = stability[0]
        lines.append("### Key Findings")
        if first_week.get('overlap_with_prev_week'):
            lines.append(f"- Week-over-week recipe overlap: {first_week.get('overlap_with_prev_week')}%")
            lines.append(f"- New recipes added: {first_week.get('recipes_added', 0)}")
            lines.append(f"- Recipes removed: {first_week.get('recipes_removed', 0)}")
    lines.append("")
    
    # Recipe Lifecycle
    lines.append("## 2. Recipe Lifecycle Analysis")
    lines.append("")
    lines.append("![Recipe Survival Distribution](../charts/recipe_survival_distribution.png)")
    lines.append("")
    lines.append("### Top Recipes (Current Week)")
    if top_recipes:
        for i, recipe in enumerate(top_recipes[:5], 1):
            name = recipe.get('name', 'Unknown')
            difficulty = recipe.get('difficulty', 'N/A')
            lines.append(f"{i}. **{name}** (Difficulty {difficulty})")
    lines.append("")
    
    # Ingredient Trends
    lines.append("## 3. Ingredient Trends")
    lines.append("")
    lines.append("![Ingredient Popularity Over Time](../charts/ingredient_trends.png)")
    lines.append("")
    lines.append("### Trending Ingredients")
    if ingredient_trends:
        for i, ing in enumerate(ingredient_trends[:5], 1):
            name = ing.get('ingredient_name', 'Unknown')
            count = ing.get('recipe_count', 0)
            lines.append(f"- **{name}**: {count} recipes")
    lines.append("")
    
    # Allergen Analysis
    lines.append("## 4. Allergen Analysis")
    lines.append("")
    lines.append("![Allergen Density Heatmap](../charts/allergen_density_heatmap.png)")
    lines.append("")
    
    # Data Quality
    lines.append("## Data Quality Notes")
    lines.append("")
    lines.append(f"- **Report Generated:** {datetime.now().isoformat()}")
    lines.append(f"- **Week Start Date:** {week_date}")
    lines.append(f"- **Data Source:** Databricks Delta Lake")
    lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("*This report was generated automatically by the HelloFresh Data Platform.*")
    lines.append("*Charts are updated weekly and stored in the output/charts/ directory.*")
    
    return "\n".join(lines)


def commit_report_to_git(week_date: str) -> bool:
    """Commit report to Git repository (local only, not in Databricks)."""
    if IN_DATABRICKS:
        print("⚠️  Skipping Git commit (not available in Databricks compute)")
        return False
    
    if not GIT_REPO_PATH:
        print("⚠️  Git repo path not configured")
        return False
    
    try:
        # Change to git repo directory
        os.chdir(GIT_REPO_PATH)
        
        # Git operations
        report_file = f"output/reports/weekly_report_{week_date}.md"
        subprocess.run(["git", "add", report_file], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "hfresh-pipeline"], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "hfresh@databricks.local"], check=True, capture_output=True)
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
    """Generate weekly report with charts and Git commit."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Weekly Report Generation                                ║
    ║  Gold Layer → Markdown + Charts                          ║
    ║  Databricks Delta Lake                                   ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    if not IN_DATABRICKS:
        print("⚠️  Not running in Databricks")
    
    spark = SparkSession.builder.appName("hfresh_weekly_report").getOrCreate()
    
    # Get latest week
    week_date = get_latest_week(spark)
    print(f"\nGenerating report for week: {week_date}\n")
    
    # Generate charts
    print("Generating charts...")
    if HAS_MATPLOTLIB:
        generate_menu_overlap_chart(spark, f"{CHARTS_DIR}/menu_overlap_trends.png")
        generate_recipe_survival_chart(spark, f"{CHARTS_DIR}/recipe_survival_distribution.png")
        generate_ingredient_trends_chart(spark, f"{CHARTS_DIR}/ingredient_trends.png")
        generate_allergen_density_chart(spark, f"{CHARTS_DIR}/allergen_density_heatmap.png")
    else:
        print("⚠️  Matplotlib not available, skipping chart generation")
    
    # Generate markdown report
    print("Generating markdown report...")
    markdown_content = generate_markdown_report(spark, week_date)
    report_path = save_report_to_file(markdown_content, week_date)
    
    # Commit to Git
    if report_path:
        print("Committing to Git...")
        commit_report_to_git(week_date)
    
    print(f"\n{'='*60}")
    print("✓ Report generation complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()