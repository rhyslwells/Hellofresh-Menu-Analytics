"""
SQLite WEEKLY REPORT GENERATOR

Purpose
-------
Generates weekly markdown reports with embedded charts after each transformation.
Pulls insights from Gold layer and creates visualizations.

Output
------
- Charts (PNG): stored in hfresh/output/charts/
  - menu_overlap_trends.png
  - recipe_survival_distribution.png
  - ingredient_trends.png
  - allergen_density_heatmap.png
- Reports (Markdown): stored in hfresh/output/reports/
  - weekly_report_YYYY-MM-DD.md
- Git integration: Commits reports to repository

Usage
-----
From command line:
python scripts/6_weekly_report.py

With GitHub Actions (after 3_gold_analytics.py):
python scripts/6_weekly_report.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import subprocess
import os

# Data visualization
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import seaborn as sns
    import pandas as pd
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


# ======================
# Configuration
# ======================

DB_PATH = Path("hfresh/hfresh.db")
PROJECT_ROOT = Path.cwd()
CHARTS_DIR = PROJECT_ROOT / "hfresh" / "output" / "charts"
REPORTS_DIR = PROJECT_ROOT / "hfresh" / "output" / "reports"


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

def generate_menu_overlap_chart(conn: sqlite3.Connection, output_path: Path) -> None:
    """Chart 1: Menu overlap trends (last 3 weeks)."""
    if not HAS_MATPLOTLIB:
        print("⚠️  Matplotlib not available, skipping chart generation")
        return
    
    cursor = conn.cursor()
    # Get last 3 weeks of overlap data
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
        return
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'overlap', 'added', 'removed'])
    df = df.sort_values('week_start_date')
    
    print(f"  → Generated chart with {len(df)} weeks of data")
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Top chart: Overlap percentage
    ax1.plot(df['week_start_date'], df['overlap'], marker='o', linewidth=3, markersize=12, color='#2E86AB', label='Overlap %')
    ax1.fill_between(range(len(df)), df['overlap'], alpha=0.3, color='#2E86AB')
    ax1.set_title('Menu Overlap Trend (Last 3 Weeks)', fontsize=14, fontweight='bold', pad=15)
    ax1.set_ylabel('Overlap Percentage (%)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.set_ylim(0, 105)
    for i, (x, y) in enumerate(zip(range(len(df)), df['overlap'])):
        ax1.text(x, y + 3, f'{y:.1f}%', ha='center', fontsize=10, fontweight='bold')
    
    # Bottom chart: Added vs Removed
    x = range(len(df))
    width = 0.35
    bars1 = ax2.bar([i - width/2 for i in x], df['added'], width, label='Added', color='#A23B72', alpha=0.8, edgecolor='black', linewidth=1.5)
    bars2 = ax2.bar([i + width/2 for i in x], df['removed'], width, label='Removed', color='#F18F01', alpha=0.8, edgecolor='black', linewidth=1.5)
    ax2.set_title('Recipes Added vs Removed', fontsize=14, fontweight='bold', pad=15)
    ax2.set_ylabel('Count', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Week', fontsize=12, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(df['week_start_date'], rotation=45, ha='right', fontsize=10)
    ax2.legend(fontsize=11, loc='upper right')
    ax2.grid(True, alpha=0.3, axis='y', linestyle='--')
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  ✓ Menu overlap chart saved")


def generate_recipe_survival_chart(conn: sqlite3.Connection, output_path: Path) -> None:
    """Chart 2: Recipe survival distribution with improved styling."""
    if not HAS_MATPLOTLIB:
        return
    
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
        return
    
    df = pd.DataFrame(rows, columns=['total_weeks_active', 'is_currently_active'])
    
    active = df[df['is_currently_active'] == 1]['total_weeks_active']
    inactive = df[df['is_currently_active'] == 0]['total_weeks_active']
    
    print(f"  → Generated chart with {len(active)} active and {len(inactive)} inactive recipes")
    
    fig, ax = plt.subplots(figsize=(13, 7))
    n, bins, patches = ax.hist([active, inactive], label=['Currently Active', 'Churned'], bins=20, alpha=0.8, edgecolor='black', linewidth=1.5, color=['#2E86AB', '#A23B72'])
    ax.set_title('Recipe Survival Distribution', fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Weeks Active', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Recipes', fontsize=12, fontweight='bold')
    ax.legend(fontsize=12, loc='upper right')
    ax.grid(True, alpha=0.3, axis='y', linestyle='--')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  ✓ Recipe survival chart saved")


def generate_ingredient_trends_chart(conn: sqlite3.Connection, output_path: Path) -> None:
    """Chart 3: Top ingredients over last 3 weeks."""
    if not HAS_MATPLOTLIB:
        return
    
    cursor = conn.cursor()
    # Get last 3 weeks of top ingredient data
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
        return
    
    df = df.sort_values('week_start_date')
    pivot_df = df.pivot_table(index='week_start_date', columns='ingredient_name', values='recipe_count', fill_value=0)
    
    fig, ax = plt.subplots(figsize=(14, 7))
    pivot_df.plot(ax=ax, marker='o', linewidth=2.5, markersize=10, color=sns.color_palette("husl", len(pivot_df.columns)))
    ax.set_title('Top 8 Ingredients Over Last 3 Weeks', fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Week Start Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Recipe Count', fontsize=12, fontweight='bold')
    ax.legend(title='Ingredient', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10, title_fontsize=11)
    ax.grid(True, alpha=0.3, linestyle='--')
    plt.xticks(rotation=45, ha='right', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  ✓ Ingredient trends chart saved")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Ingredient trends chart saved")


def generate_allergen_density_chart(conn: sqlite3.Connection, output_path: Path) -> None:
    """Chart 4: Allergen density heatmap (last 3 weeks)."""
    if not HAS_MATPLOTLIB:
        return
    
    cursor = conn.cursor()
    # Get last 3 weeks of allergen data
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
        return
    
    df = df.sort_values('week_start_date')
    pivot_df = df.pivot_table(index='allergen_name', columns='week_start_date', values='percentage_of_menu', fill_value=0)
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, annot=True, fmt='.1f', cmap='RdYlGn_r', cbar_kws={'label': '% of Menu'}, linewidths=0.5)
    plt.title('Allergen Density Heatmap - Last 3 Weeks (%)', fontsize=14, fontweight='bold')
    plt.xlabel('Week Start Date', fontsize=11)
    plt.ylabel('Allergen', fontsize=11)
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Allergen density chart saved")

def save_report_to_file(content: str, pull_date: str) -> str:
    """Save markdown report to file (local filesystem)."""
    filename = f"weekly_report_{pull_date}.md"
    
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        filepath = REPORTS_DIR / filename
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

def generate_markdown_report(conn: sqlite3.Connection, week_date: str) -> str:
    """Generate markdown report with embedded chart references and SQL tables."""
    
    summary = get_week_summary(conn, week_date)
    top_recipes = get_top_recipes(conn, limit=10)
    stability = get_menu_stability(conn, limit=5)
    ingredient_trends = get_ingredient_trends(conn, limit=10)
    popular_ings = get_popular_ingredients(conn, limit=10)
    cuisines = get_cuisine_distribution(conn)
    lifecycle = get_recipe_lifecycle(conn)
    
    lines = []
    lines.append("# HelloFresh Data Analysis Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Analysis Period:** Week of {week_date}")
    lines.append(f"**Data Source:** SQLite Gold Layer")
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
    
    # Weekly Menu Metrics Table
    lines.append("### Weekly Metrics Table")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    if summary:
        lines.append(f"| Total Recipes | {summary.get('total_recipes', 0)} |")
        lines.append(f"| Unique Recipes | {summary.get('unique_recipes', 0)} |")
        lines.append(f"| New Recipes | {summary.get('new_recipes', 0)} |")
        lines.append(f"| Returning Recipes | {summary.get('returning_recipes', 0)} |")
        lines.append(f"| Avg Difficulty | {summary.get('avg_difficulty', 'N/A')} |")
        lines.append(f"| Avg Prep Time (min) | {summary.get('avg_prep_time', 'N/A')} |")
    lines.append("")
    
    # Menu Evolution
    lines.append("## 1. Menu Evolution")
    lines.append("")
    lines.append(f"![Menu Overlap Trends](../charts/menu_overlap_trends.png)")
    lines.append("")
    
    # Menu Stability Table
    lines.append("### Recent Menu Stability")
    lines.append("")
    lines.append("| Week | Overlap % | New | Removed | Retained |")
    lines.append("|------|-----------|-----|---------|----------|")
    if stability:
        for week in stability[:5]:
            overlap = week.get('overlap_with_prev_week') or 'N/A'
            lines.append(f"| {week.get('week_start_date')} | {overlap} | {week.get('recipes_added', 0)} | {week.get('recipes_removed', 0)} | - |")
    lines.append("")
    
    # Recipe Lifecycle
    lines.append("## 2. Recipe Lifecycle Analysis")
    lines.append("")
    lines.append(f"![Recipe Survival Distribution](../charts/recipe_survival_distribution.png)")
    lines.append("")
    lines.append("### Top 10 Active Recipes (Current Week)")
    lines.append("")
    lines.append("| Rank | Recipe Name | Difficulty | Prep Time | Appearances |")
    lines.append("|------|-------------|-----------|-----------|-------------|")
    if top_recipes:
        for i, recipe in enumerate(top_recipes[:10], 1):
            name = recipe.get('name', 'Unknown')
            difficulty = recipe.get('difficulty', 'N/A')
            prep = recipe.get('prep_time', 'N/A')
            apps = recipe.get('menu_appearances', 0)
            lines.append(f"| {i} | {name} | {difficulty} | {prep} | {apps} |")
    lines.append("")
    
    # Ingredient Trends
    lines.append("## 3. Ingredient Trends")
    lines.append("")
    lines.append(f"![Ingredient Popularity Over Time](../charts/ingredient_trends.png)")
    lines.append("")
    lines.append("### Top Trending Ingredients (Latest Week)")
    lines.append("")
    lines.append("| Rank | Ingredient | Recipes | Popularity |")
    lines.append("|------|-----------|---------|------------|")
    if ingredient_trends:
        for i, ing in enumerate(ingredient_trends[:10], 1):
            name = ing.get('ingredient_name', 'Unknown')
            count = ing.get('recipe_count', 0)
            rank = ing.get('popularity_rank', 'N/A')
            lines.append(f"| {i} | {name} | {count} | #{rank} |")
    lines.append("")
    
    # Most Used Ingredients
    lines.append("### Most Used Ingredients (All Time)")
    lines.append("")
    lines.append("| Rank | Ingredient | In Recipes | Menu Appearances |")
    lines.append("|------|------------|-----------|------------------|")
    if popular_ings:
        for i, ing in enumerate(popular_ings[:10], 1):
            name = ing.get('ingredient_name', 'Unknown')
            recipes = ing.get('recipe_count', 0)
            menus = ing.get('menu_count', 0)
            lines.append(f"| {i} | {name} | {recipes} | {menus} |")
    lines.append("")
    
    # Allergen Analysis
    lines.append("## 4. Allergen Analysis")
    lines.append("")
    lines.append(f"![Allergen Density Heatmap](../charts/allergen_density_heatmap.png)")
    lines.append("")
    
    lines.append("## 5. Cuisine Distribution")
    lines.append("")
    lines.append("| Rank | Cuisine | Recipes | Menu Appearances |")
    lines.append("|------|---------|---------|------------------|")
    if cuisines:
        for i, cuisine in enumerate(cuisines[:10], 1):
            cuisine_name = cuisine.get('cuisine', 'Unknown')
            count = cuisine.get('recipe_count', 0)
            apps = cuisine.get('menu_appearances', 0)
            lines.append(f"| {i} | {cuisine_name} | {count} | {apps} |")
    lines.append("")
    
    lines.append("## 6. Recipe Lifecycle Status")
    lines.append("")
    lines.append("| Status | Count | Avg Days Active |")
    lines.append("|--------|-------|-----------------|")
    if lifecycle:
        for status, data in lifecycle.items():
            count = data.get('count', 0)
            days = data.get('avg_days', 0)
            lines.append(f"| {status} | {count} | {days} |")
    lines.append("")
    
    # Data Quality
    lines.append("## Data Quality Notes")
    lines.append("")
    lines.append(f"- **Report Generated:** {datetime.now().isoformat()}")
    lines.append(f"- **Week Start Date:** {week_date}")
    lines.append(f"- **Data Source:** SQLite Database (`hfresh/hfresh.db`)")
    lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("*This report was generated automatically by the HelloFresh Data Platform.*")
    lines.append("*Charts are stored in the output/charts/ directory.*")
    
    return "\n".join(lines)


def commit_report_to_git(week_date: str) -> bool:
    """Commit report to Git repository."""
    try:
        os.chdir(PROJECT_ROOT)
        
        # Git operations
        report_file = f"hfresh/output/reports/weekly_report_{week_date}.md"
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
    """Generate weekly report with charts and Git commit."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Weekly Report Generation                                ║
    ║  Gold Layer → Markdown + Charts                          ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = get_db_connection()
    
    # Get latest week
    week_date = get_latest_week(conn)
    print(f"\nGenerating report for week: {week_date}\n")
    
    # Generate charts
    print("Generating charts...")
    
    # Check data availability first
    data_check = check_data_available(conn)
    print(f"  Data check: {data_check}")
    
    if HAS_MATPLOTLIB:
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        generate_menu_overlap_chart(conn, CHARTS_DIR / "menu_overlap_trends.png")
        generate_recipe_survival_chart(conn, CHARTS_DIR / "recipe_survival_distribution.png")
        generate_ingredient_trends_chart(conn, CHARTS_DIR / "ingredient_trends.png")
        generate_allergen_density_chart(conn, CHARTS_DIR / "allergen_density_heatmap.png")
    else:
        print("⚠️  Matplotlib not available, skipping chart generation")
    
    # Generate markdown report
    print("Generating markdown report...")
    markdown_content = generate_markdown_report(conn, week_date)
    report_path = save_report_to_file(markdown_content, week_date)
    
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