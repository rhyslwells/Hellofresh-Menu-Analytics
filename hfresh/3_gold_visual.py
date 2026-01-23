"""
HFRESH DATA VISUALIZATIONS

Purpose
-------
Creates visualizations of menu trends, recipe patterns, and ingredient analysis.

Generates charts:
- Menu stability over time
- Recipe lifespan distribution
- Ingredient popularity
- Cuisine distribution
- Recipe complexity trends

Requirements
------------
pip install matplotlib pandas

Usage
-----
python 5_visualize.py
"""

import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime


# ======================
# Configuration
# ======================

SILVER_DB = Path("hfresh/silver_data.db")
OUTPUT_DIR = Path("hfresh/charts")

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ======================
# Data Loading Functions
# ======================

def load_query(db_path: Path, query: str) -> pd.DataFrame:
    """Execute SQL query and return as DataFrame."""
    conn = sqlite3.connect(db_path)
    df = pd.DataFrame(conn.execute(query).fetchall(),
                     columns=[desc[0] for desc in conn.execute(query).description])
    conn.close()
    return df


# ======================
# Visualization Functions
# ======================

def plot_menu_composition_over_time():
    """Plot how menu composition changes over time."""
    query = """
        SELECT 
            m.start_date,
            COUNT(DISTINCT mr.recipe_id) as recipe_count,
            COUNT(DISTINCT i.ingredient_id) as ingredient_count,
            AVG(r.difficulty) as avg_difficulty
        FROM menus m
        JOIN menu_recipes mr ON m.menu_id = mr.menu_id
        JOIN recipes r ON mr.recipe_id = r.recipe_id
        LEFT JOIN recipe_ingredients ri ON r.recipe_id = ri.recipe_id
        LEFT JOIN ingredients i ON ri.ingredient_id = i.ingredient_id
        GROUP BY m.start_date
        ORDER BY m.start_date
    """
    
    df = load_query(SILVER_DB, query)
    df['start_date'] = pd.to_datetime(df['start_date'])
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    
    # Recipe count over time
    axes[0].plot(df['start_date'], df['recipe_count'], marker='o', linewidth=2, color='#FF6B35')
    axes[0].set_title('Recipes per Menu Over Time', fontsize=14, fontweight='bold')
    axes[0].set_ylabel('Number of Recipes')
    axes[0].grid(True, alpha=0.3)
    
    # Average difficulty over time
    axes[1].plot(df['start_date'], df['avg_difficulty'], marker='s', linewidth=2, color='#004E89')
    axes[1].set_title('Average Recipe Difficulty Over Time', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Week Start Date')
    axes[1].set_ylabel('Average Difficulty')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'menu_composition_over_time.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: menu_composition_over_time.png")
    plt.close()


def plot_recipe_lifespan_distribution():
    """Plot distribution of how long recipes stay in rotation."""
    query = """
        SELECT 
            r.name,
            CAST(JULIANDAY(r.last_seen_date) - JULIANDAY(r.first_seen_date) AS INTEGER) as days_in_catalog,
            COUNT(DISTINCT mr.menu_id) as menu_appearances
        FROM recipes r
        LEFT JOIN menu_recipes mr ON r.recipe_id = mr.recipe_id
        GROUP BY r.recipe_id
    """
    
    df = load_query(SILVER_DB, query)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram of days in catalog
    axes[0].hist(df['days_in_catalog'], bins=20, color='#F77F00', edgecolor='black', alpha=0.7)
    axes[0].set_title('Recipe Lifespan Distribution', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Days in Catalog')
    axes[0].set_ylabel('Number of Recipes')
    axes[0].grid(True, alpha=0.3, axis='y')
    
    # Histogram of menu appearances
    axes[1].hist(df['menu_appearances'], bins=15, color='#06A77D', edgecolor='black', alpha=0.7)
    axes[1].set_title('Menu Appearance Frequency', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Number of Menu Appearances')
    axes[1].set_ylabel('Number of Recipes')
    axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'recipe_lifespan_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: recipe_lifespan_distribution.png")
    plt.close()


def plot_top_ingredients():
    """Plot most popular ingredients."""
    query = """
        SELECT 
            i.name,
            COUNT(DISTINCT ri.recipe_id) as recipe_count
        FROM ingredients i
        JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id
        GROUP BY i.ingredient_id
        ORDER BY recipe_count DESC
        LIMIT 20
    """
    
    df = load_query(SILVER_DB, query)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    bars = ax.barh(df['name'], df['recipe_count'], color='#D62828')
    ax.set_title('Top 20 Most Used Ingredients', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Number of Recipes', fontsize=12)
    ax.set_ylabel('Ingredient', fontsize=12)
    ax.invert_yaxis()
    ax.grid(True, alpha=0.3, axis='x')
    
    # Add value labels
    for i, (bar, value) in enumerate(zip(bars, df['recipe_count'])):
        ax.text(value + 1, i, str(value), va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'top_ingredients.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: top_ingredients.png")
    plt.close()


def plot_cuisine_distribution():
    """Plot cuisine distribution."""
    query = """
        SELECT 
            COALESCE(r.cuisine, 'Unknown') as cuisine,
            COUNT(DISTINCT r.recipe_id) as recipe_count
        FROM recipes r
        GROUP BY cuisine
        ORDER BY recipe_count DESC
        LIMIT 15
    """
    
    df = load_query(SILVER_DB, query)
    
    fig, ax = plt.subplots(figsize=(10, 10))
    
    colors = plt.cm.Set3(range(len(df)))
    wedges, texts, autotexts = ax.pie(
        df['recipe_count'],
        labels=df['cuisine'],
        autopct='%1.1f%%',
        colors=colors,
        startangle=90
    )
    
    ax.set_title('Cuisine Distribution', fontsize=16, fontweight='bold', pad=20)
    
    # Improve text readability
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'cuisine_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: cuisine_distribution.png")
    plt.close()


def plot_recipe_complexity():
    """Plot recipe complexity by ingredient count."""
    query = """
        SELECT 
            r.recipe_id,
            r.name,
            COUNT(DISTINCT ri.ingredient_id) as ingredient_count,
            r.difficulty
        FROM recipes r
        LEFT JOIN recipe_ingredients ri ON r.recipe_id = ri.recipe_id
        GROUP BY r.recipe_id
    """
    
    df = load_query(SILVER_DB, query)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Scatter plot: ingredient count vs difficulty
    scatter = axes[0].scatter(
        df['ingredient_count'], 
        df['difficulty'],
        alpha=0.6,
        c=df['ingredient_count'],
        cmap='viridis',
        s=50
    )
    axes[0].set_title('Recipe Complexity: Ingredients vs Difficulty', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Number of Ingredients')
    axes[0].set_ylabel('Difficulty Level')
    axes[0].grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=axes[0], label='Ingredient Count')
    
    # Distribution of ingredient counts
    axes[1].hist(df['ingredient_count'], bins=20, color='#9B59B6', edgecolor='black', alpha=0.7)
    axes[1].set_title('Ingredient Count Distribution', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Number of Ingredients per Recipe')
    axes[1].set_ylabel('Number of Recipes')
    axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'recipe_complexity.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: recipe_complexity.png")
    plt.close()


def plot_allergen_coverage():
    """Plot allergen distribution across recipes."""
    query = """
        SELECT 
            a.name,
            COUNT(DISTINCT ra.recipe_id) as recipe_count
        FROM allergens a
        JOIN recipe_allergens ra ON a.allergen_id = ra.allergen_id
        GROUP BY a.allergen_id
        ORDER BY recipe_count DESC
        LIMIT 15
    """
    
    df = load_query(SILVER_DB, query)
    
    if len(df) == 0:
        print("⚠ No allergen data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    bars = ax.bar(df['name'], df['recipe_count'], color='#E74C3C', edgecolor='black', alpha=0.8)
    ax.set_title('Most Common Allergens in Recipes', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Allergen', fontsize=12)
    ax.set_ylabel('Number of Recipes', fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'allergen_coverage.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: allergen_coverage.png")
    plt.close()


# ======================
# Main Execution
# ======================

def generate_all_charts():
    """Generate all visualization charts."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Data Visualizations                          ║
    ║  Generating Charts...                                    ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    print("\nGenerating charts...\n")
    
    try:
        plot_menu_composition_over_time()
        plot_recipe_lifespan_distribution()
        plot_top_ingredients()
        plot_cuisine_distribution()
        plot_recipe_complexity()
        plot_allergen_coverage()
    except Exception as e:
        print(f"\n✗ Error generating charts: {e}")
        return
    
    print(f"\n{'='*60}")
    print(f"✓ All charts generated!")
    print(f"{'='*60}")
    print(f"\nCharts saved to: {OUTPUT_DIR.absolute()}\n")
    print("Generated files:")
    for chart_file in sorted(OUTPUT_DIR.glob("*.png")):
        print(f"  - {chart_file.name}")
    print()


if __name__ == "__main__":
    generate_all_charts()