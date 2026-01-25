"""
SQLite GOLD LAYER ANALYTICS

Purpose
-------
Creates 5 analytical tables from Silver layer data.
Implements blueprint metrics for menu evolution, recipe lifecycle, ingredient trends,
and menu stability analysis.

Gold Layer Tables (5 total)
---------------------------
1. weekly_menu_metrics: Menu composition metrics per week
2. recipe_survival_metrics: Recipe lifespan analysis
3. ingredient_trends: Ingredient popularity over time
4. menu_stability_metrics: Week-over-week menu changes
5. allergen_density: Allergen coverage analysis

Usage
-----
From command line:
python scripts/3_gold_analytics.py

With GitHub Actions (after 2_silver.py):
python scripts/3_gold_analytics.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional


# ======================
# Configuration
# ======================

DB_PATH = Path("hfresh/hfresh.db")


# ======================
# Database Connection
# ======================

def get_db_connection() -> sqlite3.Connection:
    """Get SQLite database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ======================
# Analytics Computations
# ======================

def compute_weekly_menu_metrics(conn: sqlite3.Connection) -> None:
    """Table 1: Weekly menu composition metrics."""
    print("Computing weekly menu metrics...")
    
    try:
        cursor = conn.cursor()
        
        # Delete existing data to prevent duplicates
        cursor.execute("DELETE FROM weekly_menu_metrics")
        
        # Insert computed metrics
        cursor.execute("""
            INSERT INTO weekly_menu_metrics
            (week_start_date, locale, total_recipes, unique_recipes, new_recipes, 
             returning_recipes, avg_difficulty, avg_prep_time_minutes, _created_at)
            SELECT 
                m.start_date as week_start_date,
                'en-GB' as locale,
                COUNT(DISTINCT mr.recipe_id) as total_recipes,
                COUNT(DISTINCT mr.recipe_id) as unique_recipes,
                SUM(CASE WHEN r.first_seen_date = m.start_date THEN 1 ELSE 0 END) as new_recipes,
                SUM(CASE WHEN r.first_seen_date < m.start_date THEN 1 ELSE 0 END) as returning_recipes,
                ROUND(AVG(CAST(r.difficulty AS REAL)), 2) as avg_difficulty,
                ROUND(AVG(CAST(
                    COALESCE(
                        CAST(SUBSTR(r.prep_time, 1, INSTR(r.prep_time, ' ') - 1) AS INTEGER),
                        0
                    ) AS REAL
                )), 2) as avg_prep_time_minutes,
                ? as _created_at
            FROM menus m
            LEFT JOIN menu_recipes mr ON m.id = mr.menu_id AND mr.is_active = 1
            LEFT JOIN recipes r ON mr.recipe_id = r.id
            WHERE m.is_active = 1
            GROUP BY m.start_date
        """, (datetime.utcnow().isoformat(),))
        
        conn.commit()
        print("  ✓ Weekly menu metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing weekly menu metrics: {e}")
        conn.rollback()


def compute_recipe_survival_metrics(conn: sqlite3.Connection) -> None:
    """Table 2: Recipe lifespan and survival analysis."""
    print("Computing recipe survival metrics...")
    
    try:
        cursor = conn.cursor()
        
        # Delete existing data to prevent duplicates
        cursor.execute("DELETE FROM recipe_survival_metrics")
        
        # Calculate weeks between dates (approximate: days / 7)
        cursor.execute("""
            INSERT INTO recipe_survival_metrics
            (recipe_id, recipe_name, first_appearance_date, last_appearance_date,
             total_weeks_active, consecutive_weeks_active, weeks_since_last_seen,
             is_currently_active, _created_at)
            SELECT 
                r.id as recipe_id,
                r.name as recipe_name,
                r.first_seen_date as first_appearance_date,
                r.last_seen_date as last_appearance_date,
                MAX(1, CAST((
                    JULIANDAY(r.last_seen_date) - JULIANDAY(r.first_seen_date)
                ) / 7.0 + 1 AS INTEGER)) as total_weeks_active,
                CASE 
                    WHEN r.is_active = 1 THEN MAX(1, CAST((
                        JULIANDAY(r.last_seen_date) - JULIANDAY(r.first_seen_date)
                    ) / 7.0 + 1 AS INTEGER))
                    ELSE 0
                END as consecutive_weeks_active,
                MAX(0, CAST((
                    JULIANDAY(DATE('now')) - JULIANDAY(r.last_seen_date)
                ) / 7.0 AS INTEGER)) as weeks_since_last_seen,
                r.is_active as is_currently_active,
                ? as _created_at
            FROM recipes r
        """, (datetime.utcnow().isoformat(),))
        
        conn.commit()
        print("  ✓ Recipe survival metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing recipe survival metrics: {e}")
        conn.rollback()


def compute_ingredient_trends(conn: sqlite3.Connection) -> None:
    """Table 3: Ingredient popularity over time."""
    print("Computing ingredient trends...")
    
    try:
        cursor = conn.cursor()
        
        # Delete existing data to prevent duplicates
        cursor.execute("DELETE FROM ingredient_trends")
        
        cursor.execute("""
            INSERT INTO ingredient_trends
            (ingredient_id, ingredient_name, week_start_date, recipe_count,
             week_over_week_change, popularity_rank, _created_at)
            WITH weekly_ingredients AS (
                SELECT 
                    i.ingredient_id,
                    i.name as ingredient_name,
                    m.start_date as week_start_date,
                    COUNT(DISTINCT r.id) as recipe_count,
                    ROW_NUMBER() OVER (PARTITION BY m.start_date ORDER BY COUNT(DISTINCT r.id) DESC) as popularity_rank
                FROM ingredients i
                JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id AND ri.is_active = 1
                JOIN recipes r ON ri.recipe_id = r.id
                JOIN menu_recipes mr ON r.id = mr.recipe_id AND mr.is_active = 1
                JOIN menus m ON mr.menu_id = m.id
                WHERE i.is_active = 1
                GROUP BY i.ingredient_id, i.name, m.start_date
            )
            SELECT 
                ingredient_id,
                ingredient_name,
                week_start_date,
                recipe_count,
                0 as week_over_week_change,
                popularity_rank,
                ? as _created_at
            FROM weekly_ingredients
        """, (datetime.utcnow().isoformat(),))
        
        conn.commit()
        print("  ✓ Ingredient trends computed")
    except Exception as e:
        print(f"  ⚠️  Error computing ingredient trends: {e}")
        conn.rollback()


def compute_menu_stability_metrics(conn: sqlite3.Connection) -> None:
    """Table 4: Week-over-week menu stability (Python-assisted for set operations)."""
    print("Computing menu stability metrics...")
    
    try:
        cursor = conn.cursor()
        
        # Delete existing data to prevent duplicates
        cursor.execute("DELETE FROM menu_stability_metrics")
        
        # Fetch all weeks with their recipes
        cursor.execute("""
            SELECT DISTINCT m.start_date
            FROM menus m
            WHERE m.is_active = 1
            ORDER BY m.start_date
        """)
        
        weeks = [row[0] for row in cursor.fetchall()]
        
        # For each week, get its recipes and compare with previous week
        for i, week_date in enumerate(weeks):
            # Get recipes for this week
            cursor.execute("""
                SELECT DISTINCT mr.recipe_id
                FROM menu_recipes mr
                JOIN menus m ON mr.menu_id = m.id
                WHERE m.start_date = ? AND mr.is_active = 1 AND m.is_active = 1
            """, (week_date,))
            
            current_recipes = set(row[0] for row in cursor.fetchall())
            current_count = len(current_recipes)
            
            if i == 0:
                # First week - no previous data
                overlap = None
                new_rate = None
                churned_rate = None
                retained = 0
                added = current_count
                removed = 0
            else:
                # Get recipes for previous week
                prev_week_date = weeks[i - 1]
                cursor.execute("""
                    SELECT DISTINCT mr.recipe_id
                    FROM menu_recipes mr
                    JOIN menus m ON mr.menu_id = m.id
                    WHERE m.start_date = ? AND mr.is_active = 1 AND m.is_active = 1
                """, (prev_week_date,))
                
                prev_recipes = set(row[0] for row in cursor.fetchall())
                prev_count = len(prev_recipes)
                
                # Set operations
                intersection = current_recipes & prev_recipes
                retained = len(intersection)
                added = current_count - retained
                removed = prev_count - retained
                
                # Calculate percentages
                if prev_count > 0:
                    overlap = round(100.0 * retained / prev_count, 2)
                    new_rate = round(100.0 * added / prev_count, 2)
                    churned_rate = round(100.0 * removed / prev_count, 2)
                else:
                    overlap = None
                    new_rate = None
                    churned_rate = None
            
            # Insert record
            cursor.execute("""
                INSERT INTO menu_stability_metrics
                (week_start_date, locale, overlap_with_prev_week, new_recipe_rate,
                 churned_recipe_rate, recipes_retained, recipes_added, recipes_removed, _created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                week_date, 'en-GB', overlap, new_rate, churned_rate,
                retained, added, removed, datetime.utcnow().isoformat()
            ))
        
        conn.commit()
        print("  ✓ Menu stability metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing menu stability metrics: {e}")
        conn.rollback()


def compute_allergen_density(conn: sqlite3.Connection) -> None:
    """Table 5: Allergen coverage analysis."""
    print("Computing allergen density...")
    
    try:
        cursor = conn.cursor()
        
        # Delete existing data to prevent duplicates
        cursor.execute("DELETE FROM allergen_density")
        
        # First compute total recipes per week
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_total_recipes_per_week AS
            SELECT 
                m.start_date as week_start_date,
                COUNT(DISTINCT mr.recipe_id) as total_recipes
            FROM menus m
            LEFT JOIN menu_recipes mr ON m.id = mr.menu_id AND mr.is_active = 1
            WHERE m.is_active = 1
            GROUP BY m.start_date
        """)
        
        # Now compute allergen density
        cursor.execute("""
            INSERT INTO allergen_density
            (week_start_date, allergen_id, allergen_name, recipe_count, percentage_of_menu, _created_at)
            SELECT 
                m.start_date as week_start_date,
                a.allergen_id,
                a.name as allergen_name,
                COUNT(DISTINCT r.id) as recipe_count,
                ROUND(100.0 * COUNT(DISTINCT r.id) / 
                    NULLIF((SELECT total_recipes FROM temp_total_recipes_per_week WHERE week_start_date = m.start_date), 0), 
                    2) as percentage_of_menu,
                ? as _created_at
            FROM menus m
            LEFT JOIN menu_recipes mr ON m.id = mr.menu_id AND mr.is_active = 1
            LEFT JOIN recipes r ON mr.recipe_id = r.id
            LEFT JOIN recipe_allergens ra ON r.id = ra.recipe_id AND ra.is_active = 1
            LEFT JOIN allergens a ON ra.allergen_id = a.allergen_id
            WHERE m.is_active = 1 AND a.allergen_id IS NOT NULL
            GROUP BY m.start_date, a.allergen_id, a.name
        """, (datetime.utcnow().isoformat(),))
        
        cursor.execute("DROP TABLE IF EXISTS temp_total_recipes_per_week")
        
        conn.commit()
        print("  ✓ Allergen density computed")
    except Exception as e:
        print(f"  ⚠️  Error computing allergen density: {e}")
        conn.rollback()


def build_gold_layer() -> None:
    """Build all gold layer analytics tables."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Gold Layer Analytics                                    ║
    ║  Silver → 5 Analytical Metrics Tables                    ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = get_db_connection()
    
    # Compute all metrics
    print()
    compute_weekly_menu_metrics(conn)
    compute_recipe_survival_metrics(conn)
    compute_ingredient_trends(conn)
    compute_menu_stability_metrics(conn)
    compute_allergen_density(conn)
    
    # Summary
    print(f"\n{'='*60}")
    print("Gold layer complete!")
    print(f"{'='*60}\n")
    
    cursor = conn.cursor()
    tables = [
        ('weekly_menu_metrics', 'weekly_menu_metrics'),
        ('recipe_survival_metrics', 'recipe_survival_metrics'),
        ('ingredient_trends', 'ingredient_trends'),
        ('menu_stability_metrics', 'menu_stability_metrics'),
        ('allergen_density', 'allergen_density'),
    ]
    
    for table, label in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {label:30} {count:>8} records")
        except:
            print(f"  {label:30} {0:>8} records")
    
    print(f"\nGold layer: {DB_PATH}\n")
    conn.close()


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    build_gold_layer()
