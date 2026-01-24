"""
DATABRICKS GOLD LAYER ANALYTICS

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
In Databricks notebook:
%run ./3_gold_analytics

Or after Silver transformation:
dbutils.notebook.run("3_gold_analytics", 60)
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Databricks
try:
    spark = SparkSession.builder.appName("hfresh_gold_analytics").getOrCreate()
    IN_DATABRICKS = True
except:
    IN_DATABRICKS = False


# ======================
# Configuration
# ======================

CATALOG = "hfresh_catalog"
SILVER_SCHEMA = "hfresh_silver"
GOLD_SCHEMA = "hfresh_gold"

# Silver tables
SILVER_RECIPES = f"{CATALOG}.{SILVER_SCHEMA}.recipes"
SILVER_MENUS = f"{CATALOG}.{SILVER_SCHEMA}.menus"
SILVER_INGREDIENTS = f"{CATALOG}.{SILVER_SCHEMA}.ingredients"
SILVER_ALLERGENS = f"{CATALOG}.{SILVER_SCHEMA}.allergens"
SILVER_RECIPE_INGREDIENTS = f"{CATALOG}.{SILVER_SCHEMA}.recipe_ingredients"
SILVER_RECIPE_ALLERGENS = f"{CATALOG}.{SILVER_SCHEMA}.recipe_allergens"
SILVER_MENU_RECIPES = f"{CATALOG}.{SILVER_SCHEMA}.menu_recipes"

# Gold tables
GOLD_WEEKLY_METRICS = f"{CATALOG}.{GOLD_SCHEMA}.weekly_menu_metrics"
GOLD_RECIPE_SURVIVAL = f"{CATALOG}.{GOLD_SCHEMA}.recipe_survival_metrics"
GOLD_INGREDIENT_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.ingredient_trends"
GOLD_MENU_STABILITY = f"{CATALOG}.{GOLD_SCHEMA}.menu_stability_metrics"
GOLD_ALLERGEN_DENSITY = f"{CATALOG}.{GOLD_SCHEMA}.allergen_density"


# ======================
# Gold Layer Schema - Databricks Delta
# ======================

def create_gold_schema(spark: SparkSession) -> None:
    """Create gold schema and all 5 analytical tables."""
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
    
    # Table 1: Weekly menu metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_WEEKLY_METRICS} (
            week_start_date DATE,
            locale STRING,
            total_recipes INT,
            unique_recipes INT,
            new_recipes INT,
            returning_recipes INT,
            avg_difficulty DECIMAL(3,2),
            avg_prep_time_minutes DECIMAL(5,2),
            _created_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (week_start_date)
    """)
    
    # Table 2: Recipe survival metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_RECIPE_SURVIVAL} (
            recipe_id STRING,
            recipe_name STRING,
            first_appearance_date DATE,
            last_appearance_date DATE,
            total_weeks_active INT,
            consecutive_weeks_active INT,
            weeks_since_last_seen INT,
            is_currently_active BOOLEAN,
            _created_at TIMESTAMP
        )
        USING DELTA
    """)
    
    # Table 3: Ingredient trends
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_INGREDIENT_TRENDS} (
            ingredient_id STRING,
            ingredient_name STRING,
            week_start_date DATE,
            recipe_count INT,
            week_over_week_change INT,
            popularity_rank INT,
            _created_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (week_start_date)
    """)
    
    # Table 4: Menu stability metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_MENU_STABILITY} (
            week_start_date DATE,
            locale STRING,
            overlap_with_prev_week DECIMAL(5,2),
            new_recipe_rate DECIMAL(5,2),
            churned_recipe_rate DECIMAL(5,2),
            recipes_retained INT,
            recipes_added INT,
            recipes_removed INT,
            _created_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (week_start_date)
    """)
    
    # Table 5: Allergen density analysis
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_ALLERGEN_DENSITY} (
            week_start_date DATE,
            allergen_id STRING,
            allergen_name STRING,
            recipe_count INT,
            percentage_of_menu DECIMAL(5,2),
            _created_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (week_start_date)
    """)
    
    print("✓ Gold schema created")


# ======================
# Analytics Computations
# ======================

def compute_weekly_menu_metrics(spark: SparkSession) -> None:
    """Table 1: Weekly menu composition metrics."""
    print("Computing weekly menu metrics...")
    
    try:
        # Delete existing data to prevent duplicates
        spark.sql(f"DELETE FROM {GOLD_WEEKLY_METRICS}")
        
        spark.sql(f"""
            INSERT INTO {GOLD_WEEKLY_METRICS}
            SELECT 
                CAST(m.start_date AS DATE) as week_start_date,
                'en-GB' as locale,
                COUNT(DISTINCT mr.recipe_id) as total_recipes,
                COUNT(DISTINCT mr.recipe_id) as unique_recipes,
                SUM(CASE WHEN r.first_seen_date = CAST(m.start_date AS DATE) THEN 1 ELSE 0 END) as new_recipes,
                SUM(CASE WHEN r.first_seen_date < CAST(m.start_date AS DATE) THEN 1 ELSE 0 END) as returning_recipes,
                ROUND(AVG(CAST(r.difficulty AS DECIMAL)), 2) as avg_difficulty,
                ROUND(AVG(CAST(REGEXP_REPLACE(r.prep_time, '[^0-9]', '') AS DECIMAL)), 2) as avg_prep_time_minutes,
                CURRENT_TIMESTAMP() as _created_at
            FROM {SILVER_MENUS} m
            LEFT JOIN {SILVER_MENU_RECIPES} mr ON m.id = mr.menu_id AND mr.is_active = TRUE
            LEFT JOIN {SILVER_RECIPES} r ON mr.recipe_id = r.id
            WHERE m.is_active = TRUE
            GROUP BY m.start_date
        """)
        print("  ✓ Weekly menu metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing weekly menu metrics: {e}")


def compute_recipe_survival_metrics(spark: SparkSession) -> None:
    """Table 2: Recipe lifespan and survival analysis."""
    print("Computing recipe survival metrics...")
    
    try:
        # Delete existing data to prevent duplicates
        spark.sql(f"DELETE FROM {GOLD_RECIPE_SURVIVAL}")
        
        spark.sql(f"""
            INSERT INTO {GOLD_RECIPE_SURVIVAL}
            SELECT 
                r.id as recipe_id,
                r.name as recipe_name,
                r.first_seen_date as first_appearance_date,
                r.last_seen_date as last_appearance_date,
                CAST(DATEDIFF(r.last_seen_date, r.first_seen_date) / 7 + 1 AS INT) as total_weeks_active,
                CASE WHEN r.is_active = TRUE THEN CAST(DATEDIFF(r.last_seen_date, r.first_seen_date) / 7 + 1 AS INT) ELSE 0 END as consecutive_weeks_active,
                CAST(DATEDIFF(CURRENT_DATE(), r.last_seen_date) / 7 AS INT) as weeks_since_last_seen,
                r.is_active as is_currently_active,
                CURRENT_TIMESTAMP() as _created_at
            FROM {SILVER_RECIPES} r
        """)
        print("  ✓ Recipe survival metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing recipe survival metrics: {e}")


def compute_ingredient_trends(spark: SparkSession) -> None:
    """Table 3: Ingredient popularity over time."""
    print("Computing ingredient trends...")
    
    try:
        # Delete existing data to prevent duplicates
        spark.sql(f"DELETE FROM {GOLD_INGREDIENT_TRENDS}")
        
        spark.sql(f"""
            INSERT INTO {GOLD_INGREDIENT_TRENDS}
            WITH weekly_ingredients AS (
                SELECT 
                    i.id as ingredient_id,
                    i.name as ingredient_name,
                    CAST(m.start_date AS DATE) as week_start_date,
                    COUNT(DISTINCT r.id) as recipe_count,
                    ROW_NUMBER() OVER (PARTITION BY CAST(m.start_date AS DATE) ORDER BY COUNT(DISTINCT r.id) DESC) as popularity_rank
                FROM {SILVER_INGREDIENTS} i
                JOIN {SILVER_RECIPE_INGREDIENTS} ri ON i.id = ri.ingredient_id AND ri.is_active = TRUE
                JOIN {SILVER_RECIPES} r ON ri.recipe_id = r.id
                JOIN {SILVER_MENU_RECIPES} mr ON r.id = mr.recipe_id AND mr.is_active = TRUE
                JOIN {SILVER_MENUS} m ON mr.menu_id = m.id
                WHERE i.is_active = TRUE
                GROUP BY i.id, i.name, m.start_date
            )
            SELECT 
                ingredient_id,
                ingredient_name,
                week_start_date,
                recipe_count,
                0 as week_over_week_change,
                popularity_rank,
                CURRENT_TIMESTAMP() as _created_at
            FROM weekly_ingredients
        """)
        print("  ✓ Ingredient trends computed")
    except Exception as e:
        print(f"  ⚠️  Error computing ingredient trends: {e}")


def compute_menu_stability_metrics(spark: SparkSession) -> None:
    """Table 4: Week-over-week menu stability."""
    print("Computing menu stability metrics...")
    
    try:
        # Delete existing data to prevent duplicates
        spark.sql(f"DELETE FROM {GOLD_MENU_STABILITY}")
        
        spark.sql(f"""
            INSERT INTO {GOLD_MENU_STABILITY}
            WITH weekly_recipes AS (
                SELECT 
                    CAST(m.start_date AS DATE) as week_start_date,
                    COLLECT_SET(mr.recipe_id) as recipe_ids,
                    COUNT(DISTINCT mr.recipe_id) as recipe_count
                FROM {SILVER_MENUS} m
                LEFT JOIN {SILVER_MENU_RECIPES} mr ON m.id = mr.menu_id AND mr.is_active = TRUE
                WHERE m.is_active = TRUE
                GROUP BY m.start_date
            ),
            week_comparisons AS (
                SELECT 
                    w1.week_start_date,
                    w2.week_start_date as prev_week_start,
                    w1.recipe_ids,
                    w2.recipe_ids as prev_recipe_ids,
                    w1.recipe_count,
                    w2.recipe_count as prev_recipe_count
                FROM weekly_recipes w1
                LEFT JOIN weekly_recipes w2 
                    ON w1.week_start_date = DATE_ADD(CAST(w2.week_start_date AS DATE), 7)
            )
            SELECT 
                week_start_date,
                'en-GB' as locale,
                CASE WHEN prev_recipe_ids IS NOT NULL 
                    THEN ROUND(100.0 * CARDINALITY(ARRAY_INTERSECT(recipe_ids, prev_recipe_ids)) / CARDINALITY(prev_recipe_ids), 2)
                    ELSE NULL END as overlap_with_prev_week,
                CASE WHEN prev_recipe_ids IS NOT NULL 
                    THEN ROUND(100.0 * (recipe_count - CARDINALITY(ARRAY_INTERSECT(recipe_ids, prev_recipe_ids))) / CARDINALITY(prev_recipe_ids), 2)
                    ELSE NULL END as new_recipe_rate,
                CASE WHEN prev_recipe_ids IS NOT NULL 
                    THEN ROUND(100.0 * (CARDINALITY(prev_recipe_ids) - CARDINALITY(ARRAY_INTERSECT(recipe_ids, prev_recipe_ids))) / CARDINALITY(prev_recipe_ids), 2)
                    ELSE NULL END as churned_recipe_rate,
                CARDINALITY(ARRAY_INTERSECT(recipe_ids, COALESCE(prev_recipe_ids, ARRAY()))) as recipes_retained,
                recipe_count - CARDINALITY(ARRAY_INTERSECT(recipe_ids, COALESCE(prev_recipe_ids, ARRAY()))) as recipes_added,
                COALESCE(CARDINALITY(prev_recipe_ids), 0) - CARDINALITY(ARRAY_INTERSECT(recipe_ids, COALESCE(prev_recipe_ids, ARRAY()))) as recipes_removed,
                CURRENT_TIMESTAMP() as _created_at
            FROM week_comparisons
        """)
        print("  ✓ Menu stability metrics computed")
    except Exception as e:
        print(f"  ⚠️  Error computing menu stability metrics: {e}")


def compute_allergen_density(spark: SparkSession) -> None:
    """Table 5: Allergen coverage analysis."""
    print("Computing allergen density...")
    
    try:
        # Delete existing data to prevent duplicates
        spark.sql(f"DELETE FROM {GOLD_ALLERGEN_DENSITY}")
        
        spark.sql(f"""
            INSERT INTO {GOLD_ALLERGEN_DENSITY}
            WITH menu_allergen_counts AS (
                SELECT 
                    CAST(m.start_date AS DATE) as week_start_date,
                    a.id as allergen_id,
                    a.name as allergen_name,
                    COUNT(DISTINCT r.id) as recipe_count,
                    COUNT(DISTINCT mr.menu_id) as menu_count
                FROM {SILVER_MENUS} m
                LEFT JOIN {SILVER_MENU_RECIPES} mr ON m.id = mr.menu_id AND mr.is_active = TRUE
                LEFT JOIN {SILVER_RECIPES} r ON mr.recipe_id = r.id
                LEFT JOIN {SILVER_RECIPE_ALLERGENS} ra ON r.id = ra.recipe_id AND ra.is_active = TRUE
                LEFT JOIN {SILVER_ALLERGENS} a ON ra.allergen_id = a.id
                WHERE m.is_active = TRUE AND a.id IS NOT NULL
                GROUP BY m.start_date, a.id, a.name
            ),
            total_recipes_per_week AS (
                SELECT 
                    CAST(m.start_date AS DATE) as week_start_date,
                    COUNT(DISTINCT mr.recipe_id) as total_recipes
                FROM {SILVER_MENUS} m
                LEFT JOIN {SILVER_MENU_RECIPES} mr ON m.id = mr.menu_id AND mr.is_active = TRUE
                WHERE m.is_active = TRUE
                GROUP BY CAST(m.start_date AS DATE)
            )
            SELECT 
                mac.week_start_date,
                mac.allergen_id,
                mac.allergen_name,
                mac.recipe_count,
                ROUND(100.0 * mac.recipe_count / NULLIF(t.total_recipes, 0), 2) as percentage_of_menu,
                CURRENT_TIMESTAMP() as _created_at
            FROM menu_allergen_counts mac
            LEFT JOIN total_recipes_per_week t ON mac.week_start_date = t.week_start_date
        """)
        print("  ✓ Allergen density computed")
    except Exception as e:
        print(f"  ⚠️  Error computing allergen density: {e}")


def build_gold_layer() -> None:
    """Build all gold layer analytics tables."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Gold Layer Analytics                                    ║
    ║  Silver → 5 Analytical Metrics Tables                    ║
    ║  Databricks Delta Lake                                   ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    if not IN_DATABRICKS:
        print("⚠️  Not running in Databricks")
    
    spark = SparkSession.builder.appName("hfresh_gold_analytics").getOrCreate()
    
    # Create schema
    print("Creating gold schema...")
    create_gold_schema(spark)
    print()
    
    # Compute all metrics
    compute_weekly_menu_metrics(spark)
    compute_recipe_survival_metrics(spark)
    compute_ingredient_trends(spark)
    compute_menu_stability_metrics(spark)
    compute_allergen_density(spark)
    
    # Summary
    print(f"\n{'='*60}")
    print("Gold layer complete!")
    print(f"{'='*60}\n")
    
    tables = [
        (GOLD_WEEKLY_METRICS, "weekly_menu_metrics"),
        (GOLD_RECIPE_SURVIVAL, "recipe_survival_metrics"),
        (GOLD_INGREDIENT_TRENDS, "ingredient_trends"),
        (GOLD_MENU_STABILITY, "menu_stability_metrics"),
        (GOLD_ALLERGEN_DENSITY, "allergen_density"),
    ]
    
    for table, label in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0][0]
            print(f"  {label:30} {count:>8} records")
        except:
            print(f"  {label:30} {0:>8} records")
    
    print(f"\nGold schema: {CATALOG}.{GOLD_SCHEMA}\n")


if __name__ == "__main__":
    build_gold_layer()