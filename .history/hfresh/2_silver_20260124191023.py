"""
DATABRICKS SILVER LAYER NORMALIZATION

Purpose
-------
Transforms Bronze → Silver layer with SCD Type 2 tracking.
Reads raw API responses and builds normalized, slowly-changing dimension tables.

Architecture
------------
- Source: hfresh_bronze.api_responses Delta table
- Output: hfresh_silver.* Delta tables
- SCD Tracking: first_seen_date, last_seen_date, is_active
- MERGE for idempotent updates

Core Tables (SCD Type 2)
------------------------
- recipes
- ingredients
- allergens
- tags
- labels
- menus

Bridge Tables (Many-to-Many)
----------------------------
- recipe_ingredients
- recipe_allergens
- recipe_tags
- recipe_labels
- menu_recipes

Usage
-----
In Databricks notebook:
%run ./2_silver

Or after Bronze:
dbutils.notebook.run("2_silver", 60, {"pull_date": "2026-01-24"})
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import json
from datetime import datetime

# Databricks imports
try:
    spark = SparkSession.builder.appName("hfresh_silver_normalization").getOrCreate()
    IN_DATABRICKS = True
except:
    IN_DATABRICKS = False


# ======================
# Configuration
# ======================

BRONZE_CATALOG = "hfresh_catalog"
BRONZE_SCHEMA = "hfresh_bronze"
SILVER_SCHEMA = "hfresh_silver"

BRONZE_TABLE = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.api_responses"
SILVER_RECIPES = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.recipes"
SILVER_INGREDIENTS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.ingredients"
SILVER_ALLERGENS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.allergens"
SILVER_TAGS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.tags"
SILVER_LABELS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.labels"
SILVER_MENUS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.menus"
SILVER_RECIPE_INGREDIENTS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.recipe_ingredients"
SILVER_RECIPE_ALLERGENS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.recipe_allergens"
SILVER_RECIPE_TAGS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.recipe_tags"
SILVER_RECIPE_LABELS = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.recipe_labels"
SILVER_MENU_RECIPES = f"{BRONZE_CATALOG}.{SILVER_SCHEMA}.menu_recipes"


# ======================
# Database Schema - Databricks Delta
# ======================

def create_silver_schema(spark: SparkSession) -> None:
    """Create silver schema and all tables."""
    
    # Create catalog and schema
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {BRONZE_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{SILVER_SCHEMA}")
    
    # Recipes (SCD Type 2)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_RECIPES} (
            recipe_id STRING NOT NULL,
            name STRING,
            headline STRING,
            description STRING,
            difficulty INT,
            prep_time STRING,
            total_time STRING,
            serving_size INT,
            cuisine STRING,
            image_url STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Ingredients (SCD Type 2)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_INGREDIENTS} (
            ingredient_id STRING NOT NULL,
            name STRING,
            family STRING,
            type STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Allergens (SCD Type 2)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_ALLERGENS} (
            allergen_id STRING NOT NULL,
            name STRING,
            type STRING,
            icon_url STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Tags (SCD Type 2)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TAGS} (
            tag_id STRING NOT NULL,
            name STRING,
            type STRING,
            icon_url STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Labels (SCD Type 2)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_LABELS} (
            label_id STRING NOT NULL,
            name STRING,
            description STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Menus
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_MENUS} (
            menu_id STRING NOT NULL,
            url STRING,
            year_week INT,
            start_date STRING,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Bridge: Recipe-Ingredients
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_RECIPE_INGREDIENTS} (
            recipe_id STRING NOT NULL,
            ingredient_id STRING NOT NULL,
            quantity STRING,
            unit STRING,
            position INT,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Bridge: Recipe-Allergens
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_RECIPE_ALLERGENS} (
            recipe_id STRING NOT NULL,
            allergen_id STRING NOT NULL,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Bridge: Recipe-Tags
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_RECIPE_TAGS} (
            recipe_id STRING NOT NULL,
            tag_id STRING NOT NULL,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Bridge: Recipe-Labels
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_RECIPE_LABELS} (
            recipe_id STRING NOT NULL,
            label_id STRING NOT NULL,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    # Bridge: Menu-Recipes
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_MENU_RECIPES} (
            menu_id STRING NOT NULL,
            recipe_id STRING NOT NULL,
            position INT,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            _ingestion_ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (first_seen_date)
    """)
    
    print("✓ Silver schema created")


# ======================
# Database Setup
# ======================

def init_database() -> sqlite3.Connection:
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(SILVER_DB)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


# ======================
# Bronze Data Reader & SCD Merge Functions
# ======================

def upsert_entity_with_scd(
    spark: SparkSession,
    target_table: str,
    source_df,
    pull_date: str,
    id_column: str,
) -> None:
    """
    Upsert entity with SCD Type 2 using Delta MERGE.
    Handles both new and existing records properly.
    """
    try:
        pull_date_col = F.to_date(F.lit(pull_date))
        
        # Add SCD columns to source (only for new records)
        source_with_dates = source_df \
            .withColumn("first_seen_date", pull_date_col) \
            .withColumn("last_seen_date", pull_date_col) \
            .withColumn("is_active", F.lit(True)) \
            .withColumn("_ingestion_ts", F.current_timestamp())
        
        source_with_dates.createOrReplaceTempView("source_records")
        
        # Check if table has data
        try:
            spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists or spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0] == 0:
            # First load - just insert everything
            source_with_dates.write \
                .format("delta") \
                .mode("append") \
                .insertInto(target_table)
        else:
            # Upsert with SCD logic
            merge_sql = f"""
                MERGE INTO {target_table} t
                USING source_records s
                ON t.{id_column} = s.{id_column}
                WHEN MATCHED THEN
                    UPDATE SET 
                        last_seen_date = s.last_seen_date,
                        is_active = TRUE,
                        _ingestion_ts = s._ingestion_ts
                WHEN NOT MATCHED THEN
                    INSERT (
                        {id_column},
                        first_seen_date,
                        last_seen_date,
                        is_active,
                        _ingestion_ts
                    )
                    VALUES (
                        s.{id_column},
                        s.first_seen_date,
                        s.last_seen_date,
                        TRUE,
                        s._ingestion_ts
                    )
            """
            spark.sql(merge_sql)
    except Exception as e:
        print(f"Error during SCD upsert to {target_table}: {e}")
        # Fallback to append mode
        print(f"  Falling back to append mode")
        source_df.write \
            .format("delta") \
            .mode("append") \
            .insertInto(target_table)


# ======================
# Entity Extraction & Processing
# ======================

def process_bronze_to_silver(spark: SparkSession, pull_date: str = None) -> None:
    """
    Main transformation: Read Bronze → Transform → Write Silver with SCD Type 2.
    
    Strategy:
    1. Read latest bronze data (or specific pull_date)
    2. Parse JSON payloads
    3. Extract entities and relationships
    4. Upsert to Silver tables with SCD Type 2 logic
    """
    if not pull_date:
        # Get latest pull_date from bronze
        pull_date = spark.sql(f"SELECT MAX(pull_date) FROM {BRONZE_TABLE}") \
            .collect()[0][0]
    
    print(f"Processing pull_date: {pull_date}")
    
    # Read bronze data for this pull
    bronze_data = spark.sql(f"""
        SELECT 
            pull_date,
            endpoint,
            payload
        FROM {BRONZE_TABLE}
        WHERE pull_date = '{pull_date}'
    """)
    
    # Process each endpoint
    for row in bronze_data.collect():
        endpoint = row['endpoint']
        payload_str = row['payload']
        payload = json.loads(payload_str)
        
        print(f"  Processing {endpoint}...")
        
        if endpoint == "menus":
            process_menus_endpoint(spark, payload, pull_date)
        elif endpoint == "ingredients":
            process_reference_endpoint(spark, SILVER_INGREDIENTS, payload, pull_date, 
                                     ['id', 'name', 'family', 'type'])
        elif endpoint == "allergens":
            process_reference_endpoint(spark, SILVER_ALLERGENS, payload, pull_date,
                                     ['id', 'name', 'type', 'iconPath'])
        elif endpoint == "tags":
            process_reference_endpoint(spark, SILVER_TAGS, payload, pull_date,
                                     ['id', 'name', 'type', 'iconPath'])
        elif endpoint == "labels":
            process_reference_endpoint(spark, SILVER_LABELS, payload, pull_date,
                                     ['id', 'name', 'description'])


def process_reference_endpoint(spark: SparkSession, table: str, payload: dict, 
                               pull_date: str, columns: list) -> None:
    """Process reference data (ingredients, allergens, tags, labels)."""
    data = payload.get('data', [])
    if not data:
        return
    
    try:
        # Flatten data to rows
        rows = []
        for item in data:
            row_dict = {}
            for col in columns:
                row_dict[col] = item.get(col)
            rows.append(row_dict)
        
        # Create DataFrame
        df = spark.createDataFrame(rows)
        
        # Rename 'id' column to match table schema
        if 'id' in df.columns:
            id_col = list(columns)[0]  # First column should be the ID
            df = df.withColumnRenamed('id', id_col)
        
        # Upsert with SCD
        upsert_entity_with_scd(spark, table, df, pull_date, columns[0])
        print(f"  ✓ Processed {len(rows)} records to {table.split('.')[-1]}")
    except Exception as e:
        print(f"  ⚠️  Error processing {table}: {e}")


def process_menus_endpoint(spark: SparkSession, payload: dict, pull_date: str) -> None:
    """
    Process menus with embedded recipes.
    Handles: menus, recipes, recipe-ingredient relationships, menu-recipe relationships.
    """
    menus_data = payload.get('data', [])
    
    # Process menus
    menu_rows = []
    recipe_rows = []
    recipe_ingredient_rows = []
    menu_recipe_rows = []
    
    for menu in menus_data:
        menu_id = menu.get('id')
        menu_rows.append({
            'id': menu_id,
            'url': menu.get('url'),
            'year_week': menu.get('year_week'),
            'start_date': menu.get('start'),
        })
        
        # Process embedded recipes
        for idx, recipe in enumerate(menu.get('recipes', [])):
            recipe_id = recipe.get('id')
            recipe_rows.append({
                'id': recipe_id,
                'name': recipe.get('name'),
                'headline': recipe.get('headline'),
                'description': recipe.get('description'),
                'difficulty': recipe.get('difficulty'),
                'prep_time': recipe.get('prepTime'),
                'total_time': recipe.get('totalTime'),
                'serving_size': recipe.get('servingSize'),
                'cuisine': recipe.get('cuisine', {}).get('name'),
                'image_url': recipe.get('imagePath'),
            })
            
            # Menu-Recipe relationship
            menu_recipe_rows.append({
                'menu_id': menu_id,
                'recipe_id': recipe_id,
                'position': idx,
            })
            
            # Recipe-Ingredient relationships
            for ing_idx, ingredient in enumerate(recipe.get('ingredients', [])):
                recipe_ingredient_rows.append({
                    'recipe_id': recipe_id,
                    'ingredient_id': ingredient.get('id'),
                    'quantity': ingredient.get('quantity'),
                    'unit': ingredient.get('unit'),
                    'position': ing_idx,
                })
    
    # Write to Silver (with SCD logic for entities, simple insert for relationships)
    if menu_rows:
        spark.createDataFrame(menu_rows).createOrReplaceTempView("source_data")
        upsert_entity_scd(spark, SILVER_MENUS, 'id', spark.sql("SELECT * FROM source_data"), pull_date)
    
    if recipe_rows:
        spark.createDataFrame(recipe_rows).createOrReplaceTempView("source_data")
        upsert_entity_scd(spark, SILVER_RECIPES, 'id', spark.sql("SELECT * FROM source_data"), pull_date)
    
    if recipe_ingredient_rows:
        df = spark.createDataFrame(recipe_ingredient_rows)
        upsert_bridge_relationship(spark, SILVER_RECIPE_INGREDIENTS, df, pull_date)
    
    if menu_recipe_rows:
        df = spark.createDataFrame(menu_recipe_rows)
        upsert_bridge_relationship(spark, SILVER_MENU_RECIPES, df, pull_date)


def upsert_bridge_relationship(spark: SparkSession, table: str, df, pull_date: str) -> None:
    """Insert or update bridge table relationships."""
    pull_date_col = F.to_date(F.lit(pull_date))
    ingestion_ts = F.current_timestamp()
    
    df = df \
        .withColumn("first_seen_date", pull_date_col) \
        .withColumn("last_seen_date", pull_date_col) \
        .withColumn("is_active", F.lit(True)) \
        .withColumn("_ingestion_ts", ingestion_ts)
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .insertInto(table)


# ======================
# Entity Processors
# ======================

# Now handled by process_bronze_to_silver() above


# ======================
# Main Transformation
# ======================

def transform_bronze_to_silver() -> None:
    """
    Main transformation orchestrator.
    Reads Bronze Delta table and writes to Silver with SCD Type 2.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Silver Layer Transformation                             ║
    ║  Bronze → Normalized SCD Type 2 Tables                   ║
    ║  Databricks Delta Lake                                   ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Initialize Spark
    if not IN_DATABRICKS:
        print("⚠️  Not running in Databricks")
    
    spark = SparkSession.builder.appName("hfresh_silver_normalization").getOrCreate()
    
    # Create schema and tables
    print("Creating silver schema...")
    create_silver_schema(spark)
    
    # Process bronze data
    print("\nProcessing bronze snapshots...")
    process_bronze_to_silver(spark)
    
    # Summary
    print(f"\n{'='*60}")
    print("Transformation complete!")
    print(f"{'='*60}\n")
    
    # Show row counts
    tables = [
        (SILVER_RECIPES, "recipes"),
        (SILVER_INGREDIENTS, "ingredients"),
        (SILVER_ALLERGENS, "allergens"),
        (SILVER_TAGS, "tags"),
        (SILVER_LABELS, "labels"),
        (SILVER_MENUS, "menus"),
        (SILVER_RECIPE_INGREDIENTS, "recipe_ingredients"),
        (SILVER_RECIPE_ALLERGENS, "recipe_allergens"),
        (SILVER_RECIPE_TAGS, "recipe_tags"),
        (SILVER_RECIPE_LABELS, "recipe_labels"),
        (SILVER_MENU_RECIPES, "menu_recipes"),
    ]
    
    for table_name, label in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0][0]
            print(f"  {label:25} {count:>8} records")
        except:
            print(f"  {label:25} {0:>8} records")
    
    print(f"\nSilver schema: {BRONZE_CATALOG}.{SILVER_SCHEMA}\n")


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    transform_bronze_to_silver()