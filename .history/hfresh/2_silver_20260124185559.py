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

def upsert_entity_scd(
    spark: SparkSession,
    target_table: str,
    id_column: str,
    source_df,
    pull_date: str,
) -> None:
    """
    Upsert entity with SCD Type 2 logic using Delta MERGE.
    
    - New records: first_seen_date = pull_date
    - Existing: Update last_seen_date, keep first_seen unchanged
    """
    pull_date_col = F.to_date(F.lit(pull_date))
    ingestion_ts = F.current_timestamp()
    
    # Add SCD columns to source
    source_df = source_df \
        .withColumn("_pull_date", pull_date_col) \
        .withColumn("_ingestion_ts", ingestion_ts)
    
    # MERGE statement for SCD Type 2
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING (
            SELECT DISTINCT * FROM source_data
        ) s
        ON t.{id_column} = s.{id_column}
        WHEN MATCHED THEN
            UPDATE SET 
                last_seen_date = s._pull_date,
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
                s._pull_date,
                s._pull_date,
                TRUE,
                s._ingestion_ts
            )
    """)


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
    
    # Flatten data to rows
    rows = []
    for item in data:
        rows.append({col: item.get(col) for col in columns})
    
    # Create DataFrame and upsert
    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("source_data")
    
    id_col = "id"
    upsert_entity_scd(spark, table, id_col, df, pull_date)


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

def process_recipe(conn: sqlite3.Connection, recipe: dict, pull_date: str) -> None:
    """Process a single recipe and its relationships."""
    if not recipe.get('id'):
        return
        
    fields = {
        'name': recipe.get('name'),
        'headline': recipe.get('headline'),
        'description': recipe.get('description'),
        'difficulty': recipe.get('difficulty'),
        'prep_time': recipe.get('prepTime'),
        'total_time': recipe.get('totalTime'),
        'serving_size': recipe.get('servingSize'),
        'cuisine': recipe.get('cuisine', {}).get('name') if recipe.get('cuisine') else None,
        'image_url': recipe.get('imagePath') or (recipe.get('image', {}).get('link') if recipe.get('image') else None),
    }
    
    upsert_entity(conn, 'recipes', 'recipe_id', recipe, pull_date, fields)
    
    # Process ingredients relationship
    for idx, ingredient in enumerate(recipe.get('ingredients', [])):
        ing_id = ingredient.get('id')
        if ing_id:
            # Also upsert the ingredient itself (from embedded data)
            ing_fields = {
                'name': ingredient.get('name'),
                'family': ingredient.get('family'),
                'type': ingredient.get('type'),
            }
            upsert_entity(conn, 'ingredients', 'ingredient_id', ingredient, pull_date, ing_fields)
            
            # Create bridge relationship
            upsert_bridge(
                conn,
                'recipe_ingredients',
                {'recipe_id': recipe['id'], 'ingredient_id': ing_id},
                pull_date,
                {
                    'quantity': ingredient.get('quantity'),
                    'unit': ingredient.get('unit'),
                    'position': idx,
                }
            )
    
    # Process allergens
    for allergen in recipe.get('allergens', []):
        if allergen.get('id'):
            # Upsert allergen from embedded data
            allergen_fields = {
                'name': allergen.get('name'),
                'type': allergen.get('type'),
                'icon_url': allergen.get('iconPath'),
            }
            upsert_entity(conn, 'allergens', 'allergen_id', allergen, pull_date, allergen_fields)
            
            # Create bridge
            upsert_bridge(
                conn,
                'recipe_allergens',
                {'recipe_id': recipe['id'], 'allergen_id': allergen['id']},
                pull_date
            )
    
    # Process tags
    for tag in recipe.get('tags', []):
        if tag.get('id'):
            tag_fields = {
                'name': tag.get('name'),
                'type': tag.get('type'),
                'icon_url': tag.get('iconPath'),
            }
            upsert_entity(conn, 'tags', 'tag_id', tag, pull_date, tag_fields)
            
            upsert_bridge(
                conn,
                'recipe_tags',
                {'recipe_id': recipe['id'], 'tag_id': tag['id']},
                pull_date
            )
    
    # Process labels
    for label in recipe.get('labels', []):
        if label.get('id'):
            label_fields = {
                'name': label.get('name'),
                'description': label.get('description'),
            }
            upsert_entity(conn, 'labels', 'label_id', label, pull_date, label_fields)
            
            upsert_bridge(
                conn,
                'recipe_labels',
                {'recipe_id': recipe['id'], 'label_id': label['id']},
                pull_date
            )


def process_ingredients(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process ingredients from API response (reference data)."""
    for ingredient in payload.get('data', []):
        fields = {
            'name': ingredient.get('name'),
            'family': ingredient.get('family'),
            'type': ingredient.get('type'),
        }
        upsert_entity(conn, 'ingredients', 'ingredient_id', ingredient, pull_date, fields)


def process_allergens(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process allergens from API response (reference data)."""
    for allergen in payload.get('data', []):
        fields = {
            'name': allergen.get('name'),
            'type': allergen.get('type'),
            'icon_url': allergen.get('iconPath'),
        }
        upsert_entity(conn, 'allergens', 'allergen_id', allergen, pull_date, fields)


def process_tags(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process tags from API response (reference data)."""
    for tag in payload.get('data', []):
        fields = {
            'name': tag.get('name'),
            'type': tag.get('type'),
            'icon_url': tag.get('iconPath'),
        }
        upsert_entity(conn, 'tags', 'tag_id', tag, pull_date, fields)


def process_labels(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process labels from API response (reference data)."""
    for label in payload.get('data', []):
        fields = {
            'name': label.get('name'),
            'description': label.get('description'),
        }
        upsert_entity(conn, 'labels', 'label_id', label, pull_date, fields)


def process_menus(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """
    Process menus from API response.
    
    CRITICAL: Menus now have embedded recipes, so we need to:
    1. Process the menu entity
    2. Extract and process all embedded recipes
    3. Create menu-recipe bridge relationships
    """
    for menu in payload.get('data', []):
        if not menu.get('id'):
            continue
            
        # Process menu entity
        menu_fields = {
            'url': menu.get('url'),
            'year_week': menu.get('year_week'),
            'start_date': menu.get('start'),
        }
        upsert_entity(conn, 'menus', 'menu_id', menu, pull_date, menu_fields)
        
        # Process embedded recipes
        recipes = menu.get('recipes', [])
        print(f"    Processing menu {menu['id']} with {len(recipes)} embedded recipes")
        
        for idx, recipe in enumerate(recipes):
            # Process the recipe and all its relationships
            process_recipe(conn, recipe, pull_date)
            
            # Create menu-recipe bridge
            if recipe.get('id'):
                upsert_bridge(
                    conn,
                    'menu_recipes',
                    {'menu_id': menu['id'], 'recipe_id': recipe['id']},
                    pull_date,
                    {'position': idx}
                )


# ======================
# Main Transformation
# ======================

def transform_bronze_to_silver() -> None:
    """
    Main transformation orchestrator.
    Reads bronze snapshots and builds silver layer.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Silver Layer Transformation                             ║
    ║  Bronze → Normalized Relational Tables                   ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Initialize database
    print("Initializing silver database...")
    conn = init_database()
    
    # Load all bronze snapshots
    print("Loading bronze snapshots...")
    snapshots = load_bronze_snapshots()
    print(f"Found {len(snapshots)} snapshots\n")
    
    # Process by endpoint type
    processors = {
        'ingredients': process_ingredients,
        'allergens': process_allergens,
        'tags': process_tags,
        'labels': process_labels,
        'menus': process_menus,  # This now handles embedded recipes
    }
    
    processed_count = 0
    
    for snapshot in snapshots:
        endpoint = snapshot['metadata']['endpoint']
        pull_date = snapshot['pull_date']
        
        print(f"Processing {endpoint} from {pull_date}...")
        
        if endpoint in processors:
            processors[endpoint](conn, snapshot['payload'], pull_date)
            processed_count += 1
            
            if processed_count % 10 == 0:
                conn.commit()
    
    conn.commit()
    
    # Print summary statistics
    print(f"\n{'='*60}")
    print("Transformation complete!")
    print(f"{'='*60}\n")
    
    cursor = conn.cursor()
    
    tables = ['recipes', 'ingredients', 'allergens', 'tags', 'labels', 'menus',
              'recipe_ingredients', 'recipe_allergens', 'recipe_tags', 
              'recipe_labels', 'menu_recipes']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:25} {count:>8} records")
    
    # Show menu composition
    print(f"\n{'='*60}")
    print("Menu composition:")
    print(f"{'='*60}\n")
    
    cursor.execute("""
        SELECT 
            m.menu_id,
            m.start_date,
            m.year_week,
            COUNT(mr.recipe_id) as recipe_count
        FROM menus m
        LEFT JOIN menu_recipes mr ON m.menu_id = mr.menu_id
        GROUP BY m.menu_id, m.start_date, m.year_week
        ORDER BY m.start_date DESC
    """)
    
    for row in cursor.fetchall():
        print(f"  Menu {row[0]:4} | Week {row[2]} ({row[1]}) | {row[3]:3} recipes")
    
    print(f"\nDatabase saved to: {SILVER_DB.absolute()}\n")
    
    conn.close()


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    transform_bronze_to_silver()