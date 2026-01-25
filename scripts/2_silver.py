"""
SQLite SILVER LAYER NORMALIZATION

Purpose
-------
Transforms Bronze → Silver layer with SCD Type 2 tracking.
Reads raw API responses and builds normalized, slowly-changing dimension tables.

Architecture
------------
- Source: hfresh.db, api_responses table
- Output: hfresh.db, silver schema tables
- SCD Tracking: first_seen_date, last_seen_date, is_active
- UPDATE/INSERT for idempotent upserts

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
From command line:
python scripts/2_silver.py

With GitHub Actions (after 1_bronze.py):
env HELLOFRESH_API_KEY="..." python scripts/2_silver.py
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


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
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ======================
# SCD Type 2 Upsert Logic
# ======================

def upsert_entity_with_scd(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict],
    pull_date: str,
    id_column: str,
) -> None:
    """
    Upsert entity with SCD Type 2 using UPDATE + INSERT pattern.
    
    Strategy:
    1. UPDATE existing records: set is_active=1, last_seen_date=pull_date if ID still exists
    2. INSERT new records: set first_seen_date=pull_date, is_active=1
    3. (Optional) Deactivate records not in this pull
    """
    if not rows:
        return
    
    cursor = conn.cursor()
    
    try:
        # Extract all IDs from incoming records
        incoming_ids = [row.get(id_column) for row in rows]
        
        # Step 1: Deactivate records that are no longer in source (optional, be careful)
        # For now, we just update existing + insert new
        
        # Step 2: UPDATE existing records with new data + update last_seen_date
        for row in rows:
            record_id = row.get(id_column)
            if not record_id:
                continue
            
            # Build the SET clause dynamically
            set_clauses = []
            params = []
            
            for col, val in row.items():
                if col != id_column:  # Don't update ID
                    set_clauses.append(f"  {col} = ?")
                    params.append(val)
            
            set_clauses.append("  last_seen_date = ?")
            set_clauses.append("  is_active = 1")
            set_clauses.append("  _ingestion_ts = ?")
            params.append(pull_date)
            params.append(datetime.utcnow().isoformat())
            
            # Try UPDATE first
            update_sql = f"""
            UPDATE {table}
            SET {', '.join(set_clauses)}
            WHERE {id_column} = ?
            """
            params.append(record_id)
            
            cursor.execute(update_sql, params)
            
            # If no rows updated, INSERT
            if cursor.rowcount == 0:
                # Build INSERT for this record
                cols = list(row.keys()) + [
                    'first_seen_date',
                    'last_seen_date',
                    'is_active',
                    '_ingestion_ts'
                ]
                placeholders = ', '.join(['?' for _ in cols])
                insert_sql = f"""
                INSERT INTO {table} ({', '.join(cols)})
                VALUES ({placeholders})
                """
                
                values = [row.get(col) for col in list(row.keys())]
                values.extend([
                    pull_date,
                    pull_date,
                    1,
                    datetime.utcnow().isoformat()
                ])
                
                cursor.execute(insert_sql, values)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error during SCD upsert to {table}: {e}")
        raise


def upsert_bridge_relationship(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict],
    pull_date: str,
) -> None:
    """Insert or update bridge table relationships with SCD columns."""
    if not rows:
        return
    
    cursor = conn.cursor()
    
    try:
        for row in rows:
            # Get the ID columns (typically two foreign keys)
            cols = list(row.keys()) + [
                'first_seen_date',
                'last_seen_date',
                'is_active',
                '_ingestion_ts'
            ]
            placeholders = ', '.join(['?' for _ in cols])
            
            sql = f"""
            INSERT OR IGNORE INTO {table} ({', '.join(cols)})
            VALUES ({placeholders})
            """
            
            values = [row.get(col) for col in list(row.keys())]
            values.extend([
                pull_date,
                pull_date,
                1,
                datetime.utcnow().isoformat()
            ])
            
            cursor.execute(sql, values)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error during bridge upsert to {table}: {e}")
        raise



# ======================
# Entity Processing
# ======================

def process_menus_endpoint(
    conn: sqlite3.Connection,
    payload: dict,
    pull_date: str
) -> None:
    """
    Process menus with embedded recipes.
    Handles: menus, recipes, recipe-ingredient relationships, menu-recipe relationships.
    """
    menus_data = payload.get('data', [])
    if not menus_data:
        return
    
    try:
        menu_rows = []
        recipe_rows = []
        recipe_ingredient_rows = []
        menu_recipe_rows = []
        
        for menu in menus_data:
            menu_id = menu.get('id')
            if not menu_id:
                continue
                
            menu_rows.append({
                'id': menu_id,
                'url': menu.get('url'),
                'year_week': menu.get('year_week'),
                'start_date': menu.get('start'),
            })
            
            # Process embedded recipes
            for idx, recipe in enumerate(menu.get('recipes', [])):
                recipe_id = recipe.get('id')
                if not recipe_id:
                    continue
                    
                recipe_rows.append({
                    'id': recipe_id,
                    'name': recipe.get('name'),
                    'headline': recipe.get('headline'),
                    'description': recipe.get('description'),
                    'difficulty': recipe.get('difficulty'),
                    'prep_time': recipe.get('prepTime'),
                    'total_time': recipe.get('totalTime'),
                    'serving_size': recipe.get('servingSize'),
                    'cuisine': recipe.get('cuisine', {}).get('name') if recipe.get('cuisine') else None,
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
                    ing_id = ingredient.get('id')
                    if ing_id:
                        recipe_ingredient_rows.append({
                            'recipe_id': recipe_id,
                            'ingredient_id': ing_id,
                            'quantity': ingredient.get('quantity'),
                            'unit': ingredient.get('unit'),
                            'position': ing_idx,
                        })
        
        # Write to Silver
        if menu_rows:
            upsert_entity_with_scd(conn, 'menus', menu_rows, pull_date, 'id')
            print(f"  ✓ Processed {len(menu_rows)} menus")
        
        if recipe_rows:
            upsert_entity_with_scd(conn, 'recipes', recipe_rows, pull_date, 'id')
            print(f"  ✓ Processed {len(recipe_rows)} recipes")
        
        if recipe_ingredient_rows:
            upsert_bridge_relationship(conn, 'recipe_ingredients', recipe_ingredient_rows, pull_date)
            print(f"  ✓ Processed {len(recipe_ingredient_rows)} recipe-ingredient links")
        
        if menu_recipe_rows:
            upsert_bridge_relationship(conn, 'menu_recipes', menu_recipe_rows, pull_date)
            print(f"  ✓ Processed {len(menu_recipe_rows)} menu-recipe links")
    except Exception as e:
        print(f"  ⚠️  Error processing menus: {e}")


def process_reference_endpoint(
    conn: sqlite3.Connection,
    table: str,
    payload: dict,
    pull_date: str,
    columns: list
) -> None:
    """Process reference data (ingredients, allergens, tags, labels)."""
    data = payload.get('data', [])
    if not data:
        return
    
    try:
        # Flatten data to rows, mapping API column names to DB column names
        rows = []
        for item in data:
            row_dict = {}
            for col in columns:
                # Map common API patterns to DB column names
                api_field = col
                if col.endswith('_id'):
                    api_field = 'id'
                elif col == 'icon_url':
                    api_field = 'iconPath'
                
                row_dict[col] = item.get(api_field)
            rows.append(row_dict)
        
        # Upsert with SCD
        upsert_entity_with_scd(conn, table, rows, pull_date, columns[0])
        print(f"  ✓ Processed {len(rows)} records to {table}")
    except Exception as e:
        print(f"  ⚠️  Error processing {table}: {e}")


def process_bronze_to_silver(conn: sqlite3.Connection, pull_date: str = None) -> None:
    """
    Main transformation: Read Bronze → Transform → Write Silver with SCD Type 2.
    """
    cursor = conn.cursor()
    
    # Get latest pull_date if not specified
    if not pull_date:
        cursor.execute("SELECT MAX(pull_date) FROM api_responses")
        result = cursor.fetchone()
        pull_date = result[0] if result and result[0] else str(datetime.now().date())
    
    print(f"Processing pull_date: {pull_date}")
    
    try:
        # Read bronze data for this pull
        cursor.execute("""
            SELECT DISTINCT endpoint, payload
            FROM api_responses
            WHERE pull_date = ?
            ORDER BY endpoint
        """, (pull_date,))
        
        rows = cursor.fetchall()
        if not rows:
            print("  ⚠️  No bronze data found for this date")
            return
        
        # Process each endpoint
        for row in rows:
            endpoint = row[0]
            payload_str = row[1]
            
            try:
                payload = json.loads(payload_str)
            except:
                print(f"  ⚠️  Could not parse JSON for {endpoint}")
                continue
            
            if endpoint == "menus":
                process_menus_endpoint(conn, payload, pull_date)
            elif endpoint == "ingredients":
                process_reference_endpoint(
                    conn, 'ingredients', payload, pull_date,
                    ['ingredient_id', 'name', 'family', 'type']
                )
            elif endpoint == "allergens":
                process_reference_endpoint(
                    conn, 'allergens', payload, pull_date,
                    ['allergen_id', 'name', 'type', 'icon_url']
                )
            elif endpoint == "tags":
                process_reference_endpoint(
                    conn, 'tags', payload, pull_date,
                    ['tag_id', 'name', 'type', 'icon_url']
                )
            elif endpoint == "labels":
                process_reference_endpoint(
                    conn, 'labels', payload, pull_date,
                    ['label_id', 'name', 'description']
                )
    except Exception as e:
        print(f"  ⚠️  Error in process_bronze_to_silver: {e}")


# ======================
# Main Transformation
# ======================

def transform_bronze_to_silver() -> None:
    """
    Main transformation orchestrator.
    Reads Bronze table and writes to Silver with SCD Type 2.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  Silver Layer Transformation                             ║
    ║  Bronze → Normalized SCD Type 2 Tables                   ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = get_db_connection()
    
    # Process bronze data
    print("\nProcessing bronze snapshots...")
    process_bronze_to_silver(conn)
    
    # Summary
    print(f"\n{'='*60}")
    print("Transformation complete!")
    print(f"{'='*60}\n")
    
    # Show row counts
    cursor = conn.cursor()
    tables = [
        ('recipes', 'recipes'),
        ('ingredients', 'ingredients'),
        ('allergens', 'allergens'),
        ('tags', 'tags'),
        ('labels', 'labels'),
        ('menus', 'menus'),
        ('recipe_ingredients', 'recipe_ingredients'),
        ('recipe_allergens', 'recipe_allergens'),
        ('recipe_tags', 'recipe_tags'),
        ('recipe_labels', 'recipe_labels'),
        ('menu_recipes', 'menu_recipes'),
    ]
    
    for table_name, label in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE is_active = 1")
            count = cursor.fetchone()[0]
            print(f"  {label:25} {count:>8} records (active)")
        except:
            print(f"  {label:25} {0:>8} records")
    
    print(f"\nSilver layer: {DB_PATH}\n")
    conn.close()


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    transform_bronze_to_silver()