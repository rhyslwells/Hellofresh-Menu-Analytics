"""
HFRESH SILVER LAYER TRANSFORMATION

Purpose
-------
Reads bronze-layer snapshots and builds normalized, slowly-changing
dimension tables. Tracks entity lifecycle (first_seen, last_seen, is_active).

Architecture
------------
Bronze → Silver transformation that:
- Deduplicates entities across weekly pulls
- Tracks temporal state (first/last seen dates)
- Builds relational bridge tables
- Enables historical analysis

Tables Created
--------------
Core entities:
- recipes
- ingredients
- allergens
- tags
- labels
- menus

Bridge tables (many-to-many):
- recipe_ingredients
- recipe_allergens
- recipe_tags
- recipe_labels
- menu_recipes

Usage
-----
python silver_transform.py
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any


# ======================
# Configuration
# ======================

BRONZE_DIR = Path("./bronze_data")
SILVER_DB = Path("./silver_data.db")


# ======================
# Database Schema
# ======================

SCHEMA_SQL = """
-- Core entity: Recipes
CREATE TABLE IF NOT EXISTS recipes (
    recipe_id TEXT PRIMARY KEY,
    name TEXT,
    headline TEXT,
    description TEXT,
    difficulty INTEGER,
    prep_time TEXT,
    total_time TEXT,
    serving_size INTEGER,
    cuisine TEXT,
    image_url TEXT,
    raw_json TEXT,  -- Full API response for reference
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Core entity: Ingredients
CREATE TABLE IF NOT EXISTS ingredients (
    ingredient_id TEXT PRIMARY KEY,
    name TEXT,
    family TEXT,
    type TEXT,
    raw_json TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Core entity: Allergens
CREATE TABLE IF NOT EXISTS allergens (
    allergen_id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    icon_url TEXT,
    raw_json TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Core entity: Tags
CREATE TABLE IF NOT EXISTS tags (
    tag_id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    icon_url TEXT,
    raw_json TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Core entity: Labels
CREATE TABLE IF NOT EXISTS labels (
    label_id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    raw_json TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Core entity: Menus
CREATE TABLE IF NOT EXISTS menus (
    menu_id TEXT PRIMARY KEY,
    name TEXT,
    start_date TEXT,
    end_date TEXT,
    week_number INTEGER,
    year INTEGER,
    raw_json TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1
);

-- Bridge: Recipe → Ingredients
CREATE TABLE IF NOT EXISTS recipe_ingredients (
    recipe_id TEXT,
    ingredient_id TEXT,
    quantity TEXT,
    unit TEXT,
    position INTEGER,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    PRIMARY KEY (recipe_id, ingredient_id, first_seen_date),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id)
);

-- Bridge: Recipe → Allergens
CREATE TABLE IF NOT EXISTS recipe_allergens (
    recipe_id TEXT,
    allergen_id TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    PRIMARY KEY (recipe_id, allergen_id, first_seen_date),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (allergen_id) REFERENCES allergens(allergen_id)
);

-- Bridge: Recipe → Tags
CREATE TABLE IF NOT EXISTS recipe_tags (
    recipe_id TEXT,
    tag_id TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    PRIMARY KEY (recipe_id, tag_id, first_seen_date),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

-- Bridge: Recipe → Labels
CREATE TABLE IF NOT EXISTS recipe_labels (
    recipe_id TEXT,
    label_id TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    PRIMARY KEY (recipe_id, label_id, first_seen_date),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (label_id) REFERENCES labels(label_id)
);

-- Bridge: Menu → Recipes
CREATE TABLE IF NOT EXISTS menu_recipes (
    menu_id TEXT,
    recipe_id TEXT,
    position INTEGER,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    PRIMARY KEY (menu_id, recipe_id, first_seen_date),
    FOREIGN KEY (menu_id) REFERENCES menus(menu_id),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_recipes_active ON recipes(is_active, last_seen_date);
CREATE INDEX IF NOT EXISTS idx_ingredients_active ON ingredients(is_active);
CREATE INDEX IF NOT EXISTS idx_menus_dates ON menus(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_menu_recipes_menu ON menu_recipes(menu_id);
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipe ON recipe_ingredients(recipe_id);
"""


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
# Bronze Data Reader
# ======================

def load_bronze_snapshots() -> list[dict[str, Any]]:
    """
    Load all bronze snapshots in chronological order.
    Returns list of (pull_date, endpoint, payload) tuples.
    """
    snapshots = []
    
    for pull_dir in sorted(BRONZE_DIR.iterdir()):
        if not pull_dir.is_dir():
            continue
            
        pull_date = pull_dir.name
        
        for snapshot_file in sorted(pull_dir.glob("*.json")):
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            snapshots.append({
                'pull_date': pull_date,
                'metadata': data['metadata'],
                'payload': data['payload'],
            })
    
    return snapshots


# ======================
# Entity Upsert Logic
# ======================

def upsert_entity(
    conn: sqlite3.Connection,
    table: str,
    entity_id_field: str,
    entity: dict,
    pull_date: str,
    additional_fields: dict | None = None,
) -> None:
    """
    Insert or update entity with slowly-changing dimension logic.
    
    - If new: insert with first_seen = last_seen = pull_date
    - If exists: update last_seen_date, keep first_seen unchanged
    """
    entity_id = entity.get('id') or entity.get(entity_id_field)
    if not entity_id:
        return
    
    cursor = conn.cursor()
    
    # Check if entity exists
    cursor.execute(
        f"SELECT first_seen_date FROM {table} WHERE {entity_id_field} = ?",
        (entity_id,)
    )
    result = cursor.fetchone()
    
    # Build field mappings
    fields = additional_fields or {}
    fields['raw_json'] = json.dumps(entity)
    
    if result is None:
        # New entity
        fields['first_seen_date'] = pull_date
        fields['last_seen_date'] = pull_date
        fields['is_active'] = 1
        fields[entity_id_field] = entity_id
        
        columns = ', '.join(fields.keys())
        placeholders = ', '.join(['?' for _ in fields])
        
        cursor.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            list(fields.values())
        )
    else:
        # Update existing
        fields['last_seen_date'] = pull_date
        fields['is_active'] = 1
        
        set_clause = ', '.join([f"{k} = ?" for k in fields.keys()])
        
        cursor.execute(
            f"UPDATE {table} SET {set_clause} WHERE {entity_id_field} = ?",
            list(fields.values()) + [entity_id]
        )


def upsert_bridge(
    conn: sqlite3.Connection,
    table: str,
    key_fields: dict,
    pull_date: str,
    additional_fields: dict | None = None,
) -> None:
    """
    Upsert bridge table relationship.
    """
    cursor = conn.cursor()
    
    # Build WHERE clause for existence check
    where_parts = [f"{k} = ?" for k in key_fields.keys()]
    where_clause = " AND ".join(where_parts)
    where_values = list(key_fields.values())
    
    cursor.execute(
        f"SELECT first_seen_date FROM {table} WHERE {where_clause}",
        where_values
    )
    result = cursor.fetchone()
    
    fields = {**key_fields, **(additional_fields or {})}
    
    if result is None:
        # New relationship
        fields['first_seen_date'] = pull_date
        fields['last_seen_date'] = pull_date
        
        columns = ', '.join(fields.keys())
        placeholders = ', '.join(['?' for _ in fields])
        
        cursor.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            list(fields.values())
        )
    else:
        # Update last_seen
        cursor.execute(
            f"UPDATE {table} SET last_seen_date = ? WHERE {where_clause}",
            [pull_date] + where_values
        )


# ======================
# Entity Processors
# ======================

def process_recipes(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process recipes from API response."""
    for recipe in payload.get('data', []):
        fields = {
            'name': recipe.get('name'),
            'headline': recipe.get('headline'),
            'description': recipe.get('description'),
            'difficulty': recipe.get('difficulty'),
            'prep_time': recipe.get('prepTime'),
            'total_time': recipe.get('totalTime'),
            'serving_size': recipe.get('servingSize'),
            'cuisine': recipe.get('cuisine', {}).get('name') if recipe.get('cuisine') else None,
            'image_url': recipe.get('image', {}).get('link') if recipe.get('image') else None,
        }
        
        upsert_entity(conn, 'recipes', 'recipe_id', recipe, pull_date, fields)
        
        # Process ingredients relationship
        for idx, ingredient in enumerate(recipe.get('ingredients', [])):
            ing_id = ingredient.get('id')
            if ing_id:
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
                upsert_bridge(
                    conn,
                    'recipe_allergens',
                    {'recipe_id': recipe['id'], 'allergen_id': allergen['id']},
                    pull_date
                )
        
        # Process tags
        for tag in recipe.get('tags', []):
            if tag.get('id'):
                upsert_bridge(
                    conn,
                    'recipe_tags',
                    {'recipe_id': recipe['id'], 'tag_id': tag['id']},
                    pull_date
                )
        
        # Process labels
        for label in recipe.get('labels', []):
            if label.get('id'):
                upsert_bridge(
                    conn,
                    'recipe_labels',
                    {'recipe_id': recipe['id'], 'label_id': label['id']},
                    pull_date
                )


def process_ingredients(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process ingredients from API response."""
    for ingredient in payload.get('data', []):
        fields = {
            'name': ingredient.get('name'),
            'family': ingredient.get('family'),
            'type': ingredient.get('type'),
        }
        upsert_entity(conn, 'ingredients', 'ingredient_id', ingredient, pull_date, fields)


def process_allergens(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process allergens from API response."""
    for allergen in payload.get('data', []):
        fields = {
            'name': allergen.get('name'),
            'type': allergen.get('type'),
            'icon_url': allergen.get('iconPath'),
        }
        upsert_entity(conn, 'allergens', 'allergen_id', allergen, pull_date, fields)


def process_tags(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process tags from API response."""
    for tag in payload.get('data', []):
        fields = {
            'name': tag.get('name'),
            'type': tag.get('type'),
            'icon_url': tag.get('iconPath'),
        }
        upsert_entity(conn, 'tags', 'tag_id', tag, pull_date, fields)


def process_labels(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process labels from API response."""
    for label in payload.get('data', []):
        fields = {
            'name': label.get('name'),
            'description': label.get('description'),
        }
        upsert_entity(conn, 'labels', 'label_id', label, pull_date, fields)


def process_menus(conn: sqlite3.Connection, payload: dict, pull_date: str) -> None:
    """Process menus from API response."""
    for menu in payload.get('data', []):
        fields = {
            'name': menu.get('name'),
            'start_date': menu.get('startDate'),
            'end_date': menu.get('endDate'),
            'week_number': menu.get('week'),
            'year': menu.get('year'),
        }
        
        upsert_entity(conn, 'menus', 'menu_id', menu, pull_date, fields)
        
        # Process menu-recipe relationships
        for idx, recipe in enumerate(menu.get('recipes', [])):
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
        'recipes': process_recipes,
        'ingredients': process_ingredients,
        'allergens': process_allergens,
        'tags': process_tags,
        'labels': process_labels,
        'menus': process_menus,
    }
    
    processed_count = 0
    
    for snapshot in snapshots:
        endpoint = snapshot['metadata']['endpoint']
        pull_date = snapshot['pull_date']
        
        if endpoint in processors:
            processors[endpoint](conn, snapshot['payload'], pull_date)
            processed_count += 1
            
            if processed_count % 50 == 0:
                print(f"Processed {processed_count}/{len(snapshots)} snapshots...")
                conn.commit()
    
    conn.commit()
    
    # Print summary statistics
    print(f"\n{'='*60}")
    print("Transformation complete!")
    print(f"{'='*60}\n")
    
    cursor = conn.cursor()
    
    tables = ['recipes', 'ingredients', 'allergens', 'tags', 'labels', 'menus',
              'recipe_ingredients', 'recipe_allergens', 'menu_recipes']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:25} {count:>8} records")
    
    # Show entity lifecycle examples
    print(f"\n{'='*60}")
    print("Sample entity lifecycle (recipes active > 10 weeks):")
    print(f"{'='*60}\n")
    
    cursor.execute("""
        SELECT 
            name,
            first_seen_date,
            last_seen_date,
            JULIANDAY(last_seen_date) - JULIANDAY(first_seen_date) as days_active
        FROM recipes
        WHERE is_active = 1
        ORDER BY days_active DESC
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0][:40]:40} | {row[1]} → {row[2]} ({int(row[3])} days)")
    
    print(f"\nDatabase saved to: {SILVER_DB.absolute()}\n")
    
    conn.close()


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    transform_bronze_to_silver()