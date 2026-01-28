"""
Initialize SQLite database and schema for HelloFresh pipeline.

Run once before executing the pipeline:
    python scripts/init_sqlite.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("hfresh") / "hfresh.db"


def init_database():
    """Create SQLite database and schema."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Enable foreign keys
    cur.execute("PRAGMA foreign_keys = ON")
    
    print("\n📦 Creating schema...")
    
    # Bronze layer
    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pull_date TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            locale TEXT,
            page_number INTEGER,
            payload TEXT,
            ingestion_timestamp TEXT
        )
    """)
    print("  ✓ api_responses")
    
    # Silver layer - entities with SCD Type 2
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            id TEXT PRIMARY KEY,
            name TEXT,
            headline TEXT,
            description TEXT,
            difficulty REAL,
            prep_time TEXT,
            total_time TEXT,
            serving_size TEXT,
            cuisine TEXT,
            image_url TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ recipes")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id TEXT PRIMARY KEY,
            name TEXT,
            family TEXT,
            type TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ ingredients")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS allergens (
            allergen_id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,
            icon_url TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ allergens")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            tag_id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,
            icon_url TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ tags")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS labels (
            label_id TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ labels")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS menus (
            id TEXT PRIMARY KEY,
            url TEXT,
            year_week TEXT,
            start_date TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT
        )
    """)
    print("  ✓ menus")
    
    # Bridge tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_id TEXT NOT NULL,
            ingredient_id TEXT NOT NULL,
            quantity TEXT,
            unit TEXT,
            position INTEGER,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT,
            PRIMARY KEY (recipe_id, ingredient_id)
        )
    """)
    print("  ✓ recipe_ingredients")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_allergens (
            recipe_id TEXT NOT NULL,
            allergen_id TEXT NOT NULL,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT,
            PRIMARY KEY (recipe_id, allergen_id)
        )
    """)
    print("  ✓ recipe_allergens")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_tags (
            recipe_id TEXT NOT NULL,
            tag_id TEXT NOT NULL,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT,
            PRIMARY KEY (recipe_id, tag_id)
        )
    """)
    print("  ✓ recipe_tags")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_labels (
            recipe_id TEXT NOT NULL,
            label_id TEXT NOT NULL,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT,
            PRIMARY KEY (recipe_id, label_id)
        )
    """)
    print("  ✓ recipe_labels")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS menu_recipes (
            menu_id TEXT NOT NULL,
            recipe_id TEXT NOT NULL,
            position INTEGER,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            _ingestion_ts TEXT,
            PRIMARY KEY (menu_id, recipe_id)
        )
    """)
    print("  ✓ menu_recipes")
    
    # Gold layer - analytical tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_menu_metrics (
            week_start_date TEXT PRIMARY KEY,
            locale TEXT,
            total_recipes INTEGER,
            unique_recipes INTEGER,
            new_recipes INTEGER,
            returning_recipes INTEGER,
            avg_difficulty REAL,
            avg_prep_time_minutes REAL,
            created_at TEXT
        )
    """)
    print("  ✓ weekly_menu_metrics")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_survival_metrics (
            recipe_id TEXT PRIMARY KEY,
            recipe_name TEXT,
            first_appearance_date TEXT,
            last_appearance_date TEXT,
            total_weeks_active INTEGER,
            consecutive_weeks_active INTEGER,
            weeks_since_last_seen INTEGER,
            is_currently_active INTEGER,
            created_at TEXT
        )
    """)
    print("  ✓ recipe_survival_metrics")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredient_trends (
            ingredient_id TEXT,
            ingredient_name TEXT,
            week_start_date TEXT,
            recipe_count INTEGER,
            week_over_week_change INTEGER,
            popularity_rank INTEGER,
            created_at TEXT,
            PRIMARY KEY (ingredient_id, week_start_date)
        )
    """)
    print("  ✓ ingredient_trends")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS menu_stability_metrics (
            week_start_date TEXT PRIMARY KEY,
            locale TEXT,
            overlap_with_prev_week REAL,
            new_recipe_rate REAL,
            churned_recipe_rate REAL,
            recipes_retained INTEGER,
            recipes_added INTEGER,
            recipes_removed INTEGER,
            created_at TEXT
        )
    """)
    print("  ✓ menu_stability_metrics")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS allergen_density (
            allergen_id TEXT,
            allergen_name TEXT,
            week_start_date TEXT,
            recipe_count INTEGER,
            percentage_of_menu REAL,
            created_at TEXT,
            PRIMARY KEY (allergen_id, week_start_date)
        )
    """)
    print("  ✓ allergen_density")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Database initialized at {DB_PATH}\n")


if __name__ == "__main__":
    init_database()