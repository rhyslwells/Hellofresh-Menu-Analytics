import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, List

DB_PATH = Path("hfresh/silver_data.db")


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS api_responses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pull_date TEXT,
        endpoint TEXT,
        locale TEXT,
        page_number INTEGER,
        payload TEXT,
        ingestion_timestamp TEXT
    );

    CREATE TABLE IF NOT EXISTS recipes (
        id TEXT PRIMARY KEY,
        name TEXT,
        headline TEXT,
        description TEXT,
        difficulty REAL,
        prep_time INTEGER,
        total_time INTEGER,
        serving_size INTEGER,
        cuisine TEXT,
        image_url TEXT,
        first_seen_date TEXT,
        last_seen_date TEXT,
        is_active INTEGER
    );

    CREATE TABLE IF NOT EXISTS ingredients (
        id TEXT PRIMARY KEY,
        name TEXT,
        family TEXT,
        is_active INTEGER
    );

    CREATE TABLE IF NOT EXISTS menu_recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        menu_id TEXT,
        recipe_id TEXT,
        position INTEGER,
        UNIQUE(menu_id, recipe_id)
    );

    CREATE TABLE IF NOT EXISTS recipe_ingredients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recipe_id TEXT,
        ingredient_id TEXT,
        quantity TEXT,
        unit TEXT,
        position INTEGER,
        UNIQUE(recipe_id, ingredient_id)
    );

    CREATE TABLE IF NOT EXISTS weekly_menu_metrics (
        week_start_date TEXT,
        locale TEXT,
        total_recipes INTEGER,
        unique_recipes INTEGER,
        new_recipes INTEGER,
        returning_recipes INTEGER,
        avg_difficulty REAL,
        avg_prep_time_minutes REAL,
        _created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS recipe_survival_metrics (
        recipe_id TEXT PRIMARY KEY,
        recipe_name TEXT,
        first_appearance_date TEXT,
        last_appearance_date TEXT,
        total_weeks_active INTEGER,
        consecutive_weeks_active INTEGER,
        weeks_since_last_seen INTEGER,
        is_currently_active INTEGER,
        _created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS ingredient_trends (
        ingredient_id TEXT,
        ingredient_name TEXT,
        week_start_date TEXT,
        recipe_count INTEGER,
        week_over_week_change INTEGER,
        popularity_rank INTEGER,
        _created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS menu_stability_metrics (
        week_start_date TEXT,
        locale TEXT,
        overlap_with_prev_week REAL,
        new_recipe_rate REAL,
        churned_recipe_rate REAL,
        recipes_retained INTEGER,
        recipes_added INTEGER,
        recipes_removed INTEGER,
        _created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS allergen_density (
        week_start_date TEXT,
        allergen_id TEXT,
        allergen_name TEXT,
        recipe_count INTEGER,
        percentage_of_menu REAL,
        _created_at TEXT
    );

    """)
    conn.commit()
    conn.close()


def write_api_response(pull_date: str, endpoint: str, locale: str, page:int, payload: Dict[str, Any], ingestion_timestamp: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO api_responses (pull_date, endpoint, locale, page_number, payload, ingestion_timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (pull_date, endpoint, locale, page, json.dumps(payload), ingestion_timestamp)
    )
    conn.commit()
    conn.close()


def fetch_api_responses(endpoint: str = None) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    if endpoint:
        cur.execute("SELECT * FROM api_responses WHERE endpoint = ? ORDER BY id", (endpoint,))
    else:
        cur.execute("SELECT * FROM api_responses ORDER BY id")
    rows = cur.fetchall()
    results = []
    for r in rows:
        d = dict(r)
        try:
            d['payload'] = json.loads(d['payload'])
        except Exception:
            d['payload'] = {}
        results.append(d)
    conn.close()
    return results


def upsert_recipe(recipe: Dict[str, Any], pull_date: str):
    conn = get_conn()
    cur = conn.cursor()
    rid = recipe.get('id')
    name = recipe.get('name')
    difficulty = recipe.get('difficulty')
    prep_time = recipe.get('prepTime') if recipe.get('prepTime') is not None else None
    total_time = recipe.get('totalTime') if recipe.get('totalTime') is not None else None
    cuisine = recipe.get('cuisine', {}).get('name') if recipe.get('cuisine') else None
    image_url = recipe.get('imagePath')
    headline = recipe.get('headline')
    description = recipe.get('description')
    serving_size = recipe.get('servingSize')

    # Check if exists
    cur.execute("SELECT id, first_seen_date FROM recipes WHERE id = ?", (rid,))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO recipes (id, name, headline, description, difficulty, prep_time, total_time, serving_size, cuisine, image_url, first_seen_date, last_seen_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (rid, name, headline, description, difficulty, prep_time, total_time, serving_size, cuisine, image_url, pull_date, pull_date, 1)
        )
    else:
        # Update last_seen_date and attributes
        cur.execute(
            "UPDATE recipes SET name = ?, headline = ?, description = ?, difficulty = ?, prep_time = ?, total_time = ?, serving_size = ?, cuisine = ?, image_url = ?, last_seen_date = ?, is_active = 1 WHERE id = ?",
            (name, headline, description, difficulty, prep_time, total_time, serving_size, cuisine, image_url, pull_date, rid)
        )
    conn.commit()
    conn.close()


def upsert_menu_recipe(menu_id: str, recipe_id: str, position: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO menu_recipes (menu_id, recipe_id, position) VALUES (?, ?, ?)", (menu_id, recipe_id, position))
        conn.commit()
    finally:
        conn.close()


def upsert_recipe_ingredient(recipe_id: str, ingredient_id: str, quantity: Any, unit: Any, position: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO recipe_ingredients (recipe_id, ingredient_id, quantity, unit, position) VALUES (?, ?, ?, ?, ?)", (recipe_id, ingredient_id, str(quantity) if quantity is not None else None, unit, position))
        conn.commit()
    finally:
        conn.close()
