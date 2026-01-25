"""
SQLite BRONZE LAYER INGESTION

Purpose
-------
Fetches weekly menus (with embedded recipes) from the HelloFresh API.
Writes raw JSON responses to SQLite database in the bronze layer.

Architecture
------------
- Bronze Layer: Raw API responses (immutable, append-only)
- SQLite: Local database, suitable for <1GB datasets
- Table: api_responses
- Full audit trail with ingestion timestamps

Output
------
Database: hfresh/hfresh.db
Table: api_responses
Columns:
  - pull_date (TEXT)
  - endpoint (TEXT)
  - locale (TEXT)
  - page_number (INTEGER)
  - payload (TEXT - JSON)
  - ingestion_timestamp (TEXT)

Usage
-----
From command line:
python scripts/1_bronze.py

With GitHub Actions:
env HELLOFRESH_API_KEY="..." python scripts/1_bronze.py
"""

import os
import json
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any
from pathlib import Path
import requests


# ======================
# Configuration
# ======================

BASE_URL = "https://api.hfresh.info"
LOCALE_COUNTRY = "en-GB"
PER_PAGE = 50
RATE_LIMIT_SLEEP_SECONDS = 1

# SQLite Configuration
DB_PATH = Path("hfresh/hfresh.db")


def get_api_key() -> str:
    """Retrieve API key from environment variable."""
    api_key = os.environ.get("HELLOFRESH_API_KEY")
    if not api_key:
        raise RuntimeError(
            "API key not found! Set HELLOFRESH_API_KEY environment variable."
        )
    return api_key


# ======================
# SQLite Setup
# ======================

def get_db_connection() -> sqlite3.Connection:
    """Get SQLite database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def write_to_bronze(
    conn: sqlite3.Connection,
    pull_date: str,
    endpoint: str,
    locale: str,
    page: int,
    payload: dict,
) -> None:
    """Write API response to Bronze table."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO api_responses 
            (pull_date, endpoint, locale, page_number, payload, ingestion_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                pull_date,
                endpoint,
                locale,
                page,
                json.dumps(payload),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    except Exception as e:
        print(f"Error writing to Bronze: {e}")
        raise


# ======================
# HTTP Session
# ======================

def create_session() -> requests.Session:
    """Create authenticated session with API key."""
    api_token = get_api_key()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    })
    return session


def get_with_rate_limit(
    session: requests.Session,
    url: str,
    params: dict | None = None,
) -> dict:
    """Perform a GET request with basic handling for rate limits and errors."""
    response = session.get(url, params=params)

    if response.status_code == 429:
        time.sleep(RATE_LIMIT_SLEEP_SECONDS)
        return get_with_rate_limit(session, url, params)

    response.raise_for_status()
    return response.json()


# ======================
# API Extraction Functions
# ======================

def fetch_paginated_endpoint(
    session: requests.Session,
    conn: sqlite3.Connection,
    endpoint_name: str,
    locale: str,
    pull_date: str,
    additional_params: dict | None = None,
    per_page: int = PER_PAGE,
) -> list[dict]:
    """
    Fetch all pages from an endpoint and write to Bronze table.
    Returns all records.
    """
    url = f"{BASE_URL}/{locale}/{endpoint_name}"
    page = 1
    all_records = []
    
    while True:
        params = {"page": page, "per_page": per_page}
        if additional_params:
            params.update(additional_params)
        
        payload = get_with_rate_limit(session, url, params)
        
        # Write to Bronze table
        write_to_bronze(
            conn=conn,
            pull_date=pull_date,
            endpoint=endpoint_name,
            locale=locale,
            page=page,
            payload=payload,
        )
        
        # Check if we got any data
        data = payload.get("data", [])
        if not data:
            break
        
        all_records.extend(data)
        
        # Check metadata
        meta = payload.get("meta", {})
        if page >= meta.get("last_page", 1):
            break
            
        page += 1
        time.sleep(0.1)  # Gentle rate limiting
    
    print(f"  ✓ {endpoint_name}: {len(all_records)} records, {page} pages")
    return all_records


def fetch_menus_for_week(
    session: requests.Session,
    conn: sqlite3.Connection,
    locale: str,
    pull_date: str,
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """
    Fetch menus for a specific week WITH recipes embedded.
    """
    print(f"  Fetching menus (with recipes embedded)")
    
    additional_params = {
        "include_recipes": 1,  # Must be integer, not string
    }
    
    menus = fetch_paginated_endpoint(
        session=session,
        conn=conn,
        endpoint_name="menus",
        locale=locale,
        pull_date=pull_date,
        additional_params=additional_params,
    )
    
    # Filter to requested date range (API may return all menus)
    filtered_menus = []
    for menu in menus:
        menu_start = menu.get("start")
        if menu_start:
            try:
                menu_date = datetime.strptime(menu_start, "%Y-%m-%d")
                if start_date <= menu_date <= end_date:
                    filtered_menus.append(menu)
            except (ValueError, TypeError):
                filtered_menus.append(menu)
        else:
            filtered_menus.append(menu)
    
    total_recipes = sum(len(menu.get("recipes", [])) for menu in filtered_menus)
    print(f"  → {len(filtered_menus)} menus (of {len(menus)} total) containing {total_recipes} recipes")
    
    return filtered_menus


# ======================
# Reference Data Logic
# ======================

def reference_data_exists(conn: sqlite3.Connection) -> bool:
    """Check if reference data has already been ingested."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) FROM api_responses 
            WHERE endpoint IN ('ingredients', 'allergens', 'tags', 'labels')
            """
        )
        result = cursor.fetchone()
        return result[0] > 0 if result else False
    except:
        return False


# ======================
# Weekly Pull Orchestration
# ======================

def perform_weekly_pull(session: requests.Session, conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Execute this week's data pull.
    
    Strategy:
    - ALWAYS: Fetch next week's menus (with recipes embedded)
    - FIRST RUN ONLY: Fetch reference data baseline
    """
    today = datetime.now()
    pull_date = today.strftime("%Y-%m-%d")
    
    # Calculate next week's date range
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    
    next_monday = today + timedelta(days=days_until_monday)
    next_sunday = next_monday + timedelta(days=6)
    
    print(f"\n{'='*60}")
    print(f"PULL DATE: {pull_date}")
    print(f"Target week: {next_monday.date()} to {next_sunday.date()}")
    print(f"{'='*60}\n")
    
    results = {}
    
    # ALWAYS: Fetch next week's menus with embedded recipes
    results["menus"] = fetch_menus_for_week(
        session, conn, LOCALE_COUNTRY, pull_date, next_monday, next_sunday
    )
    
    # FIRST RUN: Fetch reference data baseline
    has_ref_data = reference_data_exists(conn)
    if not has_ref_data:
        print(f"\n  → First run detected - fetching reference data baseline")
        
        for endpoint in ["ingredients", "allergens", "tags", "labels"]:
            results[endpoint] = fetch_paginated_endpoint(
                session, conn, endpoint, LOCALE_COUNTRY, pull_date
            )
    else:
        print(f"\n  → Reference data already exists, skipping")
    
    return results


# ======================
# Entry Point
# ======================

def main() -> None:
    """Main execution."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Bronze Layer Ingestion                       ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Initialize SQLite connection
    print("Connecting to database...")
    conn = get_db_connection()
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available")
    print(f"✓ Database: {DB_PATH}\n")
    
    # Execute weekly pull
    results = perform_weekly_pull(session, conn)
    
    print(f"\n{'='*60}")
    print(f"✓ Weekly pull complete!")
    print(f"{'='*60}\n")
    
    # Summary
    print("Captured this week:")
    for endpoint, data in results.items():
        print(f"  {endpoint:20} {len(data):>5} records")
    
    # Show row count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM api_responses")
    row_count = cursor.fetchone()[0]
    print(f"\nBronze layer total: {row_count} records\n")
    
    conn.close()


if __name__ == "__main__":
    main()