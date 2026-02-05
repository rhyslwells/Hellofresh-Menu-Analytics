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
import argparse
from datetime import datetime, timedelta, timezone
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


# ======================
# Temporal Utilities
# ======================

def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO date string (YYYY-MM-DD) to datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def get_week_bounds(target_date: datetime) -> tuple[datetime, datetime]:
    """
    Get Monday-Sunday bounds for the week containing target_date.
    
    Args:
        target_date: Any date in the target week
        
    Returns:
        (monday, sunday) as datetime objects at start of day
    """
    # Get Monday of this week
    days_since_monday = target_date.weekday()
    monday = target_date - timedelta(days=days_since_monday)
    sunday = monday + timedelta(days=6)
    
    # Reset to start of day
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return monday, sunday


def parse_temporal_arguments(
    week: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[datetime, datetime]:
    """
    Parse temporal arguments and return (start_date, end_date).
    
    Args:
        week: ISO week in format 'YYYY-Www' (e.g., '2026-W05'), or a date (YYYY-MM-DD)
        start_date: Start date (YYYY-MM-DD), inclusive
        end_date: End date (YYYY-MM-DD), inclusive
        
    Returns:
        (start_datetime, end_datetime) tuple
        
    Raises:
        ValueError: If arguments are invalid
    """
    # If week is provided, use it to set date range
    if week:
        # Check if it's ISO week format (YYYY-Www)
        if "-W" in week:
            try:
                year, week_num = week.split("-W")
                year = int(year)
                week_num = int(week_num)
                
                # Calculate Monday of week
                jan4 = datetime(year, 1, 4)
                monday_week1 = jan4 - timedelta(days=jan4.weekday())
                monday_target_week = monday_week1 + timedelta(weeks=week_num - 1)
                
                start = monday_target_week.replace(hour=0, minute=0, second=0, microsecond=0)
                end = (monday_target_week + timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
                
                return start, end
            except (ValueError, IndexError):
                raise ValueError(
                    f"Invalid ISO week format: {week}. Expected YYYY-Www (e.g., 2026-W05)"
                )
        else:
            # Treat as a date
            target_date = parse_iso_date(week)
            start, end = get_week_bounds(target_date)
            return start, end
    
    # If explicit date range provided
    if start_date and end_date:
        start = parse_iso_date(start_date)
        end = parse_iso_date(end_date)
        
        if start > end:
            raise ValueError(f"start_date must be before end_date: {start_date} > {end_date}")
        
        return start, end
    
    # Default: next week (legacy behavior)
    today = datetime.now()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    
    next_monday = today + timedelta(days=days_until_monday)
    next_sunday = next_monday + timedelta(days=6)
    
    return next_monday, next_sunday


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
                datetime.now(timezone.utc).isoformat(),
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

def perform_weekly_pull(
    session: requests.Session,
    conn: sqlite3.Connection,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> dict[str, Any]:
    """
    Execute data pull for specified date range.
    
    Strategy:
    - ALWAYS: Fetch menus for date range (with recipes embedded)
    - FIRST RUN ONLY: Fetch reference data baseline
    
    Args:
        session: Requests session
        conn: SQLite connection
        start_date: Start date (inclusive). If None, defaults to next week
        end_date: End date (inclusive). If None, defaults to next week
    """
    today = datetime.now()
    pull_date = today.strftime("%Y-%m-%d")
    
    # Use provided dates or calculate next week
    if start_date is None or end_date is None:
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        
        start_date = today + timedelta(days=days_until_monday)
        end_date = start_date + timedelta(days=6)
    
    print(f"\n{'='*60}")
    print(f"PULL DATE: {pull_date}")
    print(f"Target week: {start_date.date()} to {end_date.date()}")
    print(f"{'='*60}\n")
    
    results = {}
    
    # ALWAYS: Fetch menus for the specified date range with embedded recipes
    results["menus"] = fetch_menus_for_week(
        session, conn, LOCALE_COUNTRY, pull_date, start_date, end_date
    )

    # Fetch full recipe details for every recipe referenced in menus
    # so downstream Silver transformation can populate bridge tables.
    def fetch_recipe_detail(session: requests.Session, locale: str, recipe_id: str) -> dict:
        url = f"{BASE_URL}/{locale}/recipes/{recipe_id}"
        return get_with_rate_limit(session, url)

    def fetch_and_store_recipe_details(session: requests.Session, conn: sqlite3.Connection, recipe_ids: list[str], locale: str, pull_date: str) -> None:
        unique_ids = sorted(set(recipe_ids))
        print(f"\n  Fetching {len(unique_ids)} recipe details to bronze")
        for rid in unique_ids:
            try:
                payload = fetch_recipe_detail(session, locale, rid)
                # Store each recipe detail under the `recipes` endpoint in bronze
                write_to_bronze(conn, pull_date, "recipes", locale, 1, payload)
                time.sleep(0.05)
            except Exception as e:
                print(f"  ⚠️ Error fetching recipe {rid}: {e}")

    # collect recipe ids from menus
    recipe_ids = []
    for menu in results.get("menus", []):
        for r in menu.get("recipes", []):
            if r.get("id"):
                recipe_ids.append(r.get("id"))

    if recipe_ids:
        fetch_details = os.environ.get('FETCH_RECIPE_DETAILS', '0').lower() in ('1', 'true', 'yes')
        if fetch_details:
            fetch_and_store_recipe_details(session, conn, recipe_ids, LOCALE_COUNTRY, pull_date)
        else:
            print(f"  → Skipping per-recipe detail fetch (set FETCH_RECIPE_DETAILS=1 to enable)")
    
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

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="HelloFresh Bronze Layer Ingestion - SQLite Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest next week (default behavior)
  python scripts/1_bronze.py
  
  # Ingest a specific week using ISO week format
  python scripts/1_bronze.py --week 2026-W05
  
  # Ingest a specific week using a date from that week
  python scripts/1_bronze.py --week 2026-01-15
  
  # Ingest a specific date range
  python scripts/1_bronze.py --start-date 2026-01-01 --end-date 2026-01-31
  
  # With recipe details fetched
  python scripts/1_bronze.py --week 2026-W05 --fetch-recipes
        """,
    )
    
    parser.add_argument(
        "--week",
        type=str,
        default=None,
        help="Target week in ISO format (YYYY-Www) or any date in the week (YYYY-MM-DD). "
             "Example: 2026-W05 or 2026-01-15",
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for data range (YYYY-MM-DD). Must be used with --end-date",
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for data range (YYYY-MM-DD). Must be used with --start-date",
    )
    
    parser.add_argument(
        "--fetch-recipes",
        action="store_true",
        help="Fetch per-recipe details (slower but more complete data)",
    )
    
    return parser.parse_args()


def main() -> None:
    """Main execution."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Bronze Layer Ingestion                       ║
    ║  SQLite Database                                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Parse CLI arguments
    args = parse_arguments()
    
    # Validate and parse temporal arguments
    try:
        if args.week and (args.start_date or args.end_date):
            raise ValueError(
                "Cannot use --week with --start-date/--end-date. Choose one."
            )
        
        if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
            raise ValueError(
                "--start-date and --end-date must be used together"
            )
        
        start_date, end_date = parse_temporal_arguments(
            week=args.week,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    except ValueError as e:
        print(f"Error: {e}")
        print("\nRun 'python scripts/1_bronze.py --help' for usage.")
        exit(1)
    
    # Set recipe fetch environment variable if requested
    if args.fetch_recipes:
        os.environ['FETCH_RECIPE_DETAILS'] = '1'
    
    # Initialize SQLite connection
    print("Connecting to database...")
    conn = get_db_connection()
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available")
    print(f"✓ Database: {DB_PATH}\n")
    
    # Execute pull for specified date range
    results = perform_weekly_pull(session, conn, start_date, end_date)
    
    print(f"\n{'='*60}")
    print(f"✓ Pull complete!")
    print(f"{'='*60}\n")
    
    # Summary
    print("Captured:")
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