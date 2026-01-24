"""
DATABRICKS BRONZE LAYER INGESTION

Purpose
-------
Fetches weekly menus (with embedded recipes) from the HelloFresh API.
Writes raw JSON responses to Databricks Delta tables in the bronze layer.

Architecture
------------
- Bronze Layer: Raw API responses (immutable, append-only)
- Delta Tables: ACID-compliant, versioned, scalable
- Partitioned by: pull_date, endpoint
- Full audit trail with metadata

Output
------
Database: hfresh_bronze
Table: api_responses
Columns:
  - pull_date (DATE, partition key)
  - endpoint (STRING, partition key)
  - locale (STRING)
  - page_number (INT)
  - payload (STRING - JSON)
  - ingestion_timestamp (TIMESTAMP)

Usage
-----
In Databricks notebook:
%run ./1_bronze

Or parameterized:
dbutils.notebook.run("1_bronze", 60, {"pull_date": "2026-01-24"})
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import Any
import requests

# Databricks imports (when running in Databricks)
try:
    from databricks.sql import sql
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    IN_DATABRICKS = True
except ImportError:
    IN_DATABRICKS = False


# ======================
# Configuration
# ======================

BASE_URL = "https://api.hfresh.info"
LOCALE_COUNTRY = "en-GB"
PER_PAGE = 50
RATE_LIMIT_SLEEP_SECONDS = 1

# Databricks Configuration
DATABRICKS_CATALOG = "hfresh_catalog"  # Or 'main' for default catalog
BRONZE_SCHEMA = "hfresh_bronze"
BRONZE_TABLE = f"{DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.api_responses"

# For local development (fallback)
BRONZE_DIR_LOCAL = None  # Disabled - use Databricks only


# ======================
# Databricks Setup
# ======================

def init_databricks() -> SparkSession:
    """Initialize Spark session and create schema if needed."""
    spark = SparkSession.builder.appName("hfresh_bronze_ingestion").getOrCreate()
    
    # Create catalog and schema (idempotent)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {DATABRICKS_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}")
    
    # Create bronze table if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            pull_date DATE,
            endpoint STRING,
            locale STRING,
            page_number INT,
            payload STRING,
            ingestion_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (pull_date, endpoint)
    """)
    
    return spark


def write_to_bronze(
    spark: SparkSession,
    pull_date: str,
    endpoint: str,
    locale: str,
    page: int,
    payload: dict,
) -> None:
    """Write API response to Bronze Delta table."""
    rows = [(
        pull_date,
        endpoint,
        locale,
        page,
        json.dumps(payload),
        datetime.utcnow().isoformat()
    )]
    
    schema = StructType([
        StructField("pull_date", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("locale", StringType(), False),
        StructField("page_number", IntegerType(), False),
        StructField("payload", StringType(), False),
        StructField("ingestion_timestamp", StringType(), False)
    ])
    
    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("pull_date", F.to_date(F.col("pull_date")))
    df = df.withColumn("ingestion_timestamp", F.to_timestamp(F.col("ingestion_timestamp")))
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .insertInto(BRONZE_TABLE)


# ======================
# HTTP Utilities
# ======================

def get_with_rate_limit(
    session: requests.Session,
    url: str,
    params: dict | None = None,
) -> dict:
    """GET request with rate limit handling."""
    response = session.get(url, params=params)

    if response.status_code == 429:
        print(f"Rate limited. Sleeping {RATE_LIMIT_SLEEP_SECONDS}s...")
        time.sleep(RATE_LIMIT_SLEEP_SECONDS)
        return get_with_rate_limit(session, url, params)

    response.raise_for_status()
    return response.json()


# ======================
# Bronze Layer Persistence
# ======================

def save_bronze_snapshot(
    pull_date: str,
    endpoint: str,
    locale: str,
    page: int,
    payload: dict,
) -> None:
    """
    Save raw API response to bronze layer.
    
    Structure: bronze_data/YYYY-MM-DD/endpoint_page_N.json
    """
    pull_dir = BRONZE_DIR / pull_date
    pull_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{endpoint}_page_{page}.json"
    filepath = pull_dir / filename
    
    metadata = {
        "pull_date": pull_date,
        "endpoint": endpoint,
        "locale": locale,
        "page": page,
        "pulled_at_utc": datetime.utcnow().isoformat(),
    }
    
    snapshot = {
        "metadata": metadata,
        "payload": payload,
    }
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)


# ======================
# API Extraction Functions
# ======================

def fetch_paginated_endpoint(
    session: requests.Session,
    endpoint_name: str,
    locale: str,
    pull_date: str,
    additional_params: dict | None = None,
    per_page: int = PER_PAGE,
) -> list[dict]:
    """
    Fetch all pages from an endpoint and save to bronze layer.
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
        
        # Save raw snapshot
        save_bronze_snapshot(
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
    locale: str,
    pull_date: str,
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """
    Fetch menus for a specific week WITH recipes embedded.
    
    This is the efficient approach - one API call gets:
    - Menu metadata
    - All recipes in that menu
    - Recipe details (ingredients, allergens, etc.)
    
    Note: The API doesn't seem to support date filtering reliably,
    so we fetch all menus and may get menus outside our date range.
    """
    print(f"  Fetching menus (with recipes embedded)")
    
    additional_params = {
        "include_recipes": 1,  # Must be integer, not string
    }
    
    menus = fetch_paginated_endpoint(
        session=session,
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
                # Include if we can't parse the date
                filtered_menus.append(menu)
        else:
            # Include if no start date
            filtered_menus.append(menu)
    
    # Count embedded recipes
    total_recipes = sum(len(menu.get("recipes", [])) for menu in filtered_menus)
    print(f"  → {len(filtered_menus)} menus (of {len(menus)} total) containing {total_recipes} recipes")
    
    return filtered_menus


# ======================
# Reference Data Logic
# ======================

def is_first_run() -> bool:
    """Check if this is the first time the script is being run."""
    if not BRONZE_DIR.exists():
        return True
    
    # Check if any reference data exists
    for pull_dir in BRONZE_DIR.iterdir():
        if not pull_dir.is_dir():
            continue
        if list(pull_dir.glob("ingredients_*.json")) or \
           list(pull_dir.glob("allergens_*.json")):
            return False
    
    return True


# ======================
# Weekly Pull Orchestration
# ======================

def perform_weekly_pull(session: requests.Session) -> dict[str, Any]:
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
        days_until_monday = 7  # If today is Monday, get next Monday
    
    next_monday = today + timedelta(days=days_until_monday)
    next_sunday = next_monday + timedelta(days=6)
    
    print(f"\n{'='*60}")
    print(f"PULL DATE: {pull_date}")
    print(f"Target week: {next_monday.date()} to {next_sunday.date()}")
    print(f"{'='*60}\n")
    
    results = {}
    
    # ALWAYS: Fetch next week's menus with embedded recipes
    results["menus"] = fetch_menus_for_week(
        session, LOCALE_COUNTRY, pull_date, next_monday, next_sunday
    )
    
    # FIRST RUN: Fetch reference data baseline
    if is_first_run():
        print(f"\n  → First run detected - fetching reference data baseline")
        
        # These provide lookup tables for IDs referenced in recipes
        for endpoint in ["ingredients", "allergens", "tags", "labels"]:
            results[endpoint] = fetch_paginated_endpoint(
                session, endpoint, LOCALE_COUNTRY, pull_date
            )
    else:
        print(f"\n  → Skipping reference data (already exists)")
    
    return results


# ======================
# Entry Point
# ======================

def main() -> None:
    """Main execution."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Weekly Menu Ingestion                        ║
    ║  Menus + Embedded Recipes                                ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available")
    
    # Check run status
    if is_first_run():
        print(f"✓ First run - will fetch reference data baseline\n")
    else:
        print(f"✓ Subsequent run - menus only\n")
    
    # Execute weekly pull
    results = perform_weekly_pull(session)
    
    print(f"\n{'='*60}")
    print(f"✓ Weekly pull complete!")
    print(f"{'='*60}\n")
    
    # Summary
    print("Captured this week:")
    for endpoint, data in results.items():
        print(f"  {endpoint:20} {len(data):>5} records")
    
    total_pulls = len([d for d in BRONZE_DIR.iterdir() if d.is_dir()])
    print(f"\nTotal weekly snapshots: {total_pulls}")
    print(f"Bronze layer: {BRONZE_DIR.absolute()}\n")


if __name__ == "__main__":
    main()