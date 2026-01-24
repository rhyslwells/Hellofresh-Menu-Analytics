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
    
    # Enable Delta Lake
    spark.sql("SET spark.databricks.delta.preview.enabled = true")
    
    try:
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
        print("✓ Bronze schema initialized")
        return spark
    except Exception as e:
        print(f"⚠️  Schema initialization warning: {e}")
        print("   Continuing - table may already exist")
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
# HTTP Session
# ======================

def create_session() -> requests.Session:
    """Create authenticated session."""
    api_token = os.environ.get("HFRESH_API_TOKEN")
    if not api_token:
        raise RuntimeError("HFRESH_API_TOKEN environment variable required")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    })
    return session


# ======================
# API Extraction Functions
# ======================

def fetch_paginated_endpoint(
    session: requests.Session,
    spark: SparkSession,
    endpoint_name: str,
    locale: str,
    pull_date: str,
    additional_params: dict | None = None,
    per_page: int = PER_PAGE,
) -> list[dict]:
    """
    Fetch all pages from an endpoint and write to Bronze Delta table.
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
        
        # Write to Bronze Delta table
        write_to_bronze(
            spark=spark,
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
    spark: SparkSession,
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
        spark=spark,
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

def reference_data_exists(spark: SparkSession) -> bool:
    """Check if reference data has already been ingested."""
    try:
        spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE} WHERE endpoint IN ('ingredients', 'allergens', 'tags', 'labels')")
        result = spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE} WHERE endpoint IN ('ingredients', 'allergens', 'tags', 'labels')").collect()
        return result[0][0] > 0
    except:
        return False


# ======================
# Weekly Pull Orchestration
# ======================

def perform_weekly_pull(session: requests.Session, spark: SparkSession) -> dict[str, Any]:
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
        session, spark, LOCALE_COUNTRY, pull_date, next_monday, next_sunday
    )
    
    # FIRST RUN: Fetch reference data baseline
    has_ref_data = reference_data_exists(spark)
    if not has_ref_data:
        print(f"\n  → First run detected - fetching reference data baseline")
        
        for endpoint in ["ingredients", "allergens", "tags", "labels"]:
            results[endpoint] = fetch_paginated_endpoint(
                session, spark, endpoint, LOCALE_COUNTRY, pull_date
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
    ║  Databricks Delta Tables                                 ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    if not IN_DATABRICKS:
        print("⚠️  Warning: Not running in Databricks environment")
        print("   This notebook is designed for Databricks clusters")
        print("   For local development, use the legacy SQLite version\n")
    
    # Initialize Databricks
    print("Initializing Databricks...")
    spark = init_databricks()
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available")
    print(f"✓ Bronze table: {BRONZE_TABLE}\n")
    
    # Execute weekly pull
    results = perform_weekly_pull(session, spark)
    
    print(f"\n{'='*60}")
    print(f"✓ Weekly pull complete!")
    print(f"{'='*60}\n")
    
    # Summary
    print("Captured this week:")
    for endpoint, data in results.items():
        print(f"  {endpoint:20} {len(data):>5} records")
    
    # Show row count
    row_count = spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE}").collect()[0][0]
    print(f"\nBronze layer total: {row_count} records\n")


if __name__ == "__main__":
    main()