"""
HFRESH WEEKLY MENU INGESTION SCRIPT

Purpose
-------
Fetches the upcoming week's menu data from the hfresh API.
Designed to be run weekly (e.g., via cron) to build a historical dataset.

Architecture
------------
- Bronze layer: Raw JSON snapshots (append-only)
- Weekly cadence: Run once per week to capture next week's menu
- First run: Captures current + next week + reference data baseline
- Subsequent runs: Only next week's menu (reference data quarterly)

Usage
-----
1. Set HFRESH_API_TOKEN environment variable
2. Run weekly: python script.py
3. Data saved to ./bronze_data/YYYY-MM-DD/

Scheduling
----------
Recommended: Run every Monday morning
Example cron: 0 6 * * 1 /path/to/python /path/to/script.py
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any
import requests


# ======================
# Configuration
# ======================

BASE_URL = "https://api.hfresh.info"
LOCALE_COUNTRY = "en-GB"
PER_PAGE = 50
RATE_LIMIT_SLEEP_SECONDS = 1

# Bronze layer storage
BRONZE_DIR = Path("hfresh/bronze_data")

# Reference data refresh cadence (pull every N weeks)
REFERENCE_DATA_REFRESH_WEEKS = 13  # Quarterly


# ======================
# Session Setup
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
        payload = get_with_rate_limit(session, url, params)
        
        # Save raw snapshot
        save_bronze_snapshot(
            pull_date=pull_date,
            endpoint=endpoint_name,
            locale=locale,
            page=page,
            payload=payload,
        )
        
        all_records.extend(payload.get("data", []))
        
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
    Fetch menus within a specific date range.
    """
    url = f"{BASE_URL}/{locale}/menus"
    page = 1
    all_menus = []
    
    while True:
        params = {
            "page": page,
            "per_page": PER_PAGE,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }
        
        payload = get_with_rate_limit(session, url, params)
        
        save_bronze_snapshot(
            pull_date=pull_date,
            endpoint="menus",
            locale=locale,
            page=page,
            payload=payload,
        )
        
        all_menus.extend(payload.get("data", []))
        
        meta = payload.get("meta", {})
        if page >= meta.get("last_page", 1):
            break
            
        page += 1
        time.sleep(0.1)
    
    print(f"  ✓ menus ({start_date.date()} to {end_date.date()}): {len(all_menus)} records")
    return all_menus


# ======================
# Reference Data Logic
# ======================

def should_refresh_reference_data() -> bool:
    """
    Determine if reference data should be refreshed this week.
    
    Logic:
    - First run (no bronze data exists): YES
    - Every N weeks since last reference pull: YES
    - Otherwise: NO
    """
    if not BRONZE_DIR.exists():
        return True
    
    # Find most recent reference data pull
    reference_endpoints = ["recipes", "ingredients", "allergens", "tags", "labels"]
    latest_reference_pull = None
    
    for pull_dir in sorted(BRONZE_DIR.iterdir(), reverse=True):
        if not pull_dir.is_dir():
            continue
        
        # Check if any reference data exists in this pull
        for endpoint in reference_endpoints:
            if list(pull_dir.glob(f"{endpoint}_*.json")):
                latest_reference_pull = pull_dir.name
                break
        
        if latest_reference_pull:
            break
    
    if not latest_reference_pull:
        return True
    
    # Calculate weeks since last reference pull
    last_pull_date = datetime.strptime(latest_reference_pull, "%Y-%m-%d")
    weeks_since = (datetime.now() - last_pull_date).days // 7
    
    return weeks_since >= REFERENCE_DATA_REFRESH_WEEKS


# ======================
# Weekly Pull Orchestration
# ======================

def perform_weekly_pull(session: requests.Session) -> dict[str, Any]:
    """
    Execute this week's data pull.
    
    Fetches:
    - ALWAYS: Next week's menu
    - CONDITIONALLY: Reference data (first run or quarterly)
    """
    today = datetime.now()
    pull_date = today.strftime("%Y-%m-%d")
    
    # Calculate next week's date range
    # Next Monday
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7  # If today is Monday, get next Monday
    
    next_monday = today + timedelta(days=days_until_monday)
    next_sunday = next_monday + timedelta(days=6)
    
    print(f"\n{'='*60}")
    print(f"PULL DATE: {pull_date}")
    print(f"Target week: {next_monday.date()} to {next_sunday.date()}")
    print(f"{'='*60}")
    
    results = {}
    
    # ALWAYS: Fetch next week's menu
    results["menus"] = fetch_menus_for_week(
        session, LOCALE_COUNTRY, pull_date, next_monday, next_sunday
    )
    
    # CONDITIONAL: Reference data
    if should_refresh_reference_data():
        print(f"\n  → Refreshing reference data (quarterly snapshot)")
        
        results["recipes"] = fetch_paginated_endpoint(
            session, "recipes", LOCALE_COUNTRY, pull_date
        )
        
        for endpoint in ["ingredients", "allergens", "tags", "labels"]:
            results[endpoint] = fetch_paginated_endpoint(
                session, endpoint, LOCALE_COUNTRY, pull_date
            )
    else:
        print(f"\n  → Skipping reference data (not yet due)")
    
    return results


# ======================
# Entry Point
# ======================

def main() -> None:
    """Main execution."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Weekly Menu Ingestion                        ║
    ║  Bronze Layer: Raw API Snapshots                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available")
    
    # Check if this is first run
    is_first_run = not BRONZE_DIR.exists() or not list(BRONZE_DIR.iterdir())
    if is_first_run:
        print(f"✓ First run detected - will capture baseline reference data")
    
    # Execute weekly pull
    perform_weekly_pull(session)
    
    print(f"\n{'='*60}")
    print(f"✓ Weekly pull complete!")
    print(f"Bronze layer: {BRONZE_DIR.absolute()}")
    print(f"{'='*60}\n")
    
    # Show what was created
    total_pulls = len([d for d in BRONZE_DIR.iterdir() if d.is_dir()])
    print(f"Total weekly pulls: {total_pulls}")
    print(f"\nNext steps:")
    print(f"  1. Schedule this script to run weekly (e.g., cron)")
    print(f"  2. After several weeks, run silver layer transformation")
    print(f"  3. Build gold layer analytics")


if __name__ == "__main__":
    main()