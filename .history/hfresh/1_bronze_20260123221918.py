"""
HFRESH HISTORICAL MENU INGESTION SCRIPT

Purpose
-------
Fetches 2 years of weekly menus from the hfresh API and persists them
in a bronze-layer structure following the slowly-evolving upstream pattern.

Architecture
------------
- Bronze layer: Raw JSON snapshots (append-only)
- Weekly cadence simulation: Backfills historical weeks
- Captures: menus, recipes, ingredients, allergens, tags, labels
- Stores metadata: pull_date, endpoint, locale, page

Usage
-----
1. Set HFRESH_API_TOKEN environment variable
2. Run: python script.py
3. Data saved to ./bronze_data/YYYY-MM-DD/
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
RATE_LIMIT_SLEEP_SECONDS = 2

# Historical range: 2 years of weekly pulls
WEEKS_TO_BACKFILL = 1 
START_DATE = datetime.now() - timedelta(weeks=WEEKS_TO_BACKFILL)

# Bronze layer storage
BRONZE_DIR = Path("hfresh/bronze_data")


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
    Menus API supports date filtering.
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
# Weekly Pull Orchestration
# ======================

def perform_weekly_pull(
    session: requests.Session,
    pull_date: str,
    week_start: datetime,
    week_end: datetime,
) -> dict[str, Any]:
    """
    Execute a complete weekly data pull.
    
    Fetches:
    - Menus (for that week)
    - Recipes (current catalog)
    - Reference data (ingredients, allergens, tags, labels)
    """
    print(f"\n{'='*60}")
    print(f"PULL DATE: {pull_date}")
    print(f"Week: {week_start.date()} to {week_end.date()}")
    print(f"{'='*60}")
    
    results = {}
    
    # Menus (week-specific)
    results["menus"] = fetch_menus_for_week(
        session, LOCALE_COUNTRY, pull_date, week_start, week_end
    )
    
    # Recipes (full catalog)
    results["recipes"] = fetch_paginated_endpoint(
        session, "recipes", LOCALE_COUNTRY, pull_date
    )
    
    # Reference data
    for endpoint in ["ingredients", "allergens", "tags", "labels"]:
        results[endpoint] = fetch_paginated_endpoint(
            session, endpoint, LOCALE_COUNTRY, pull_date
        )
    
    return results


# ======================
# Historical Backfill
# ======================

def backfill_historical_weeks(session: requests.Session) -> None:
    """
    Simulate weekly pulls for the past 2 years.
    
    Strategy:
    - One "pull" per week
    - Each pull fetches menus for that week + current reference data
    - Pull date = Monday of each week
    """
    print(f"Starting backfill: {WEEKS_TO_BACKFILL} weeks from {START_DATE.date()}")
    
    current_week = START_DATE
    week_num = 1
    
    while current_week <= datetime.now():
        # Use Monday as canonical pull date
        pull_date = current_week.strftime("%Y-%m-%d")
        week_start = current_week
        week_end = current_week + timedelta(days=6)
        
        try:
            perform_weekly_pull(
                session=session,
                pull_date=pull_date,
                week_start=week_start,
                week_end=week_end,
            )
            
            print(f"✓ Week {week_num}/{WEEKS_TO_BACKFILL} complete")
            
        except Exception as e:
            print(f"✗ Week {week_num} failed: {e}")
            # Continue despite errors
        
        current_week += timedelta(weeks=1)
        week_num += 1
        
        # Be respectful to API
        time.sleep(1)


# ======================
# Entry Point
# ======================

def main() -> None:
    """Main execution."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Historical Menu Ingestion                    ║
    ║  Bronze Layer: Raw API Snapshots                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Create session
    session = create_session()
    
    # Verify connectivity
    countries = get_with_rate_limit(session, f"{BASE_URL}/countries")
    print(f"✓ API connected: {len(countries.get('data', []))} countries available\n")
    
    # Execute backfill
    backfill_historical_weeks(session)
    
    print(f"\n{'='*60}")
    print(f"✓ Backfill complete!")
    print(f"Bronze layer saved to: {BRONZE_DIR.absolute()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()