"""
HFRESH API INGESTION SCRIPT

Purpose
-------
Connects to the hfresh REST API using Bearer token authentication,
retrieves paginated recipe data for a given locale, and returns all
records as a Python list of dictionaries.

Key Characteristics
-------------------
- Uses requests.Session for connection reuse
- Handles pagination via the meta.last_page field
- Applies basic rate-limit protection
- Raises explicit errors for failed requests
- Designed to be extended for persistence (e.g. pandas, Delta, SQL)

Configuration
-------------
- API token must be supplied via environment variable: HFRESH_API_TOKEN
- Base URL: https://api.hfresh.info
"""

# ======================
# Imports
# ======================

import os
import time
import requests


# ======================
# Configuration
# ======================

BASE_URL = "https://api.hfresh.info"
LOCALE_COUNTRY = "de-DE"
PER_PAGE = 50
RATE_LIMIT_SLEEP_SECONDS = 60


# ======================
# Session Setup
# ======================

def create_session() -> requests.Session:
    """
    Create and configure a requests session with authentication headers.
    """
    api_token = os.environ.get("HFRESH_API_TOKEN")
    if not api_token:
        raise RuntimeError("Environment variable HFRESH_API_TOKEN is not set")

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
    """
    Perform a GET request with basic handling for rate limits and errors.
    """
    response = session.get(url, params=params)

    if response.status_code == 429:
        time.sleep(RATE_LIMIT_SLEEP_SECONDS)
        return get_with_rate_limit(session, url, params)

    response.raise_for_status()
    return response.json()


# ======================
# API Functions
# ======================

def fetch_recipes_page(
    session: requests.Session,
    page: int,
    per_page: int,
    locale_country: str,
) -> dict:
    """
    Fetch a single page of recipe results.
    """
    url = f"{BASE_URL}/{locale_country}/recipes"
    params = {
        "page": page,
        "per_page": per_page,
    }

    return get_with_rate_limit(session, url, params)


def fetch_all_recipes(
    session: requests.Session,
    locale_country: str,
    per_page: int,
) -> list[dict]:
    """
    Fetch all recipe records across all pages.
    """
    page = 1
    results: list[dict] = []

    while True:
        payload = fetch_recipes_page(
            session=session,
            page=page,
            per_page=per_page,
            locale_country=locale_country,
        )

        results.extend(payload["data"])

        meta = payload["meta"]
        if page >= meta["last_page"]:
            break

        page += 1

    return results


# ======================
# Entry Point
# ======================

def main() -> None:
    """
    Script entry point.
    """
    session = create_session()

    recipes = fetch_all_recipes(
        session=session,
        locale_country=LOCALE_COUNTRY,
        per_page=PER_PAGE,
    )

    print(f"Fetched {len(recipes)} recipes")


if __name__ == "__main__":
    main()
