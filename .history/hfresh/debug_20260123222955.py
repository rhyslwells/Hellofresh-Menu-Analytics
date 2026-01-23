"""
DEBUG SCRIPT - Inspect what the API is actually returning

Purpose
-------
Investigates why the API might be returning 224+ pages when
the website shows only 145 results.

Possible causes:
1. Pagination metadata is wrong (last_page field incorrect)
2. API includes more data than web UI (multiple locales, archived menus)
3. Date range interpretation differs
4. per_page parameter not being respected

Usage
-----
python debug_api.py
"""

import os
import requests
from datetime import datetime


BASE_URL = "https://api.hfresh.info"
LOCALE = "en-GB"


def debug_menu_endpoint():
    """Inspect the menu endpoint in detail."""
    api_token = os.environ.get("HFRESH_API_TOKEN")
    if not api_token:
        raise RuntimeError("HFRESH_API_TOKEN required")
    
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    })
    
    print("="*60)
    print("DEBUGGING MENU API ENDPOINT")
    print("="*60)
    
    # Test 1: Check what first page returns
    print("\n1. First page with default parameters:")
    print("-" * 60)
    
    url = f"{BASE_URL}/{LOCALE}/menus"
    params = {
        "page": 1,
        "per_page": 50,
        "start_date": "2025-01-17",
        "end_date": "2025-01-23",
    }
    
    response = session.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Inspect metadata
    meta = data.get("meta", {})
    print(f"Current page: {meta.get('current_page')}")
    print(f"Per page: {meta.get('per_page')}")
    print(f"Total items: {meta.get('total')}")
    print(f"Last page: {meta.get('last_page')}")
    print(f"Actual items in response: {len(data.get('data', []))}")
    
    # Show first menu item structure
    if data.get('data'):
        print(f"\nFirst menu sample:")
        first_menu = data['data'][0]
        print(f"  ID: {first_menu.get('id')}")
        print(f"  Name: {first_menu.get('name')}")
        print(f"  Start: {first_menu.get('startDate')}")
        print(f"  End: {first_menu.get('endDate')}")
        print(f"  Recipes count: {len(first_menu.get('recipes', []))}")
    
    # Test 2: Try without date filter
    print("\n\n2. First page WITHOUT date filter:")
    print("-" * 60)
    
    params_no_date = {
        "page": 1,
        "per_page": 50,
    }
    
    response2 = session.get(url, params=params_no_date)
    response2.raise_for_status()
    data2 = response2.json()
    
    meta2 = data2.get("meta", {})
    print(f"Total items (all time): {meta2.get('total')}")
    print(f"Last page (all time): {meta2.get('last_page')}")
    
    # Test 3: Check page 2 to see if pagination is real
    print("\n\n3. Checking page 2 to verify pagination:")
    print("-" * 60)
    
    params_page2 = {
        "page": 2,
        "per_page": 50,
        "start_date": "2025-01-17",
        "end_date": "2025-01-23",
    }
    
    response3 = session.get(url, params=params_page2)
    response3.raise_for_status()
    data3 = response3.json()
    
    print(f"Items on page 2: {len(data3.get('data', []))}")
    print(f"Page 2 meta: {data3.get('meta')}")
    
    # Test 4: Try with smaller per_page
    print("\n\n4. Testing with per_page=10:")
    print("-" * 60)
    
    params_small = {
        "page": 1,
        "per_page": 10,
        "start_date": "2025-01-17",
        "end_date": "2025-01-23",
    }
    
    response4 = session.get(url, params=params_small)
    response4.raise_for_status()
    data4 = response4.json()
    
    meta4 = data4.get("meta", {})
    print(f"Total items: {meta4.get('total')}")
    print(f"Per page: {meta4.get('per_page')}")
    print(f"Last page: {meta4.get('last_page')}")
    print(f"Items returned: {len(data4.get('data', []))}")
    print(f"Expected last page: {meta4.get('total') / 10}")
    
    # Test 5: Check what's in page 224
    print("\n\n5. Checking if page 224 actually has data:")
    print("-" * 60)
    
    params_high = {
        "page": 224,
        "per_page": 50,
        "start_date": "2025-01-17",
        "end_date": "2025-01-23",
    }
    
    try:
        response5 = session.get(url, params=params_high)
        response5.raise_for_status()
        data5 = response5.json()
        
        print(f"Items on page 224: {len(data5.get('data', []))}")
        if data5.get('data'):
            print(f"Page 224 meta: {data5.get('meta')}")
        else:
            print("Page 224 is EMPTY - pagination metadata likely wrong!")
    except Exception as e:
        print(f"Error accessing page 224: {e}")
    
    # Summary
    print("\n" + "="*60)
    print("DIAGNOSIS:")
    print("="*60)
    
    if meta.get('total') == 145 and meta.get('last_page') > 3:
        print("⚠️  LIKELY ISSUE: API pagination metadata is incorrect")
        print(f"   Expected pages: {(145 // 50) + 1} = 3 pages")
        print(f"   API claims: {meta.get('last_page')} pages")
        print("\n   SOLUTION: Stop pagination when data array is empty,")
        print("             not when page >= last_page")
    else:
        print("✓ Pagination appears correct")
        print(f"  Total items: {meta.get('total')}")
        print(f"  Expected pages: {(meta.get('total', 0) // 50) + 1}")


if __name__ == "__main__":
    debug_menu_endpoint()