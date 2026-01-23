"""
Test different parameter combinations for the menus endpoint
"""
import os
import requests

BASE_URL = "https://api.hfresh.info"
LOCALE = "en-GB"

api_token = os.environ.get("HFRESH_API_TOKEN")
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {api_token}",
    "Accept": "application/json",
})

url = f"{BASE_URL}/{LOCALE}/menus"

print("Testing menu endpoint parameters...\n")

# Test 1: No parameters
print("1. No parameters:")
try:
    r = session.get(url, params={"page": 1, "per_page": 10})
    print(f"   ✓ Status: {r.status_code}")
    data = r.json()
    print(f"   Menus: {len(data.get('data', []))}")
    if data.get('data'):
        first = data['data'][0]
        print(f"   First menu has 'recipes' key: {'recipes' in first}")
        print(f"   Keys: {list(first.keys())}")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Test 2: include_recipes=true (string)
print("\n2. include_recipes=true (string):")
try:
    r = session.get(url, params={"page": 1, "per_page": 10, "include_recipes": "true"})
    print(f"   ✓ Status: {r.status_code}")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Test 3: include_recipes=1 (number)
print("\n3. include_recipes=1:")
try:
    r = session.get(url, params={"page": 1, "per_page": 10, "include_recipes": 1})
    print(f"   ✓ Status: {r.status_code}")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Test 4: with[]=recipes
print("\n4. with[]=recipes:")
try:
    r = session.get(url, params={"page": 1, "per_page": 10, "with[]": "recipes"})
    print(f"   ✓ Status: {r.status_code}")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Test 5: Check what params the docs actually show
print("\n5. Checking individual menu endpoint for comparison:")
try:
    # Get a menu ID first
    r = session.get(url, params={"page": 1, "per_page": 1})
    menu_id = r.json()['data'][0]['id']
    
    # Fetch individual menu
    r2 = session.get(f"{url}/{menu_id}")
    print(f"   ✓ Status: {r2.status_code}")
    menu_detail = r2.json()
    print(f"   Menu detail has 'recipes' key: {'recipes' in menu_detail}")
    if 'recipes' in menu_detail:
        print(f"   Recipes count: {len(menu_detail.get('recipes', []))}")
except Exception as e:
    print(f"   ✗ Error: {e}")