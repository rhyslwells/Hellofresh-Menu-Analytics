"""
Verify include_recipes=1 actually returns recipes
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

print("Testing include_recipes=1...\n")

r = session.get(url, params={"page": 1, "per_page": 10, "include_recipes": 1})
print(f"Status: {r.status_code}\n")

data = r.json()
print(f"Total menus: {len(data.get('data', []))}\n")

for i, menu in enumerate(data.get('data', []), 1):
    print(f"Menu {i}:")
    print(f"  ID: {menu.get('id')}")
    print(f"  Week: {menu.get('year_week')}")
    print(f"  Start: {menu.get('start')}")
    print(f"  Has 'recipes' key: {'recipes' in menu}")
    if 'recipes' in menu:
        recipes = menu.get('recipes', [])
        print(f"  Recipes count: {len(recipes)}")
        if recipes:
            print(f"  First recipe: {recipes[0].get('name', 'N/A')}")
    print()