"""
HFRESH EXPLORATORY QUERIES

Purpose
-------
Run interesting SQL queries against the silver layer to explore your data.
Shows insights about recipes, menus, ingredients, and patterns.

Usage
-----
python 4_explore.py
"""

import sqlite3
from pathlib import Path


# ======================
# Configuration
# ======================

SILVER_DB = Path("hfresh/silver_data.db")


# ======================
# Query Definitions
# ======================

QUERIES = {
    "top_recipes": {
        "title": "Top 10 Most Common Recipes",
        "description": "Recipes that appear most frequently across menus",
        "sql": """
            SELECT 
                r.name,
                COUNT(DISTINCT mr.menu_id) as menu_appearances,
                r.difficulty,
                r.prep_time,
                r.cuisine
            FROM recipes r
            JOIN menu_recipes mr ON r.recipe_id = mr.recipe_id
            GROUP BY r.recipe_id
            ORDER BY menu_appearances DESC
            LIMIT 10
        """
    },
    
    "popular_ingredients": {
        "title": "Top 20 Most Used Ingredients",
        "description": "Ingredients appearing in the most recipes",
        "sql": """
            SELECT 
                i.name,
                i.family,
                COUNT(DISTINCT ri.recipe_id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_count
            FROM ingredients i
            JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id
            JOIN menu_recipes mr ON ri.recipe_id = mr.recipe_id
            GROUP BY i.ingredient_id
            ORDER BY recipe_count DESC
            LIMIT 20
        """
    },
    
    "menu_diversity": {
        "title": "Menu Diversity Analysis",
        "description": "How diverse is each week's menu?",
        "sql": """
            SELECT 
                m.start_date,
                m.year_week,
                COUNT(DISTINCT mr.recipe_id) as unique_recipes,
                COUNT(DISTINCT r.cuisine) as unique_cuisines,
                COUNT(DISTINCT rt.tag_id) as unique_tags,
                AVG(r.difficulty) as avg_difficulty
            FROM menus m
            JOIN menu_recipes mr ON m.menu_id = mr.menu_id
            JOIN recipes r ON mr.recipe_id = r.recipe_id
            LEFT JOIN recipe_tags rt ON r.recipe_id = rt.recipe_id
            GROUP BY m.menu_id
            ORDER BY m.start_date DESC
        """
    },
    
    "allergen_coverage": {
        "title": "Most Common Allergens",
        "description": "Allergens and how many recipes contain them",
        "sql": """
            SELECT 
                a.name,
                a.type,
                COUNT(DISTINCT ra.recipe_id) as recipe_count,
                ROUND(COUNT(DISTINCT ra.recipe_id) * 100.0 / 
                      (SELECT COUNT(DISTINCT recipe_id) FROM recipes), 2) as pct_of_recipes
            FROM allergens a
            JOIN recipe_allergens ra ON a.allergen_id = ra.allergen_id
            GROUP BY a.allergen_id
            ORDER BY recipe_count DESC
        """
    },
    
    "cuisine_distribution": {
        "title": "Cuisine Distribution",
        "description": "What cuisines are represented?",
        "sql": """
            SELECT 
                COALESCE(r.cuisine, 'Unknown') as cuisine,
                COUNT(DISTINCT r.recipe_id) as recipe_count,
                COUNT(DISTINCT mr.menu_id) as menu_appearances
            FROM recipes r
            LEFT JOIN menu_recipes mr ON r.recipe_id = mr.recipe_id
            GROUP BY cuisine
            ORDER BY recipe_count DESC
        """
    },
    
    "recipe_complexity_distribution": {
        "title": "Recipe Complexity Distribution",
        "description": "Distribution of recipes by ingredient count",
        "sql": """
            SELECT 
                ingredient_count,
                COUNT(*) as recipe_count
            FROM (
                SELECT 
                    r.recipe_id,
                    COUNT(DISTINCT ri.ingredient_id) as ingredient_count
                FROM recipes r
                LEFT JOIN recipe_ingredients ri ON r.recipe_id = ri.recipe_id
                GROUP BY r.recipe_id
            )
            GROUP BY ingredient_count
            ORDER BY ingredient_count
        """
    },
    
    "quickest_recipes": {
        "title": "Quickest Recipes",
        "description": "Recipes with shortest total time (where available)",
        "sql": """
            SELECT 
                r.name,
                r.total_time,
                r.difficulty,
                COUNT(DISTINCT ri.ingredient_id) as ingredient_count
            FROM recipes r
            LEFT JOIN recipe_ingredients ri ON r.recipe_id = ri.recipe_id
            WHERE r.total_time IS NOT NULL
            GROUP BY r.recipe_id
            ORDER BY r.total_time
            LIMIT 15
        """
    },
    
    "tag_popularity": {
        "title": "Most Popular Recipe Tags",
        "description": "Tags used most frequently",
        "sql": """
            SELECT 
                t.name,
                t.type,
                COUNT(DISTINCT rt.recipe_id) as recipe_count
            FROM tags t
            JOIN recipe_tags rt ON t.tag_id = rt.tag_id
            GROUP BY t.tag_id
            ORDER BY recipe_count DESC
            LIMIT 15
        """
    },
    
    "ingredient_pairs": {
        "title": "Common Ingredient Pairings",
        "description": "Which ingredients often appear together?",
        "sql": """
            SELECT 
                i1.name as ingredient_1,
                i2.name as ingredient_2,
                COUNT(DISTINCT ri1.recipe_id) as recipes_together
            FROM recipe_ingredients ri1
            JOIN recipe_ingredients ri2 ON ri1.recipe_id = ri2.recipe_id
            JOIN ingredients i1 ON ri1.ingredient_id = i1.ingredient_id
            JOIN ingredients i2 ON ri2.ingredient_id = i2.ingredient_id
            WHERE ri1.ingredient_id < ri2.ingredient_id
            GROUP BY ri1.ingredient_id, ri2.ingredient_id
            ORDER BY recipes_together DESC
            LIMIT 20
        """
    },
    
    "recipe_lifecycle_summary": {
        "title": "Recipe Lifecycle Summary",
        "description": "Active vs. inactive recipes",
        "sql": """
            SELECT 
                CASE 
                    WHEN is_active = 1 THEN 'Active'
                    ELSE 'Inactive'
                END as status,
                COUNT(*) as recipe_count,
                AVG(JULIANDAY(last_seen_date) - JULIANDAY(first_seen_date)) as avg_days_in_catalog
            FROM recipes
            GROUP BY is_active
        """
    }
}


# ======================
# Query Execution
# ======================

def run_query(conn: sqlite3.Connection, query_key: str, query_def: dict) -> None:
    """
    Execute a query and display results in a formatted table.
    """
    print(f"\n{'='*70}")
    print(f"{query_def['title']}")
    print(f"{'='*70}")
    print(f"{query_def['description']}\n")
    
    cursor = conn.cursor()
    cursor.execute(query_def['sql'])
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Print header
    header = " | ".join(f"{col:20}" for col in columns)
    print(header)
    print("-" * len(header))
    
    # Print rows
    row_count = 0
    for row in cursor.fetchall():
        formatted_row = " | ".join(f"{str(val):20}" for val in row)
        print(formatted_row)
        row_count += 1
    
    print(f"\n({row_count} rows)\n")


def run_all_queries() -> None:
    """
    Execute all exploratory queries.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Data Explorer                                ║
    ║  Interesting Queries & Insights                          ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    conn = sqlite3.connect(SILVER_DB)
    
    for query_key, query_def in QUERIES.items():
        try:
            run_query(conn, query_key, query_def)
        except Exception as e:
            print(f"Error running {query_key}: {e}\n")
    
    conn.close()


# ======================
# Interactive Mode
# ======================

def interactive_mode() -> None:
    """
    Let user choose which queries to run.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Data Explorer - Interactive Mode             ║
    ╚══════════════════════════════════════════════════════════╝
    
    Available queries:
    """)
    
    for i, (key, query) in enumerate(QUERIES.items(), 1):
        print(f"  {i}. {query['title']}")
    
    print(f"  {len(QUERIES) + 1}. Run all queries")
    print(f"  0. Exit\n")
    
    conn = sqlite3.connect(SILVER_DB)
    
    while True:
        try:
            choice = input("Select query number (or 0 to exit): ").strip()
            
            if choice == "0":
                print("Goodbye!")
                break
            
            choice_num = int(choice)
            
            if choice_num == len(QUERIES) + 1:
                # Run all
                for query_key, query_def in QUERIES.items():
                    run_query(conn, query_key, query_def)
            elif 1 <= choice_num <= len(QUERIES):
                # Run specific query
                query_key = list(QUERIES.keys())[choice_num - 1]
                query_def = QUERIES[query_key]
                run_query(conn, query_key, query_def)
            else:
                print("Invalid choice. Try again.\n")
                
        except ValueError:
            print("Please enter a number.\n")
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
    
    conn.close()


# ======================
# Entry Point
# ======================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        run_all_queries()
    else:
        interactive_mode()