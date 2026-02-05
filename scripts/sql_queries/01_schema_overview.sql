-- ============================================================================
-- Schema Overview: Get a high-level view of the database structure
-- ============================================================================
-- sqlite3 hfresh/hfresh.db < scripts/sql_queries/01_schema_overview.sql

.mode column
.headers on

-- Show all tables and their record counts
SELECT 
    'api_responses' as table_name,
    (SELECT COUNT(*) FROM api_responses) as row_count
UNION ALL
SELECT 'recipes', (SELECT COUNT(*) FROM recipes)
UNION ALL
SELECT 'ingredients', (SELECT COUNT(*) FROM ingredients)
UNION ALL
SELECT 'allergens', (SELECT COUNT(*) FROM allergens)
UNION ALL
SELECT 'tags', (SELECT COUNT(*) FROM tags)
UNION ALL
SELECT 'labels', (SELECT COUNT(*) FROM labels)
UNION ALL
SELECT 'menus', (SELECT COUNT(*) FROM menus)
UNION ALL
SELECT 'recipe_ingredients', (SELECT COUNT(*) FROM recipe_ingredients)
UNION ALL
SELECT 'recipe_allergens', (SELECT COUNT(*) FROM recipe_allergens)
UNION ALL
SELECT 'recipe_tags', (SELECT COUNT(*) FROM recipe_tags)
UNION ALL
SELECT 'recipe_labels', (SELECT COUNT(*) FROM recipe_labels)
UNION ALL
SELECT 'menu_recipes', (SELECT COUNT(*) FROM menu_recipes)
UNION ALL
SELECT 'weekly_menu_metrics', (SELECT COUNT(*) FROM weekly_menu_metrics)
UNION ALL
SELECT 'recipe_survival_metrics', (SELECT COUNT(*) FROM recipe_survival_metrics)
UNION ALL
SELECT 'ingredient_trends', (SELECT COUNT(*) FROM ingredient_trends)
UNION ALL
SELECT 'menu_stability_metrics', (SELECT COUNT(*) FROM menu_stability_metrics)
UNION ALL
SELECT 'allergen_density', (SELECT COUNT(*) FROM allergen_density);

-- Show schema for each table
.print ===============================================
.print RECIPES TABLE SCHEMA
.print ===============================================
.schema recipes

.print ===============================================
.print INGREDIENTS TABLE SCHEMA
.print ===============================================
.schema ingredients

.print ===============================================
.print MENUS TABLE SCHEMA
.print ===============================================
.schema menus

.print ===============================================
.print WEEKLY_MENU_METRICS TABLE SCHEMA
.print ===============================================
.schema weekly_menu_metrics

.print ===============================================
.print BRIDGE TABLES SCHEMA
.print ===============================================
.schema recipe_ingredients
.schema menu_recipes

.print ===============================================
.print GOLD ANALYTICS TABLES SCHEMA
.print ===============================================
.schema recipe_survival_metrics
.schema ingredient_trends
.schema allergen_density
.schema menu_stability_metrics
