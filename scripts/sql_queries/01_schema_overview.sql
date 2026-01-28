-- ============================================================================
-- Schema Overview: Get a high-level view of the database structure
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/01_schema_overview.sql

.mode column
.headers on

-- Show all tables and their record counts
SELECT 
    name as table_name,
    (SELECT COUNT(*) FROM recipes WHERE table_name = 'recipes') as row_count
FROM sqlite_master WHERE type='table' AND name='recipes'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM ingredients) FROM sqlite_master WHERE type='table' AND name='ingredients'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM allergens) FROM sqlite_master WHERE type='table' AND name='allergens'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM tags) FROM sqlite_master WHERE type='table' AND name='tags'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM labels) FROM sqlite_master WHERE type='table' AND name='labels'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM menus) FROM sqlite_master WHERE type='table' AND name='menus'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM api_responses) FROM sqlite_master WHERE type='table' AND name='api_responses'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM recipe_ingredients) FROM sqlite_master WHERE type='table' AND name='recipe_ingredients'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM recipe_allergens) FROM sqlite_master WHERE type='table' AND name='recipe_allergens'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM recipe_tags) FROM sqlite_master WHERE type='table' AND name='recipe_tags'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM recipe_labels) FROM sqlite_master WHERE type='table' AND name='recipe_labels'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM menu_recipes) FROM sqlite_master WHERE type='table' AND name='menu_recipes'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM weekly_menu_metrics) FROM sqlite_master WHERE type='table' AND name='weekly_menu_metrics'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM recipe_survival_metrics) FROM sqlite_master WHERE type='table' AND name='recipe_survival_metrics'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM ingredient_trends) FROM sqlite_master WHERE type='table' AND name='ingredient_trends'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM menu_stability_metrics) FROM sqlite_master WHERE type='table' AND name='menu_stability_metrics'
UNION ALL
SELECT name, (SELECT COUNT(*) FROM allergen_density) FROM sqlite_master WHERE type='table' AND name='allergen_density';

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
