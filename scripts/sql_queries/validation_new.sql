-- ============================================================================
-- Schema Validation Queries: Quick health check of database
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/validation.sql
-- For detailed gold layer checks, see: 06_gold_layer_validation.sql

.mode column
.headers on

.print ===============================================
.print TABLE EXISTENCE CHECK
.print ===============================================
.tables

.print ===============================================
.print BRONZE LAYER - API RESPONSES
.print ===============================================
SELECT 
    'api_responses' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT endpoint) as endpoints,
    COUNT(DISTINCT pull_date) as pull_dates
FROM api_responses;

.print ===============================================
.print SILVER LAYER - ENTITY TABLES
.print ===============================================

SELECT 
    'recipes' as table_name,
    COUNT(*) as total,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive
FROM recipes
UNION ALL
SELECT 
    'ingredients',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END)
FROM ingredients
UNION ALL
SELECT 
    'allergens',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END)
FROM allergens
UNION ALL
SELECT 
    'tags',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END)
FROM tags
UNION ALL
SELECT 
    'labels',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END)
FROM labels
UNION ALL
SELECT 
    'menus',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END)
FROM menus;

.print ===============================================
.print SILVER LAYER - BRIDGE TABLES
.print ===============================================

SELECT 
    'recipe_ingredients' as table_name,
    COUNT(*) as total,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active
FROM recipe_ingredients
UNION ALL
SELECT 
    'recipe_allergens',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END)
FROM recipe_allergens
UNION ALL
SELECT 
    'recipe_tags',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END)
FROM recipe_tags
UNION ALL
SELECT 
    'recipe_labels',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END)
FROM recipe_labels
UNION ALL
SELECT 
    'menu_recipes',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END)
FROM menu_recipes;

.print ===============================================
.print GOLD LAYER SUMMARY
.print ===============================================

SELECT 
    'weekly_menu_metrics' as table_name,
    COUNT(*) as row_count
FROM weekly_menu_metrics
UNION ALL
SELECT 
    'recipe_survival_metrics',
    COUNT(*)
FROM recipe_survival_metrics
UNION ALL
SELECT 
    'ingredient_trends',
    COUNT(*)
FROM ingredient_trends
UNION ALL
SELECT 
    'allergen_density',
    COUNT(*)
FROM allergen_density
UNION ALL
SELECT 
    'menu_stability_metrics',
    COUNT(*)
FROM menu_stability_metrics;
