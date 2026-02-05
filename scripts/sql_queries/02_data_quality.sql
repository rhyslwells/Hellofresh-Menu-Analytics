-- ============================================================================
-- Data Quality Checks: Verify data completeness and integrity
-- ============================================================================
-- sqlite3 hfresh/hfresh.db < scripts/sql_queries/02_data_quality.sql

.mode column
.headers on

.print ===============================================
.print ACTIVE vs INACTIVE RECORDS (SCD Type 2)
.print ===============================================

SELECT 
    'recipes' as table_name,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive_count,
    COUNT(*) as total_count
FROM recipes
UNION ALL
SELECT 
    'ingredients',
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    COUNT(*)
FROM ingredients
UNION ALL
SELECT 
    'allergens',
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    COUNT(*)
FROM allergens
UNION ALL
SELECT 
    'tags',
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    COUNT(*)
FROM tags
UNION ALL
SELECT 
    'menus',
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    COUNT(*)
FROM menus;

.print ===============================================
.print RECIPES WITH MISSING FIELDS
.print ===============================================

SELECT 
    COUNT(*) as total_recipes,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as missing_name,
    SUM(CASE WHEN difficulty IS NULL THEN 1 ELSE 0 END) as missing_difficulty,
    SUM(CASE WHEN prep_time IS NULL THEN 1 ELSE 0 END) as missing_prep_time,
    SUM(CASE WHEN cuisine IS NULL THEN 1 ELSE 0 END) as missing_cuisine
FROM recipes WHERE is_active = 1;

.print ===============================================
.print INGREDIENTS QUALITY SNAPSHOT
.print ===============================================

SELECT 
    COUNT(*) as total_ingredients,
    COUNT(DISTINCT name) as unique_names,
    COUNT(DISTINCT family) as unique_families,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as missing_name,
    SUM(CASE WHEN family IS NULL THEN 1 ELSE 0 END) as missing_family
FROM ingredients WHERE is_active = 1;

.print ===============================================
.print API RESPONSES INGESTION STATS
.print ===============================================

SELECT 
    endpoint,
    COUNT(*) as ingestion_count,
    COUNT(DISTINCT pull_date) as distinct_pull_dates,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as last_ingestion,
    SUM(CASE WHEN payload IS NULL THEN 1 ELSE 0 END) as null_payloads
FROM api_responses
GROUP BY endpoint
ORDER BY ingestion_count DESC;

.print ===============================================
.print DATE RANGE COVERAGE
.print ===============================================

SELECT 
    'recipes' as table_name,
    MIN(first_seen_date) as earliest_date,
    MAX(last_seen_date) as latest_date,
    CAST((julianday(MAX(last_seen_date)) - julianday(MIN(first_seen_date))) AS INTEGER) as days_spanned
FROM recipes
UNION ALL
SELECT 
    'menus',
    MIN(first_seen_date),
    MAX(last_seen_date),
    CAST((julianday(MAX(last_seen_date)) - julianday(MIN(first_seen_date))) AS INTEGER)
FROM menus
UNION ALL
SELECT 
    'ingredients',
    MIN(first_seen_date),
    MAX(last_seen_date),
    CAST((julianday(MAX(last_seen_date)) - julianday(MIN(first_seen_date))) AS INTEGER)
FROM ingredients;

.print ===============================================
.print RELATIONSHIP COMPLETENESS
.print ===============================================

SELECT 
    (SELECT COUNT(*) FROM recipe_ingredients WHERE is_active = 1) as active_recipe_ingredients,
    (SELECT COUNT(*) FROM recipe_allergens WHERE is_active = 1) as active_recipe_allergens,
    (SELECT COUNT(*) FROM recipe_tags WHERE is_active = 1) as active_recipe_tags,
    (SELECT COUNT(*) FROM recipe_labels WHERE is_active = 1) as active_recipe_labels,
    (SELECT COUNT(*) FROM menu_recipes WHERE is_active = 1) as active_menu_recipes;

.print ===============================================
.print RECIPES WITH NO RELATIONSHIPS
.print ===============================================

SELECT 
    COUNT(*) as recipes_with_no_ingredients,
    0 as recipes_with_no_allergens,
    0 as recipes_with_no_tags
FROM recipes r
WHERE is_active = 1
AND NOT EXISTS (SELECT 1 FROM recipe_ingredients WHERE recipe_id = r.id AND is_active = 1);
