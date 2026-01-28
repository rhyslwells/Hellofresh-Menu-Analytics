-- ============================================================================
-- SCD Type 2 Tracking: Monitor historical changes and entity lifecycle
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/05_scd_tracking.sql

.mode column
.headers on

.print ===============================================
.print RECIPES: LIFECYCLE TRACKING
.print ===============================================

SELECT 
    id,
    name,
    first_seen_date,
    last_seen_date,
    CAST((julianday(last_seen_date) - julianday(first_seen_date)) AS INTEGER) as days_active,
    is_active,
    CASE 
        WHEN is_active = 1 THEN 'Currently Active'
        ELSE 'Inactive/Churned'
    END as status
FROM recipes
ORDER BY last_seen_date DESC
LIMIT 20;

.print ===============================================
.print INGREDIENTS: LIFECYCLE TRACKING
.print ===============================================

SELECT 
    ingredient_id,
    name,
    family,
    first_seen_date,
    last_seen_date,
    CAST((julianday(last_seen_date) - julianday(first_seen_date)) AS INTEGER) as days_active,
    is_active
FROM ingredients
ORDER BY last_seen_date DESC
LIMIT 20;

.print ===============================================
.print RECENTLY DEACTIVATED RECIPES
.print ===============================================

SELECT 
    id,
    name,
    first_seen_date,
    last_seen_date,
    CAST((julianday(last_seen_date) - julianday(first_seen_date)) AS INTEGER) as total_days,
    DATE('now') - last_seen_date as days_since_deactivation
FROM recipes
WHERE is_active = 0
ORDER BY last_seen_date DESC
LIMIT 10;

.print ===============================================
.print RECENTLY ACTIVATED RECIPES
.print ===============================================

SELECT 
    id,
    name,
    first_seen_date,
    last_seen_date,
    CAST((julianday(last_seen_date) - julianday(first_seen_date)) AS INTEGER) as days_active,
    DATE('now') - first_seen_date as days_since_activation
FROM recipes
WHERE is_active = 1 AND first_seen_date = (SELECT MAX(first_seen_date) FROM recipes WHERE is_active = 1)
ORDER BY first_seen_date DESC;

.print ===============================================
.print RECIPE ATTRIBUTES THAT CHANGED
.print ===============================================

-- Sample: Find recipes with different names across ingestions
-- This requires comparing against api_responses table structure
SELECT 
    r.id,
    r.name,
    r.difficulty,
    r.prep_time,
    r.cuisine,
    r.first_seen_date,
    r.last_seen_date,
    CASE 
        WHEN r.last_seen_date > r.first_seen_date THEN 'Updated'
        ELSE 'Static'
    END as change_status
FROM recipes r
WHERE r.is_active = 1
ORDER BY r.last_seen_date DESC
LIMIT 15;

.print ===============================================
.print BRIDGE TABLES: RELATIONSHIP LIFECYCLE
.print ===============================================

SELECT 
    'recipe_ingredients' as bridge_table,
    COUNT(*) as total_links,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_links,
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive_links,
    MIN(first_seen_date) as earliest,
    MAX(last_seen_date) as latest
FROM recipe_ingredients
UNION ALL
SELECT 
    'recipe_allergens',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    MIN(first_seen_date),
    MAX(last_seen_date)
FROM recipe_allergens
UNION ALL
SELECT 
    'recipe_tags',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    MIN(first_seen_date),
    MAX(last_seen_date)
FROM recipe_tags
UNION ALL
SELECT 
    'recipe_labels',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    MIN(first_seen_date),
    MAX(last_seen_date)
FROM recipe_labels
UNION ALL
SELECT 
    'menu_recipes',
    COUNT(*),
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END),
    MIN(first_seen_date),
    MAX(last_seen_date)
FROM menu_recipes;

.print ===============================================
.print INGESTION TRACKING COMPLETENESS
.print ===============================================

SELECT 
    COUNT(*) as total_ingestions,
    COUNT(DISTINCT pull_date) as unique_pull_dates,
    COUNT(DISTINCT endpoint) as endpoints_covered,
    COUNT(DISTINCT locale) as locales_covered,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as latest_ingestion
FROM api_responses;

.print ===============================================
.print INGESTION HISTORY BY ENDPOINT
.print ===============================================

SELECT 
    endpoint,
    COUNT(*) as ingestion_count,
    MIN(ingestion_timestamp) as first_ingest,
    MAX(ingestion_timestamp) as last_ingest,
    COUNT(DISTINCT DATE(ingestion_timestamp)) as days_ingested
FROM api_responses
GROUP BY endpoint
ORDER BY last_ingest DESC;

.print ===============================================
.print NULL/EMPTY INGESTION DETECTION
.print ===============================================

SELECT 
    endpoint,
    COUNT(CASE WHEN payload IS NULL THEN 1 END) as null_payloads,
    COUNT(CASE WHEN payload = '' THEN 1 END) as empty_payloads,
    COUNT(CASE WHEN payload IS NULL OR payload = '' THEN 1 END) as total_invalid
FROM api_responses
GROUP BY endpoint
HAVING total_invalid > 0;
