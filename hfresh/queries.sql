-- =====================================================
-- HelloFresh Data Platform - Query Reference
-- Silver & Gold Layer Analytics
-- =====================================================
-- Run these queries in Databricks SQL editor
-- Customize CATALOG/SCHEMA names as needed

SET VAR catalog = 'hfresh_catalog';
SET VAR silver_schema = 'hfresh_silver';
SET VAR gold_schema = 'hfresh_gold';


-- =====================================================
-- SILVER LAYER QUERIES
-- =====================================================

-- 1. Top 10 Most Common Recipes
SELECT 
    r.name,
    COUNT(DISTINCT mr.menu_id) as menu_appearances,
    r.difficulty,
    r.prep_time,
    r.cuisine
FROM ${catalog}.${silver_schema}.recipes r
JOIN ${catalog}.${silver_schema}.menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = TRUE
WHERE r.is_active = TRUE
GROUP BY r.id, r.name, r.difficulty, r.prep_time, r.cuisine
ORDER BY menu_appearances DESC
LIMIT 10;


-- 2. Top 20 Most Used Ingredients
SELECT 
    i.name,
    COUNT(DISTINCT ri.recipe_id) as recipe_count,
    COUNT(DISTINCT mr.menu_id) as menu_count
FROM ${catalog}.${silver_schema}.ingredients i
JOIN ${catalog}.${silver_schema}.recipe_ingredients ri 
    ON i.id = ri.ingredient_id AND ri.is_active = TRUE
JOIN ${catalog}.${silver_schema}.menu_recipes mr 
    ON ri.recipe_id = mr.recipe_id AND mr.is_active = TRUE
GROUP BY i.id, i.name
ORDER BY recipe_count DESC
LIMIT 20;


-- 3. Menu Diversity Analysis
SELECT 
    CAST(m.start_date AS DATE) as week_date,
    m.year_week,
    COUNT(DISTINCT mr.recipe_id) as unique_recipes,
    COUNT(DISTINCT r.cuisine) as unique_cuisines,
    ROUND(AVG(CAST(r.difficulty AS DECIMAL)), 2) as avg_difficulty
FROM ${catalog}.${silver_schema}.menus m
JOIN ${catalog}.${silver_schema}.menu_recipes mr 
    ON m.id = mr.menu_id AND mr.is_active = TRUE
JOIN ${catalog}.${silver_schema}.recipes r 
    ON mr.recipe_id = r.id
WHERE m.is_active = TRUE
GROUP BY m.id, CAST(m.start_date AS DATE), m.year_week
ORDER BY m.start_date DESC;


-- 4. Most Common Allergens
SELECT 
    a.name,
    COUNT(DISTINCT ra.recipe_id) as recipe_count,
    ROUND(100.0 * COUNT(DISTINCT ra.recipe_id) / 
          (SELECT COUNT(DISTINCT id) FROM ${catalog}.${silver_schema}.recipes), 2) as pct_of_recipes
FROM ${catalog}.${silver_schema}.allergens a
JOIN ${catalog}.${silver_schema}.recipe_allergens ra 
    ON a.id = ra.allergen_id AND ra.is_active = TRUE
GROUP BY a.id, a.name
ORDER BY recipe_count DESC;


-- 5. Cuisine Distribution
SELECT 
    COALESCE(cuisine, 'Unknown') as cuisine,
    COUNT(DISTINCT id) as recipe_count,
    COUNT(DISTINCT mr.menu_id) as menu_appearances
FROM ${catalog}.${silver_schema}.recipes r
LEFT JOIN ${catalog}.${silver_schema}.menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = TRUE
WHERE r.is_active = TRUE
GROUP BY r.cuisine
ORDER BY recipe_count DESC;


-- 6. Recipe Complexity Distribution (by ingredient count)
SELECT 
    ingredient_count,
    COUNT(*) as recipe_count
FROM (
    SELECT 
        r.id,
        COUNT(DISTINCT ri.ingredient_id) as ingredient_count
    FROM ${catalog}.${silver_schema}.recipes r
    LEFT JOIN ${catalog}.${silver_schema}.recipe_ingredients ri 
        ON r.id = ri.recipe_id AND ri.is_active = TRUE
    WHERE r.is_active = TRUE
    GROUP BY r.id
)
GROUP BY ingredient_count
ORDER BY ingredient_count;


-- 7. Quickest Recipes (fastest prep time)
SELECT 
    name,
    prep_time,
    total_time,
    difficulty,
    COUNT(DISTINCT ri.ingredient_id) as ingredient_count
FROM ${catalog}.${silver_schema}.recipes r
LEFT JOIN ${catalog}.${silver_schema}.recipe_ingredients ri 
    ON r.id = ri.ingredient_id AND ri.is_active = TRUE
WHERE r.is_active = TRUE AND prep_time IS NOT NULL
GROUP BY r.id, r.name, r.prep_time, r.total_time, r.difficulty
ORDER BY r.prep_time ASC
LIMIT 15;


-- 8. Recipe Lifecycle Summary
SELECT 
    CASE WHEN is_active = TRUE THEN 'Active' ELSE 'Inactive' END as status,
    COUNT(*) as recipe_count,
    ROUND(AVG(DATEDIFF(last_seen_date, first_seen_date)), 0) as avg_days_active,
    MIN(first_seen_date) as earliest_first_seen,
    MAX(last_seen_date) as latest_last_seen
FROM ${catalog}.${silver_schema}.recipes
GROUP BY is_active;


-- 9. Recently Churned Recipes (disappeared in last 2 weeks)
SELECT 
    name,
    last_seen_date,
    DATEDIFF(CURRENT_DATE(), last_seen_date) as days_since_removal,
    difficulty,
    cuisine
FROM ${catalog}.${silver_schema}.recipes
WHERE is_active = FALSE 
  AND last_seen_date >= DATE_SUB(CURRENT_DATE(), 14)
ORDER BY last_seen_date DESC;


-- 10. New Recipes This Week
SELECT 
    name,
    first_seen_date,
    difficulty,
    cuisine,
    COUNT(DISTINCT ri.ingredient_id) as ingredient_count
FROM ${catalog}.${silver_schema}.recipes r
LEFT JOIN ${catalog}.${silver_schema}.recipe_ingredients ri 
    ON r.id = ri.ingredient_id
WHERE first_seen_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY r.id, r.name, r.first_seen_date, r.difficulty, r.cuisine
ORDER BY r.first_seen_date DESC;


-- =====================================================
-- GOLD LAYER QUERIES
-- =====================================================

-- 11. Weekly Menu Metrics (Last 12 weeks)
SELECT 
    week_start_date,
    total_recipes,
    unique_recipes,
    new_recipes,
    returning_recipes,
    ROUND(avg_difficulty, 2) as avg_difficulty,
    ROUND(avg_prep_time_minutes, 0) as avg_prep_time_minutes
FROM ${catalog}.${gold_schema}.weekly_menu_metrics
ORDER BY week_start_date DESC
LIMIT 12;


-- 12. Recipe Survival Metrics (Currently Active)
SELECT 
    recipe_name,
    first_appearance_date,
    last_appearance_date,
    total_weeks_active,
    weeks_since_last_seen,
    is_currently_active
FROM ${catalog}.${gold_schema}.recipe_survival_metrics
WHERE is_currently_active = TRUE
ORDER BY total_weeks_active DESC
LIMIT 20;


-- 13. Recipe Survival Metrics (Recently Churned)
SELECT 
    recipe_name,
    first_appearance_date,
    last_appearance_date,
    total_weeks_active,
    weeks_since_last_seen
FROM ${catalog}.${gold_schema}.recipe_survival_metrics
WHERE is_currently_active = FALSE 
  AND weeks_since_last_seen <= 4
ORDER BY last_appearance_date DESC;


-- 14. Top Trending Ingredients (Last 4 Weeks)
SELECT 
    week_start_date,
    ingredient_name,
    recipe_count,
    popularity_rank
FROM ${catalog}.${gold_schema}.ingredient_trends
WHERE popularity_rank <= 10
ORDER BY week_start_date DESC, popularity_rank ASC
LIMIT 40;


-- 15. Menu Stability Analysis (Last 8 Weeks)
SELECT 
    week_start_date,
    ROUND(overlap_with_prev_week, 2) as overlap_pct,
    ROUND(new_recipe_rate, 2) as new_recipe_rate_pct,
    ROUND(churned_recipe_rate, 2) as churned_recipe_rate_pct,
    recipes_retained,
    recipes_added,
    recipes_removed
FROM ${catalog}.${gold_schema}.menu_stability_metrics
ORDER BY week_start_date DESC
LIMIT 8;


-- 16. Top Allergens by Week
SELECT 
    week_start_date,
    allergen_name,
    recipe_count,
    ROUND(percentage_of_menu, 2) as pct_of_menu
FROM ${catalog}.${gold_schema}.allergen_density
ORDER BY week_start_date DESC, percentage_of_menu DESC
LIMIT 50;


-- =====================================================
-- CROSS-LAYER INSIGHTS
-- =====================================================

-- 17. Menu Retention Trends (8-week rolling average)
SELECT 
    week_start_date,
    overlap_with_prev_week,
    AVG(overlap_with_prev_week) OVER (
        ORDER BY week_start_date 
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) as retention_8wk_avg
FROM ${catalog}.${gold_schema}.menu_stability_metrics
WHERE overlap_with_prev_week IS NOT NULL
ORDER BY week_start_date DESC;


-- 18. Ingredient Lifecycle (Active -> Churned)
SELECT 
    i.name,
    COUNT(CASE WHEN r.is_active = TRUE THEN 1 END) as currently_active,
    COUNT(CASE WHEN r.is_active = FALSE THEN 1 END) as recently_churned,
    DATEDIFF(MAX(r.last_seen_date), MIN(r.first_seen_date)) as total_days_in_use
FROM ${catalog}.${silver_schema}.recipe_ingredients ri
JOIN ${catalog}.${silver_schema}.ingredients i ON ri.ingredient_id = i.id
JOIN ${catalog}.${silver_schema}.recipes r ON ri.recipe_id = r.id
GROUP BY i.id, i.name
ORDER BY (COUNT(CASE WHEN r.is_active = TRUE THEN 1 END) + 
          COUNT(CASE WHEN r.is_active = FALSE THEN 1 END)) DESC
LIMIT 25;


-- 19. Difficulty vs Menu Appearances (Quality Analysis)
SELECT 
    difficulty,
    COUNT(DISTINCT id) as recipe_count,
    AVG(CAST(prep_time AS INT)) as avg_prep_time,
    COUNT(DISTINCT mr.menu_id) as total_menu_appearances
FROM ${catalog}.${silver_schema}.recipes r
LEFT JOIN ${catalog}.${silver_schema}.menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = TRUE
WHERE r.is_active = TRUE
GROUP BY r.difficulty
ORDER BY difficulty;


-- 20. Gold Layer Data Quality Check
SELECT 
    (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.weekly_menu_metrics) as weekly_metrics_rows,
    (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.recipe_survival_metrics) as recipe_survival_rows,
    (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.ingredient_trends) as ingredient_trends_rows,
    (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.menu_stability_metrics) as menu_stability_rows,
    (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.allergen_density) as allergen_density_rows;


-- =====================================================
-- QUICK SANITY CHECKS
-- =====================================================

-- 21. Count records in all Silver tables
SELECT 
    'recipes' as table_name,
    COUNT(*) as row_count,
    COUNT(CASE WHEN is_active THEN 1 END) as active_count
FROM ${catalog}.${silver_schema}.recipes
UNION ALL
SELECT 
    'menus',
    COUNT(*),
    COUNT(CASE WHEN is_active THEN 1 END)
FROM ${catalog}.${silver_schema}.menus
UNION ALL
SELECT 
    'ingredients',
    COUNT(*),
    COUNT(CASE WHEN is_active THEN 1 END)
FROM ${catalog}.${silver_schema}.ingredients
UNION ALL
SELECT 
    'allergens',
    COUNT(*),
    COUNT(CASE WHEN is_active THEN 1 END)
FROM ${catalog}.${silver_schema}.allergens;


-- 22. Last data refresh times
SELECT 
    CAST(MAX(last_seen_date) AS DATE) as latest_data_in_system,
    DATEDIFF(CURRENT_DATE(), CAST(MAX(last_seen_date) AS DATE)) as days_since_update,
    COUNT(DISTINCT CAST(last_seen_date AS DATE)) as distinct_days_with_data
FROM ${catalog}.${silver_schema}.recipes
WHERE last_seen_date >= DATE_SUB(CURRENT_DATE(), 90);
