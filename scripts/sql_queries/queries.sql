-- =====================================================
-- HelloFresh Data Platform - Query Reference
-- Silver & Gold Layer Analytics
-- =====================================================
-- Run these queries with SQLite on hfresh.db
-- sqlite3 C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics\hfresh\hfresh.db < scripts\sql_queries\queries.sql


-- =====================================================
-- SILVER LAYER QUERIES
-- =====================================================

-- 1. Top 5 Most Common Recipes
SELECT 
    r.name,
    COUNT(DISTINCT mr.menu_id) as menu_appearances,
    r.difficulty,
    r.prep_time,
    r.cuisine
FROM recipes r
JOIN menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = 1
WHERE r.is_active = 1
GROUP BY r.id
ORDER BY menu_appearances DESC
LIMIT 5;


-- 2. Top 10 Most Used Ingredients
SELECT 
    i.name,
    COUNT(DISTINCT ri.recipe_id) as recipe_count,
    COUNT(DISTINCT mr.menu_id) as menu_count
FROM ingredients i
JOIN recipe_ingredients ri 
    ON i.id = ri.ingredient_id AND ri.is_active = 1
JOIN menu_recipes mr 
    ON ri.recipe_id = mr.recipe_id AND mr.is_active = 1
GROUP BY i.id
ORDER BY recipe_count DESC
LIMIT 10;


-- 3. Menu Diversity Analysis
SELECT 
    m.start_date as week_date,
    m.year_week,
    COUNT(DISTINCT mr.recipe_id) as unique_recipes,
    COUNT(DISTINCT r.cuisine) as unique_cuisines,
    ROUND(AVG(CAST(r.difficulty AS REAL)), 2) as avg_difficulty
FROM menus m
JOIN menu_recipes mr 
    ON m.id = mr.menu_id AND mr.is_active = 1
JOIN recipes r 
    ON mr.recipe_id = r.id
WHERE m.is_active = 1
GROUP BY m.id, m.start_date, m.year_week
ORDER BY m.start_date DESC;


-- 4. Most Common Allergens (Top 10)
SELECT 
    a.name,
    COUNT(DISTINCT ra.recipe_id) as recipe_count,
    ROUND(100.0 * COUNT(DISTINCT ra.recipe_id) / 
          (SELECT COUNT(DISTINCT id) FROM recipes), 2) as pct_of_recipes
FROM allergens a
JOIN recipe_allergens ra 
    ON a.id = ra.allergen_id AND ra.is_active = 1
GROUP BY a.id
ORDER BY recipe_count DESC
LIMIT 10;


-- 5. Cuisine Distribution
SELECT 
    COALESCE(cuisine, 'Unknown') as cuisine,
    COUNT(DISTINCT id) as recipe_count,
    COUNT(DISTINCT mr.menu_id) as menu_appearances
FROM recipes r
LEFT JOIN menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = 1
WHERE r.is_active = 1
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
    FROM recipes r
    LEFT JOIN recipe_ingredients ri 
        ON r.id = ri.recipe_id AND ri.is_active = 1
    WHERE r.is_active = 1
    GROUP BY r.id
)
GROUP BY ingredient_count
ORDER BY ingredient_count;


-- 7. Quickest Recipes (fastest prep time)
SELECT 
    r.name,
    r.prep_time,
    r.total_time,
    r.difficulty,
    COUNT(DISTINCT ri.ingredient_id) as ingredient_count
FROM recipes r
LEFT JOIN recipe_ingredients ri 
    ON r.id = ri.ingredient_id AND ri.is_active = 1
WHERE r.is_active = 1 AND r.prep_time IS NOT NULL
GROUP BY r.id
ORDER BY r.prep_time ASC
LIMIT 5;


-- 8. Recipe Lifecycle Summary
SELECT 
    CASE WHEN is_active = 1 THEN 'Active' ELSE 'Inactive' END as status,
    COUNT(*) as recipe_count,
    ROUND(AVG(CAST((julianday(last_seen_date) - julianday(first_seen_date)) AS REAL)), 0) as avg_days_active,
    MIN(first_seen_date) as earliest_first_seen,
    MAX(last_seen_date) as latest_last_seen
FROM recipes
GROUP BY is_active;


-- 9. Recently Churned Recipes (disappeared in last 2 weeks)
SELECT 
    name,
    last_seen_date,
    CAST((julianday(DATE('now')) - julianday(last_seen_date)) AS INTEGER) as days_since_removal,
    difficulty,
    cuisine
FROM recipes
WHERE is_active = 0 
  AND last_seen_date >= DATE('now', '-14 days')
ORDER BY last_seen_date DESC
LIMIT 10;


-- 10. New Recipes This Week
-- SELECT 
--     r.name,
--     r.first_seen_date,
--     r.difficulty,
--     r.cuisine,
--     COUNT(DISTINCT ri.ingredient_id) as ingredient_count
-- FROM recipes r
-- LEFT JOIN recipe_ingredients ri 
--     ON r.id = ri.ingredient_id
-- WHERE r.first_seen_date >= DATE('now', '-7 days')
-- GROUP BY r.id
-- ORDER BY r.first_seen_date DESC;


-- =====================================================
-- GOLD LAYER QUERIES
-- =====================================================

-- 11. Weekly Menu Metrics (Last 6 weeks)
SELECT 
    week_start_date,
    total_recipes,
    unique_recipes,
    new_recipes,
    returning_recipes,
    ROUND(avg_difficulty, 2) as avg_difficulty,
    ROUND(avg_prep_time_minutes, 0) as avg_prep_time_minutes
FROM weekly_menu_metrics
ORDER BY week_start_date DESC
LIMIT 6;


-- 12. Recipe Survival Metrics (Currently Active) - Top 5
SELECT 
    recipe_name,
    first_appearance_date,
    last_appearance_date,
    total_weeks_active,
    weeks_since_last_seen,
    is_currently_active
FROM recipe_survival_metrics
WHERE is_currently_active = 1
ORDER BY total_weeks_active DESC
LIMIT 5;


-- 13. Recipe Survival Metrics (Recently Churned)
SELECT 
    recipe_name,
    first_appearance_date,
    last_appearance_date,
    total_weeks_active,
    weeks_since_last_seen
FROM recipe_survival_metrics
WHERE is_currently_active = 0 
  AND weeks_since_last_seen <= 4
ORDER BY last_appearance_date DESC
LIMIT 5;


-- 14. Top Trending Ingredients (Top 5 per week)
SELECT 
    week_start_date,
    ingredient_name,
    recipe_count,
    popularity_rank
FROM ingredient_trends
WHERE popularity_rank <= 5
ORDER BY week_start_date DESC, popularity_rank ASC
LIMIT 20;


-- 15. Menu Stability Analysis (Last 8 Weeks)
SELECT 
    week_start_date,
    ROUND(overlap_with_prev_week, 2) as overlap_pct,
    ROUND(new_recipe_rate, 2) as new_recipe_rate_pct,
    ROUND(churned_recipe_rate, 2) as churned_recipe_rate_pct,
    recipes_retained,
    recipes_added,
    recipes_removed
FROM menu_stability_metrics
ORDER BY week_start_date DESC
LIMIT 8;


-- 16. Top Allergens by Week (Top 10 per week)
SELECT 
    week_start_date,
    allergen_name,
    recipe_count,
    ROUND(percentage_of_menu, 2) as pct_of_menu
FROM allergen_density
ORDER BY week_start_date DESC, percentage_of_menu DESC
LIMIT 60;


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
FROM menu_stability_metrics
WHERE overlap_with_prev_week IS NOT NULL
ORDER BY week_start_date DESC;


-- 18. Ingredient Lifecycle (Active -> Churned) - Top 10
SELECT 
    i.name,
    COUNT(CASE WHEN r.is_active = 1 THEN 1 END) as currently_active,
    COUNT(CASE WHEN r.is_active = 0 THEN 1 END) as recently_churned,
    CAST((julianday(MAX(r.last_seen_date)) - julianday(MIN(r.first_seen_date))) AS INTEGER) as total_days_in_use
FROM recipe_ingredients ri
JOIN ingredients i ON ri.ingredient_id = i.id
JOIN recipes r ON ri.recipe_id = r.id
GROUP BY i.id
ORDER BY (COUNT(CASE WHEN r.is_active = 1 THEN 1 END) + 
          COUNT(CASE WHEN r.is_active = 0 THEN 1 END)) DESC
LIMIT 10;


-- 19. Difficulty vs Menu Appearances (Quality Analysis)
SELECT 
    difficulty,
    COUNT(DISTINCT id) as recipe_count,
    ROUND(AVG(CAST(prep_time AS REAL)), 1) as avg_prep_time,
    COUNT(DISTINCT mr.menu_id) as total_menu_appearances
FROM recipes r
LEFT JOIN menu_recipes mr 
    ON r.id = mr.recipe_id AND mr.is_active = 1
WHERE r.is_active = 1
GROUP BY r.difficulty
ORDER BY difficulty;


-- 20. Gold Layer Data Quality Check
SELECT 
    (SELECT COUNT(*) FROM weekly_menu_metrics) as weekly_metrics_rows,
    (SELECT COUNT(*) FROM recipe_survival_metrics) as recipe_survival_rows,
    (SELECT COUNT(*) FROM ingredient_trends) as ingredient_trends_rows,
    (SELECT COUNT(*) FROM menu_stability_metrics) as menu_stability_rows,
    (SELECT COUNT(*) FROM allergen_density) as allergen_density_rows;


-- =====================================================
-- QUICK SANITY CHECKS
-- =====================================================

-- 21. Count records in all Silver tables
SELECT 
    'recipes' as table_name,
    COUNT(*) as row_count,
    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_count
FROM recipes
UNION ALL
SELECT 
    'menus',
    COUNT(*),
    COUNT(CASE WHEN is_active = 1 THEN 1 END)
FROM menus
UNION ALL
SELECT 
    'ingredients',
    COUNT(*),
    COUNT(CASE WHEN is_active = 1 THEN 1 END)
FROM ingredients
UNION ALL
SELECT 
    'allergens',
    COUNT(*),
    COUNT(CASE WHEN is_active = 1 THEN 1 END)
FROM allergens;


-- 22. Last data refresh times
SELECT 
    MAX(last_seen_date) as latest_data_in_system,
    CAST((julianday(DATE('now')) - julianday(MAX(last_seen_date))) AS INTEGER) as days_since_update,
    COUNT(DISTINCT DATE(last_seen_date)) as distinct_days_with_data
FROM recipes
WHERE last_seen_date >= DATE('now', '-90 days');
