-- ============================================================================
-- Analytics Insights: Query the gold layer for trends and metrics
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/04_analytics_insights.sql

.mode column
.headers on

.print ===============================================
.print WEEKLY MENU METRICS SUMMARY
.print ===============================================

SELECT 
    week_start_date,
    locale,
    total_recipes,
    unique_recipes,
    new_recipes,
    returning_recipes,
    ROUND(avg_difficulty, 2) as avg_difficulty,
    ROUND(avg_prep_time_minutes, 1) as avg_prep_time_mins
FROM weekly_menu_metrics
ORDER BY week_start_date DESC
LIMIT 10;

.print ===============================================
.print RECIPE SURVIVAL METRICS - TOP PERFORMERS
.print ===============================================

SELECT 
    recipe_id,
    recipe_name,
    first_appearance_date,
    last_appearance_date,
    total_weeks_active,
    consecutive_weeks_active,
    is_currently_active,
    weeks_since_last_seen
FROM recipe_survival_metrics
ORDER BY total_weeks_active DESC
LIMIT 15;

.print ===============================================
.print RECIPE SURVIVAL METRICS - RECENTLY CHURNED
.print ===============================================

SELECT 
    recipe_id,
    recipe_name,
    last_appearance_date,
    total_weeks_active,
    weeks_since_last_seen
FROM recipe_survival_metrics
WHERE is_currently_active = 0
ORDER BY weeks_since_last_seen DESC
LIMIT 10;

.print ===============================================
.print INGREDIENT TRENDS - TOP MOVERS THIS WEEK
.print ===============================================

SELECT 
    ingredient_name,
    week_start_date,
    recipe_count,
    week_over_week_change,
    popularity_rank
FROM ingredient_trends
ORDER BY week_start_date DESC, popularity_rank ASC
LIMIT 15;

.print ===============================================
.print INGREDIENT TRENDING OVER TIME
.print ===============================================

SELECT 
    ingredient_name,
    week_start_date,
    recipe_count,
    popularity_rank
FROM ingredient_trends
WHERE ingredient_name IN (
    SELECT ingredient_name FROM ingredient_trends 
    GROUP BY ingredient_name 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
ORDER BY ingredient_name, week_start_date DESC;

.print ===============================================
.print ALLERGEN DENSITY BY WEEK
.print ===============================================

SELECT 
    week_start_date,
    allergen_name,
    recipe_count,
    ROUND(percentage_of_menu, 2) as pct_of_menu
FROM allergen_density
ORDER BY week_start_date DESC, percentage_of_menu DESC
LIMIT 20;

.print ===============================================
.print ALLERGEN PREVALENCE SUMMARY
.print ===============================================

SELECT 
    allergen_name,
    COUNT(DISTINCT week_start_date) as weeks_present,
    ROUND(AVG(recipe_count), 1) as avg_recipe_count,
    MAX(recipe_count) as max_recipe_count,
    ROUND(AVG(percentage_of_menu), 2) as avg_pct_of_menu
FROM allergen_density
GROUP BY allergen_name
ORDER BY avg_pct_of_menu DESC;

.print ===============================================
.print MENU STABILITY METRICS
.print ===============================================

SELECT 
    week_start_date,
    locale,
    ROUND(overlap_with_prev_week, 3) as overlap_prev_week,
    ROUND(new_recipe_rate, 3) as new_recipe_rate,
    ROUND(churned_recipe_rate, 3) as churned_recipe_rate,
    recipes_retained,
    recipes_added,
    recipes_removed
FROM menu_stability_metrics
ORDER BY week_start_date DESC
LIMIT 10;

.print ===============================================
.print MENU STABILITY TRENDS
.print ===============================================

SELECT 
    week_start_date,
    ROUND(AVG(overlap_with_prev_week), 3) as avg_overlap,
    ROUND(AVG(new_recipe_rate), 3) as avg_new_rate,
    SUM(recipes_added) as total_added,
    SUM(recipes_removed) as total_removed
FROM menu_stability_metrics
GROUP BY week_start_date
ORDER BY week_start_date DESC
LIMIT 12;

.print ===============================================
.print RECIPE INTRODUCTION TIMING
.print ===============================================

SELECT 
    first_appearance_date,
    COUNT(*) as recipes_introduced
FROM recipe_survival_metrics
GROUP BY first_appearance_date
ORDER BY first_appearance_date DESC;

.print ===============================================
.print CURRENT MENU HEALTH CHECK
.print ===============================================

SELECT 
    'Menu Health' as metric,
    (SELECT COUNT(*) FROM recipes WHERE is_active = 1) as active_recipes,
    (SELECT COUNT(*) FROM menus WHERE is_active = 1) as active_menus,
    (SELECT COUNT(*) FROM recipe_survival_metrics WHERE is_currently_active = 1) as recipes_in_survival_metrics,
    (SELECT COUNT(DISTINCT week_start_date) FROM weekly_menu_metrics) as weeks_of_data;
