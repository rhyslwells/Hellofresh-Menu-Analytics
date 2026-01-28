-- ============================================================================
-- Gold Layer Validation: Verify analytics tables are populated and correct
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/06_gold_layer_validation.sql

.mode column
.headers on

.print ===============================================
.print GOLD LAYER POPULATION CHECK
.print ===============================================

SELECT 
    'weekly_menu_metrics' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT week_start_date) as unique_weeks,
    MIN(week_start_date) as earliest_week,
    MAX(week_start_date) as latest_week
FROM weekly_menu_metrics
UNION ALL
SELECT 
    'recipe_survival_metrics',
    COUNT(*),
    COUNT(DISTINCT recipe_id),
    MIN(first_appearance_date),
    MAX(last_appearance_date)
FROM recipe_survival_metrics
UNION ALL
SELECT 
    'ingredient_trends',
    COUNT(*),
    COUNT(DISTINCT week_start_date),
    MIN(week_start_date),
    MAX(week_start_date)
FROM ingredient_trends
UNION ALL
SELECT 
    'allergen_density',
    COUNT(*),
    COUNT(DISTINCT week_start_date),
    MIN(week_start_date),
    MAX(week_start_date)
FROM allergen_density
UNION ALL
SELECT 
    'menu_stability_metrics',
    COUNT(*),
    COUNT(DISTINCT week_start_date),
    MIN(week_start_date),
    MAX(week_start_date)
FROM menu_stability_metrics;

.print ===============================================
.print WEEKLY METRICS COMPLETENESS
.print ===============================================

SELECT 
    'Expected metrics per week' as check_type,
    COUNT(DISTINCT week_start_date) as weeks_with_data,
    COUNT(DISTINCT recipe_id) as unique_recipes_tracked,
    COUNT(CASE WHEN avg_difficulty IS NOT NULL THEN 1 END) as weeks_with_difficulty,
    COUNT(CASE WHEN avg_prep_time_minutes IS NOT NULL THEN 1 END) as weeks_with_prep_time
FROM weekly_menu_metrics;

.print ===============================================
.print RECIPE SURVIVAL COVERAGE
.print ===============================================

SELECT 
    COUNT(*) as total_recipes_in_survival,
    SUM(CASE WHEN is_currently_active = 1 THEN 1 ELSE 0 END) as active_recipes,
    SUM(CASE WHEN is_currently_active = 0 THEN 1 ELSE 0 END) as inactive_recipes,
    COUNT(CASE WHEN consecutive_weeks_active IS NOT NULL THEN 1 END) as with_consecutive_weeks,
    MIN(CAST(total_weeks_active AS INTEGER)) as min_weeks,
    MAX(CAST(total_weeks_active AS INTEGER)) as max_weeks,
    ROUND(AVG(CAST(total_weeks_active AS INTEGER)), 1) as avg_weeks
FROM recipe_survival_metrics;

.print ===============================================
.print INGREDIENT TRENDS DATA VALIDATION
.print ===============================================

SELECT 
    COUNT(DISTINCT ingredient_id) as unique_ingredients_tracked,
    COUNT(DISTINCT week_start_date) as weeks_tracked,
    COUNT(*) as total_trend_rows,
    MIN(recipe_count) as min_recipes,
    MAX(recipe_count) as max_recipes,
    COUNT(CASE WHEN week_over_week_change IS NOT NULL THEN 1 END) as rows_with_wow_change
FROM ingredient_trends;

.print ===============================================
.print ALLERGEN DENSITY VALIDATION
.print ===============================================

SELECT 
    COUNT(DISTINCT allergen_id) as unique_allergens,
    COUNT(DISTINCT week_start_date) as weeks_tracked,
    MIN(percentage_of_menu) as min_pct,
    MAX(percentage_of_menu) as max_pct,
    COUNT(CASE WHEN percentage_of_menu > 100 THEN 1 END) as invalid_percentages
FROM allergen_density;

.print ===============================================
.print MENU STABILITY DATA VALIDATION
.print ===============================================

SELECT 
    COUNT(*) as total_weeks,
    COUNT(CASE WHEN overlap_with_prev_week IS NOT NULL THEN 1 END) as weeks_with_overlap,
    ROUND(AVG(overlap_with_prev_week), 3) as avg_overlap,
    MIN(overlap_with_prev_week) as min_overlap,
    MAX(overlap_with_prev_week) as max_overlap,
    COUNT(CASE WHEN new_recipe_rate IS NOT NULL THEN 1 END) as weeks_with_new_rate
FROM menu_stability_metrics;

.print ===============================================
.print CROSS-TABLE CONSISTENCY CHECK
.print ===============================================

-- Verify weekly_menu_metrics weeks match other gold tables
SELECT 
    'Weeks in weekly_menu_metrics' as source,
    COUNT(DISTINCT week_start_date) as week_count
FROM weekly_menu_metrics
UNION ALL
SELECT 
    'Weeks in menu_stability_metrics',
    COUNT(DISTINCT week_start_date)
FROM menu_stability_metrics
UNION ALL
SELECT 
    'Weeks in ingredient_trends',
    COUNT(DISTINCT week_start_date)
FROM ingredient_trends
UNION ALL
SELECT 
    'Weeks in allergen_density',
    COUNT(DISTINCT week_start_date)
FROM allergen_density;

.print ===============================================
.print GOLD LAYER COMPLETENESS SCORECARD
.print ===============================================

SELECT 
    CASE 
        WHEN (SELECT COUNT(*) FROM weekly_menu_metrics) > 0 THEN '✓'
        ELSE '✗'
    END as has_weekly_metrics,
    CASE 
        WHEN (SELECT COUNT(*) FROM recipe_survival_metrics) > 0 THEN '✓'
        ELSE '✗'
    END as has_recipe_survival,
    CASE 
        WHEN (SELECT COUNT(*) FROM ingredient_trends) > 0 THEN '✓'
        ELSE '✗'
    END as has_ingredient_trends,
    CASE 
        WHEN (SELECT COUNT(*) FROM allergen_density) > 0 THEN '✓'
        ELSE '✗'
    END as has_allergen_density,
    CASE 
        WHEN (SELECT COUNT(*) FROM menu_stability_metrics) > 0 THEN '✓'
        ELSE '✗'
    END as has_menu_stability,
    CASE 
        WHEN (SELECT COUNT(DISTINCT week_start_date) FROM weekly_menu_metrics) = 
             (SELECT COUNT(DISTINCT week_start_date) FROM menu_stability_metrics) THEN '✓'
        ELSE '✗'
    END as weeks_aligned;
