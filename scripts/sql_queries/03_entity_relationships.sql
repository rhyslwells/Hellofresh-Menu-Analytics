-- ============================================================================
-- Entity Relationships: Explore connections between tables
-- ============================================================================
-- Run: sqlite3 hfresh/hfresh.db < scripts/sql_queries/03_entity_relationships.sql

.mode column
.headers on

.print ===============================================
.print RECIPE DETAILS WITH RELATIONSHIPS
.print ===============================================

SELECT 
    r.id,
    r.name,
    r.difficulty,
    r.prep_time,
    (SELECT COUNT(*) FROM recipe_ingredients WHERE recipe_id = r.id AND is_active = 1) as ingredient_count,
    (SELECT COUNT(*) FROM recipe_allergens WHERE recipe_id = r.id AND is_active = 1) as allergen_count,
    (SELECT COUNT(*) FROM recipe_tags WHERE recipe_id = r.id AND is_active = 1) as tag_count,
    (SELECT COUNT(*) FROM menu_recipes WHERE recipe_id = r.id AND is_active = 1) as menu_count,
    r.is_active,
    r.first_seen_date,
    r.last_seen_date
FROM recipes r
WHERE r.is_active = 1
LIMIT 20;

.print ===============================================
.print TOP 10 MOST COMMON INGREDIENTS
.print ===============================================

SELECT 
    i.ingredient_id,
    i.name,
    i.family,
    COUNT(DISTINCT ri.recipe_id) as recipe_count,
    i.is_active
FROM ingredients i
LEFT JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id AND ri.is_active = 1
WHERE i.is_active = 1
GROUP BY i.ingredient_id
ORDER BY recipe_count DESC
LIMIT 10;

.print ===============================================
.print TOP 10 MOST COMMON ALLERGENS
.print ===============================================

SELECT 
    a.allergen_id,
    a.name,
    a.type,
    COUNT(DISTINCT ra.recipe_id) as recipe_count,
    ROUND(100.0 * COUNT(DISTINCT ra.recipe_id) / (SELECT COUNT(*) FROM recipes WHERE is_active = 1), 2) as pct_of_active_recipes
FROM allergens a
LEFT JOIN recipe_allergens ra ON a.allergen_id = ra.allergen_id AND ra.is_active = 1
WHERE a.is_active = 1
GROUP BY a.allergen_id
ORDER BY recipe_count DESC
LIMIT 10;

.print ===============================================
.print RECIPES BY INGREDIENT FAMILY
.print ===============================================

SELECT 
    i.family,
    COUNT(DISTINCT ri.recipe_id) as recipe_count,
    COUNT(DISTINCT i.ingredient_id) as unique_ingredients
FROM ingredients i
LEFT JOIN recipe_ingredients ri ON i.ingredient_id = ri.ingredient_id AND ri.is_active = 1
WHERE i.is_active = 1 AND i.family IS NOT NULL
GROUP BY i.family
ORDER BY recipe_count DESC;

.print ===============================================
.print MENU COMPOSITION
.print ===============================================

SELECT 
    m.id,
    m.year_week,
    m.start_date,
    COUNT(DISTINCT mr.recipe_id) as recipe_count,
    m.is_active,
    m.first_seen_date,
    m.last_seen_date
FROM menus m
LEFT JOIN menu_recipes mr ON m.id = mr.menu_id AND mr.is_active = 1
WHERE m.is_active = 1
GROUP BY m.id
ORDER BY m.start_date DESC
LIMIT 20;

.print ===============================================
.print RECIPE DIFFICULTY DISTRIBUTION
.print ===============================================

SELECT 
    CASE 
        WHEN difficulty <= 1 THEN 'Easy (0-1)'
        WHEN difficulty <= 2 THEN 'Medium (1-2)'
        WHEN difficulty <= 3 THEN 'Hard (2-3)'
        ELSE 'Very Hard (3+)'
    END as difficulty_level,
    COUNT(*) as recipe_count,
    ROUND(AVG(CAST(REPLACE(prep_time, ' mins', '') AS REAL)), 1) as avg_prep_time_mins
FROM recipes
WHERE is_active = 1 AND difficulty IS NOT NULL
GROUP BY difficulty_level
ORDER BY difficulty;

.print ===============================================
.print RECIPE-INGREDIENT MANY-TO-MANY SAMPLE
.print ===============================================

SELECT 
    r.name as recipe,
    i.name as ingredient,
    ri.quantity,
    ri.unit,
    ri.position
FROM recipe_ingredients ri
JOIN recipes r ON ri.recipe_id = r.id
JOIN ingredients i ON ri.ingredient_id = i.ingredient_id
WHERE ri.is_active = 1
LIMIT 15;

.print ===============================================
.print RECIPE CUISINES BREAKDOWN
.print ===============================================

SELECT 
    cuisine,
    COUNT(*) as recipe_count,
    ROUND(AVG(difficulty), 2) as avg_difficulty,
    ROUND(AVG(CAST(REPLACE(prep_time, ' mins', '') AS REAL)), 1) as avg_prep_time_mins
FROM recipes
WHERE is_active = 1 AND cuisine IS NOT NULL
GROUP BY cuisine
ORDER BY recipe_count DESC;

.print ===============================================
.print INGREDIENT USAGE BY RECIPE
.print ===============================================

SELECT 
    r.id,
    r.name,
    COUNT(DISTINCT ri.ingredient_id) as ingredient_count
FROM recipes r
LEFT JOIN recipe_ingredients ri ON r.id = ri.recipe_id AND ri.is_active = 1
WHERE r.is_active = 1
GROUP BY r.id
ORDER BY ingredient_count DESC
LIMIT 15;
