.mode column
.headers on

.print RECIPES BY WEEK
SELECT 
    m.start_date as week,
    COUNT(DISTINCT mr.recipe_id) as recipe_count
FROM menus m
LEFT JOIN menu_recipes mr ON m.id = mr.menu_id AND mr.is_active = 1
WHERE m.is_active = 1
GROUP BY m.start_date
ORDER BY m.start_date;

.print ""
.print RECIPES SPANNING MULTIPLE WEEKS

SELECT 
    r.id,
    r.name,
    COUNT(DISTINCT m.start_date) as week_count
FROM recipes r
JOIN menu_recipes mr ON r.id = mr.recipe_id AND mr.is_active = 1
JOIN menus m ON mr.menu_id = m.id AND m.is_active = 1
GROUP BY r.id
HAVING COUNT(DISTINCT m.start_date) > 1
ORDER BY week_count DESC
LIMIT 15;

.print ""
.print RECIPES WITH ONLY 1 WEEK

SELECT COUNT(*) as recipes_with_only_one_week
FROM (
    SELECT r.id
    FROM recipes r
    JOIN menu_recipes mr ON r.id = mr.recipe_id AND mr.is_active = 1
    JOIN menus m ON mr.menu_id = m.id AND m.is_active = 1
    GROUP BY r.id
    HAVING COUNT(DISTINCT m.start_date) = 1
);
