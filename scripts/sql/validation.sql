## Schema Validation Queries

```sql
-- Verify all tables exist
SELECT COUNT(*) FROM recipes;
SELECT COUNT(*) FROM ingredients;
SELECT COUNT(*) FROM menus;
SELECT COUNT(*) FROM weekly_menu_metrics;

-- Check SCD tracking is active
SELECT COUNT(DISTINCT recipe_id) FROM recipes WHERE is_active = 1;
SELECT DATE(MAX(last_seen_date)) FROM recipes;

-- Verify relationships
SELECT COUNT(*) FROM recipe_ingredients;
SELECT COUNT(*) FROM menu_recipes;

-- Check gold analytics
SELECT COUNT(*) FROM weekly_menu_metrics;
SELECT COUNT(DISTINCT week_start_date) FROM ingredient_trends;
```

### Query the Database
```bash
sqlite3 hfresh/hfresh.db
sqlite> SELECT COUNT(*) FROM recipes WHERE is_active = 1;
sqlite> SELECT DISTINCT week_start_date FROM weekly_menu_metrics ORDER BY week_start_date DESC;
```