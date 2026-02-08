## Summary of the Gold Layer

I want to understand scripts\3_gold_analytics.py


The gold analytics layer creates **5 analytical tables** from your silver layer data:

| Table | Purpose |
|-------|---------|
| `weekly_menu_metrics` | Menu composition metrics per week (total recipes, new/returning, avg difficulty, prep time) |
| `recipe_survival_metrics` | Recipe lifespan (when recipes appear/disappear, active status, consistency) |
| `ingredient_trends` | Ingredient popularity rankings over time |
| `menu_stability_metrics` | Week-over-week changes (churn rate, retention, new recipes added/removed) |
| `allergen_density` | Allergen coverage per week (what % of menu contains each allergen) |

## How to Verify Tables Are Generating

To check if your tables are populated correctly, run the validation SQL:

```bash
sqlite3 hfresh/hfresh.db < scripts/sql_queries/06_gold_layer_validation.sql
```

This will show:
- **Row counts** for each table
- **Data completeness** (non-null values, date ranges)
- **Recipe survival stats** (active/inactive counts, weeks active)
- **Coverage metrics** (unique ingredients/allergens tracked)

## What would help you most?

1. **Understand a specific computation** (like menu stability or allergen calculation)?



