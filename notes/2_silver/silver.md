### silver

how is scd 2 done in silver?
SCD Type 2 pattern (implementation notes)
- Each entity row tracks lifecycle: `first_seen_date`, `last_seen_date`, `is_active`.
- Pattern: on each run, compare current API entities →
	- UPDATE existing rows' `last_seen_date` and `is_active` as needed
	- INSERT rows that are new with `first_seen_date = last_seen_date = run_week`
	- Mark rows not present in current payload as `is_active = FALSE` (and set `last_seen_date`)

Database layout (developer view)
- Single DB: `hfresh/hfresh.db` (14 tables total)
- Bronze: `api_responses` (raw JSON, append-only)
- Silver: normalized entity tables (recipes, ingredients, allergens, tags, labels, menus) and bridge tables (recipe_ingredients, recipe_allergens, recipe_tags, recipe_labels, menu_recipes)

