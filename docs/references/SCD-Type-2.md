# SCD Type 2: Slowly Changing Dimensions Explained

## What is SCD Type 2?

**SCD Type 2** is a data warehousing pattern for tracking **how entities change over time** by keeping **full history** of all changes.

Instead of updating records in place (which loses history), you create **new rows** for each change while keeping the old versions.

---

## The Problem SCD Type 2 Solves

### Without SCD Type 2 (Simple Update)

Imagine a recipe in your HelloFresh database:

**Week 1:**
```sql
recipe_id | name              | difficulty | last_updated
----------|-------------------|------------|-------------
R123      | Chicken Tikka     | 2          | 2026-01-15
```

**Week 2** - HelloFresh changes the difficulty:
```sql
recipe_id | name              | difficulty | last_updated
----------|-------------------|------------|-------------
R123      | Chicken Tikka     | 3          | 2026-01-22  ← Updated in place
```

**Problem:** You've **lost** the historical fact that this recipe was difficulty 2 for a week. If someone asks "What was the average difficulty in January?", your data lies.

---

## With SCD Type 2 (Keep History)

Instead of updating, you keep **both versions**:

**Week 1:**
```sql
recipe_id | name          | difficulty | first_seen | last_seen  | is_active
----------|---------------|------------|------------|------------|----------
R123      | Chicken Tikka | 2          | 2026-01-15 | 2026-01-22 | FALSE
```

**Week 2** - Add new row, mark old row inactive:
```sql
recipe_id | name          | difficulty | first_seen | last_seen  | is_active
----------|---------------|------------|------------|------------|----------
R123      | Chicken Tikka | 2          | 2026-01-15 | 2026-01-22 | FALSE
R123      | Chicken Tikka | 3          | 2026-01-22 | 9999-12-31 | TRUE
```

Now you can answer:
- "What's the current difficulty?" → 3 (is_active = TRUE)
- "What was the difficulty on 2026-01-20?" → 2 (between first_seen and last_seen)
- "When did it change?" → 2026-01-22

---

## Your HelloFresh Use Case

The HelloFresh API returns recipes **weekly**. Things that can change:

| Entity | What Might Change |
|--------|-------------------|
| **Recipe** | Name, description, difficulty, ingredients, prep time |
| **Menu** | Which recipes are included |
| **Ingredient** | Name, type, availability |
| **Recipe-Ingredient** | Quantity, unit |

With SCD Type 2, you can analyze:
- "How long did Recipe X stay on the menu?"
- "When did they change the ingredient from butter to olive oil?"
- "Which recipes have been increasing in difficulty over time?"

---

## Simplified SCD Type 2 Implementation

The blueprint uses a **lightweight version** suitable for weekly API pulls:

### Key Fields

```sql
CREATE TABLE recipes (
  recipe_id STRING,
  name STRING,
  difficulty INT,
  -- SCD Type 2 tracking
  first_seen_date DATE,      -- When we first saw this version
  last_seen_date DATE,       -- When we last saw this version
  is_active BOOLEAN          -- Is this the current version?
)
```

### Weekly Update Logic (Simplified)

```python
# Every week when you pull fresh data:

1. For each recipe in the API response:
   
   a. Check if it exists in Silver with is_active = TRUE
   
   b. If NO (new recipe):
      → Insert new row with:
        - first_seen_date = today
        - last_seen_date = '9999-12-31'
        - is_active = TRUE
   
   c. If YES (existing recipe):
      - Has anything changed (name, difficulty, etc.)?
      
      → If UNCHANGED:
        - Update last_seen_date = today (still active)
      
      → If CHANGED:
        - Update old row: last_seen_date = today, is_active = FALSE
        - Insert new row: first_seen_date = today, is_active = TRUE

2. For recipes NOT in this week's API response:
   → Update: last_seen_date = today, is_active = FALSE
   (Recipe disappeared from API)
```

---

## Concrete Example: Recipe Changes Over Time

### Week 1 (2026-01-15) - API Returns:
```json
{
  "id": "R123",
  "name": "Chicken Tikka Masala",
  "difficulty": 2,
  "prep_time": 30
}
```

**Silver Table:**
```sql
recipe_id | name                  | difficulty | prep_time | first_seen | last_seen  | is_active
----------|----------------------|------------|-----------|------------|------------|----------
R123      | Chicken Tikka Masala | 2          | 30        | 2026-01-15 | 9999-12-31 | TRUE
```

---

### Week 2 (2026-01-22) - API Returns:
```json
{
  "id": "R123",
  "name": "Chicken Tikka Masala",
  "difficulty": 2,
  "prep_time": 30
}
```
**No change** → Just update `last_seen_date`:

```sql
recipe_id | name                  | difficulty | prep_time | first_seen | last_seen  | is_active
----------|----------------------|------------|-----------|------------|------------|----------
R123      | Chicken Tikka Masala | 2          | 30        | 2026-01-15 | 2026-01-22 | TRUE
```

---

### Week 3 (2026-01-29) - API Returns:
```json
{
  "id": "R123",
  "name": "Chicken Tikka Masala",
  "difficulty": 3,           ← CHANGED!
  "prep_time": 35            ← CHANGED!
}
```

**Change detected** → Close old row, insert new row:

```sql
recipe_id | name                  | difficulty | prep_time | first_seen | last_seen  | is_active
----------|----------------------|------------|-----------|------------|------------|----------
R123      | Chicken Tikka Masala | 2          | 30        | 2026-01-15 | 2026-01-29 | FALSE ← Closed
R123      | Chicken Tikka Masala | 3          | 35        | 2026-01-29 | 9999-12-31 | TRUE  ← New
```

---

### Week 4 (2026-02-05) - API Returns:
```json
{
  "id": "R456",
  "name": "Beef Tacos",
  "difficulty": 1,
  "prep_time": 20
}
```

**R123 disappeared!** → Mark inactive:

```sql
recipe_id | name                  | difficulty | prep_time | first_seen | last_seen  | is_active
----------|----------------------|------------|-----------|------------|------------|----------
R123      | Chicken Tikka Masala | 2          | 30        | 2026-01-15 | 2026-01-29 | FALSE
R123      | Chicken Tikka Masala | 3          | 35        | 2026-01-29 | 2026-02-05 | FALSE ← Marked inactive
R456      | Beef Tacos           | 1          | 20        | 2026-02-05 | 9999-12-31 | TRUE  ← New recipe
```

---

## Querying SCD Type 2 Data

### Get Current State
```sql
SELECT * 
FROM recipes 
WHERE is_active = TRUE;
```

### Get Historical State (Point-in-Time)
```sql
-- What recipes existed on 2026-01-20?
SELECT * 
FROM recipes 
WHERE first_seen_date <= '2026-01-20' 
  AND last_seen_date >= '2026-01-20';
```

### Recipe Lifecycle Analysis
```sql
-- How long did each recipe stay active?
SELECT 
  recipe_id,
  name,
  first_seen_date,
  last_seen_date,
  DATEDIFF(day, first_seen_date, last_seen_date) as days_active
FROM recipes
WHERE is_active = FALSE
ORDER BY days_active DESC;
```

### Detect Changes
```sql
-- Find recipes that changed difficulty
SELECT 
  r1.recipe_id,
  r1.name,
  r1.difficulty as old_difficulty,
  r2.difficulty as new_difficulty,
  r2.first_seen_date as change_date
FROM recipes r1
JOIN recipes r2 
  ON r1.recipe_id = r2.recipe_id
WHERE r1.last_seen_date = r2.first_seen_date
  AND r1.difficulty != r2.difficulty;
```

---

## Why This Matters for Your Project

The blueprint's **first downstream project** is explicitly designed to **only work with SCD Type 2 data**:

### Recipe and Menu Drift Analysis

**Questions that require history:**
1. "How stable are menus week-to-week?"  
   → Need to compare recipe sets across time
   
2. "Which recipes persist longest?"  
   → Need `first_seen_date` and `last_seen_date`
   
3. "Which ingredients are increasingly common?"  
   → Need to track ingredient frequency over time

**Core metrics requiring SCD Type 2:**
- `recipe_survival_weeks` = `last_seen_date - first_seen_date`
- `menu_overlap_ratio` = Compare active recipes week-to-week
- `ingredient_trend_slope` = Linear regression on weekly counts

---

## Alternative: What if You DON'T Use SCD Type 2?

### Option 1: Simple Updates (Bad)
```sql
UPDATE recipes SET difficulty = 3 WHERE recipe_id = 'R123';
```
- ❌ Lose all history
- ❌ Can't answer "when did this change?"
- ❌ Can't do trend analysis

### Option 2: Full Snapshots (Wasteful)
Save entire database weekly:
- `recipes_2026_01_15`
- `recipes_2026_01_22`
- `recipes_2026_01_29`

- ✅ Keep history
- ❌ Massive duplication (99% of data unchanged)
- ❌ Hard to query across weeks
- ❌ Expensive storage

### Option 3: SCD Type 2 (Goldilocks)
- ✅ Keep full history
- ✅ Minimal duplication (only changed rows)
- ✅ Easy to query current and historical state
- ✅ Industry standard pattern

---

## Summary

**SCD Type 2** = Keep **all versions** of changing data by:
1. Adding `first_seen_date`, `last_seen_date`, `is_active` columns
2. Inserting new rows when data changes (instead of updating)
3. Marking old rows as inactive

**For your HelloFresh project:**
- Weekly API pulls naturally fit SCD Type 2
- Enables powerful historical analysis
- Answers "what changed and when?"
- Makes your downstream analytics meaningful

**Mental model:** Think of it like Git commits for your data. Each "version" of a recipe gets its own row, so you can always see what it looked like at any point in time.

SCD Type 2 stands for Slowly Changing Dimension Type 2 - it's a way to track how your data changes over time by keeping every version, not just the latest.
The key insight: Instead of updating a row when something changes (which erases history), you insert a new row and mark the old one as inactive.
For your HelloFresh project, this means:

When a recipe's difficulty changes from 2 to 3, you keep both versions
When a recipe disappears from the menu, you record when it was last seen
You can answer questions like "How many weeks was this recipe active?" or "What recipes were on the menu in January?"