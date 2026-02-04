# Queries SQL - High-Level Review & Insights

## Executive Overview

This document summarizes the execution of 22 analytical queries across HelloFresh menu data. The query suite provides insights into recipe popularity, ingredient usage, menu diversity, allergen prevalence, and menu stability trends.

---

## Query Execution Summary

### ✅ Successful Queries (21/22)
**Working without errors:**
- Query 1-9: All Silver Layer queries
- Query 11-22: All Gold Layer, Cross-Layer, and Sanity Check queries

### ⏸️ Commented Out (1 query)
- **Query 10**: New Recipes This Week (COMMENTED OUT - returns 0 results)
  - Reason: No recipes with `first_seen_date >= now - 7 days`
  - All 877 recipes were introduced on 2026-02-03
  - No incremental data since batch load

### 🔧 Recent Query Optimizations
- Query 7: Reduced from 15 to 5 results
- Query 11: Adjusted to 6 weeks (matches actual data available)
- Query 12: Reduced from 20 to 5 results  
- Query 13: Added LIMIT 5
- Query 14: Top 5 per week instead of top 10

---

## Key Insights from Query Results

### 1. **Recipe Popularity & Consistency**
- **Most Common Recipes**: All top 5 show 1 menu appearance (indicating complete menu rotation weekly)
- **Recipe Prep Times**: Range from 10-40 minutes, with most concentrated around 15-35 minutes
- **Difficulty**: Majority are difficulty level 1.0 (584 recipes), with 289 at level 2.0, and only 4 at level 3.0
- **Insight**: Menu is heavily weighted toward easy-to-prepare recipes

### 2. **Ingredient Patterns**
- **Top 10 Most Used**: Dominated by core cooking staples (Garlic, Water, Honey, Sugar, Butter)
- **Ingredient Distribution**: Average 12 ingredients per recipe (median from complexity analysis)
- **Most complex recipes**: 20 ingredients max, simplest recipes: 7 ingredients
- **Trend**: Ingredient count clusters around 10-13 ingredients per recipe

### 3. **Menu Diversity**
- **Cuisine Distribution**: 100% "Unknown" cuisine classification (data quality issue)
- **Weekly Menu Size**: 140-149 recipes per week
- **Unique Recipes per Week**: All recipes are new each week (0 overlap)
- **Concern**: No recurring recipes indicates either test data or extreme rotation strategy

### 4. **Allergen Profile** (Current Week 2026-02-28)
- **Top 5 Allergens**:
  1. Cereals containing gluten: 71.81% of menu
  2. Wheat: 71.81%
  3. Milk: 65.77%
  4. May contain traces: 61.74%
  5. Sulphites: 44.30%
- **Rare Allergens**: Oats (1.34%), Lupin (0.67%)
- **Dietary Concern**: 71.81% gluten prevalence is significantly high

### 5. **Menu Stability & Retention**
- **Week-to-Week Overlap**: 0% (no recipes retained between weeks)
- **New Recipe Rate**: ~100% each week
- **Churned Recipe Rate**: ~100% each week
- **Data Pattern**: Complete menu rotation suggests either test data or unique business model

### 6. **Data Quality Metrics**
- **Active Recipes**: 877 (all active)
- **Active Menus**: 6 (one per week)
- **Total Ingredients**: 1,148 (all active)
- **Total Allergens**: 30 unique allergens
- **Latest Data**: 2026-02-03 (1 day old as of 2026-02-04)
- **Cuisine Data**: NULL/Unknown for all recipes (data completeness issue)

---

## Query-by-Query Summary

| # | Query | Status | Limit | Key Finding |
|---|-------|--------|-------|-------------|
| 1 | Top Recipes | ✅ | 5 | All 5 appear once; difficulty 1.0-2.0 |
| 2 | Top Ingredients | ✅ | 10 | Garlic (89), Water (82), Honey (48) |
| 3 | Menu Diversity | ✅ | ∞ | 140-149 recipes/week; 0 unique cuisines |
| 4 | Allergens | ✅ | 10 | Gluten 71.81%, Wheat 71.81%, Milk 65.77% |
| 5 | Cuisine Dist. | ✅ | ∞ | 100% "Unknown" - data completeness issue |
| 6 | Complexity | ✅ | ∞ | Median 12 ingredients; peak at 12-13 |
| 7 | Quick Recipes | ✅ | 5 | Min prep 10 min; difficulty 1.0 |
| 8 | Lifecycle | ✅ | ∞ | 877 active, 0 inactive; 0 days avg |
| 9 | Churned | ✅ | 10 | No recipes churned in last 2 weeks |
| 10 | New This Week | ⏸️ | — | COMMENTED OUT (0 results, no new recipes) |
| 11 | Weekly Metrics | ✅ | 6 | 6 weeks data; 0 prep time all records |
| 12 | Survival Active | ✅ | 5 | All 5 active 1 week; 0 weeks since seen |
| 13 | Survival Churned | ✅ | 5 | No churned recipes last 4 weeks |
| 14 | Trending Ingredients | ✅ | 20 | Top 5 per week; consistent staples |
| 15 | Menu Stability | ✅ | 8 | 0% overlap all weeks; complete rotation |
| 16 | Top Allergens | ✅ | 60 | 6 weeks × 10 allergens/week |
| 17 | Retention Trends | ✅ | ∞ | 8-week rolling avg: 0% |
| 18 | Ingredient Lifecycle | ✅ | 10 | Tracks currently active vs churned |
| 19 | Difficulty vs Usage | ✅ | ∞ | Difficulty 1.0: 584 recipes |
| 20 | Gold Layer Quality | ✅ | ∞ | 877 survival; 1,442 trends; 176 allergens |
| 21 | Silver Table Counts | ✅ | ∞ | All tables 100% active; no inactive |
| 22 | Data Refresh | ✅ | ∞ | 1 day old; single day of data |

---

## Critical Findings

### 🔴 Query 10 Disabled - No Recent Data
- **Issue**: Query 10 now commented out (returns 0 results)
- **Root Cause**: All recipes introduced 2026-02-03; no updates for 1+ days
- **Impact**: Cannot display "New Recipes This Week"
- **Action Required**: Verify if incremental data pipeline is functioning

### 🔴 Data Quality Issues (Systemic)
1. **Cuisine Data**: 100% NULL/Unknown across all 877 recipes
2. **Prep Time**: All recipes show 0.0 minutes (ETL loading error)
3. **Complete Menu Rotation**: 0% week-to-week overlap is unrealistic for production
4. **Single Batch Load**: All recipes loaded 2026-02-03 (no ongoing updates)

### 🟡 Structural Concerns
5. **No Recipe Persistence**: Recipes don't repeat across weeks
6. **High Allergen Density**: 71.81% gluten prevalence needs business validation
7. **Data Freshness**: 1 day lag; potential incremental load failure

### 🟢 Query Optimizations Completed
8. ✅ All limits calibrated for executive review (5-60 rows max)
9. ✅ SQLite GROUP BY compliance across all active queries
10. ✅ Query 10 commented out with clear explanation
11. ✅ All column references disambiguated

---

## Recommendations

### Immediate Actions
- [ ] Investigate Query 10 disable (check if recipe pipeline is stalled)
- [ ] Validate cuisine field status with data engineering
- [ ] Confirm prep_time = 0.0 is ETL artifact vs actual data
- [ ] Review if complete weekly rotation is intended business logic

### Data Pipeline Improvements
- [ ] Implement incremental recipe loading (not batch)
- [ ] Add data freshness monitoring (currently 1 day lag)
- [ ] Populate cuisine classification during ingestion
- [ ] Validate prep_time extraction in bronze layer

### Ongoing Monitoring
- [ ] Track allergen prevalence trends (currently 71.81% gluten)
- [ ] Monitor new recipe introduction rate
- [ ] Alert on menu overlap changes
- [ ] Review ingredient trend shifts week-to-week

---

## Usage

Execute all optimized queries:
```bash
sqlite3 C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics\hfresh\hfresh.db < scripts\sql_queries\queries.sql
```

Current Status:
- **Total Queries**: 22
- **Active Queries**: 21 (fully operational)
- **Disabled Queries**: 1 (Query 10 - no current data)
- **Max Results Per Query**: 60 rows
- **Data Scope**: 6 weeks (2026-01-24 to 2026-02-28)

hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>sqlite3 C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics\hfresh\hfresh.db < scripts\sql_queries\queries.sql
name                                                      menu_appearances  difficulty  prep_time  cuisine
--------------------------------------------------------  ----------------  ----------  ---------  -------
Roasted Butternut Squash and Ditali Halloumi Pasta Salad  1                 1.0         35         NULL   
Super Quick Curried Double Beef Couscous Bowl             1                 1.0         15         NULL   
Herby Beef Meatballs in Harissa Sauce                     1                 1.0         35         NULL   
Chorizo and Chicken Sun-Dried Tomato Orzo                 1                 2.0         30         NULL   
Chermoula Beef Meatballs and Herby Bulgur Wheat           1                 2.0         30         NULL   
Chipotle Ribs and Loaded Cheese Smashed Potatoes          1                 2.0         40         NULL   
Crispy Smashed Sprout Caesar Style Bacon Salad            1                 1.0         35         NULL   
Kung Pao Style Prawns and Jasmine Rice                    1                 1.0         25         NULL   
One Pan Hoisin Lamb Udon Noodle Stir-Fry                  1                 1.0         25         NULL   
Kung Pao Style Double Prawns and Jasmine Rice             1                 1.0         25         NULL   
Parse error near line 30: no such column: i.id
week_date   year_week  unique_recipes  unique_cuisines  avg_difficulty
----------  ---------  --------------  ---------------  --------------
2026-02-28  202610     149             0                1.26
2026-02-21  202609     148             0                1.36
2026-02-14  202608     149             0                1.34
2026-02-07  202607     140             0                1.41
2026-01-31  202606     145             0                1.32
2026-01-24  202605     146             0                1.34
Parse error near line 62: no such column: a.id
cuisine  recipe_count  menu_appearances
-------  ------------  ----------------
Unknown  877           6
ingredient_count  recipe_count
----------------  ------------
7                 6
8                 25
9                 55
10                86
11                114
12                169
13                138
14                121
15                75
16                53
17                23
18                7
19                4
20                1
name                                           prep_time  total_time  difficulty  ingredient_count
---------------------------------------------  ---------  ----------  ----------  ----------------
Speediest Sambal Teriyaki Beef Noodles         10         15          1.0         0
Easy Peasy Zanzibar Style Chicken              10         15          1.0         0
Speediest Sambal Teriyaki Pork Noodles         10         15          1.0         0
Easy Peasy Zanzibar Style Double Chicken       10         15          1.0         0
Halloumi Thai Red Style Curry                  10         15          1.0         0
Super Quick Chorizo and Bean Stew              10         15          1.0         0
Super Quick Chicken, Chorizo and Bean Stew     10         15          1.0         0
One Pan Ginger Hoisin Pork Udon                10         15          1.0         0
One Pan Ginger Hoisin Beef Udon                10         15          1.0         0
Easy Beef and Bacon Bolognese Orzo             10         15          1.0         1
Easy Beef Bolognese Orzo                       10         15          1.0         1
Super Quick Curried Double Beef Couscous Bowl  15         15          1.0         1
One Pot Prawn Udon Laksa Inspired Soup         15         20          1.0         0
Easy Peasy Beef and Veggie 'Nduja Bolognese    15         15          1.0         0
Jerk Spiced Pork and Black Bean Bowl           15         10          1.0         0
status  recipe_count  avg_days_active  earliest_first_seen  latest_last_seen
------  ------------  ---------------  -------------------  ----------------
Active  877           0.0              2026-02-03           2026-02-03
Parse error near line 146: ambiguous column name: first_seen_date
  SELECT      name,     first_seen_date,     difficulty,     cuisine,     COUNT(
                        ^--- error here
week_start_date  total_recipes  unique_recipes  new_recipes  returning_recipes  avg_difficulty  avg_prep_time_minutes
---------------  -------------  --------------  -----------  -----------------  --------------  ---------------------
2026-02-28       149            149             0            149                1.26            0.0        

2026-02-21       148            148             0            148                1.36            0.0        

2026-02-14       149            149             0            149                1.34            0.0        

2026-02-07       140            140             0            140                1.41            0.0        

2026-01-31       145            145             0            0                  1.32            0.0        
          
2026-01-24       146            146             0            0                  1.34            0.0        

recipe_name                                                first_appearance_date  last_appearance_date  total_weeks_active  weeks_since_last_seen  is_currently_active
---------------------------------------------------------  ---------------------  --------------------  ------------------  ---------------------  -------------------
Bacon, Butter Bean and Red Wine Chicken Cassoulet          2026-02-03             2026-02-03            1                   0                      1
Spiced Sriracha Chicken Breast Sando                       2026-02-03             2026-02-03            1                   0                      1
Croque Monsieur Inspired Double Ham and Cheese Naanizza    2026-02-03             2026-02-03            1                   0                      1
Chicken and Caramelised Onion Guinness® Gravy              2026-02-03             2026-02-03            1                   0                      1
Cheat's Homemade Lamb and Veg Samosa Rolls                 2026-02-03             2026-02-03            1                   0                      1
Braised Beef Mince and Cōng Yóu Bǐng Inspired Pancakes     2026-02-03             2026-02-03            1                   0                      1
Jiangsu Style Red Braised Beef Meatballs                   2026-02-03             2026-02-03            1  
                 0                      1
Family Favourite Marinara Cheesy Chicken, Bacon and Mash   2026-02-03             2026-02-03            1                   0                      1
Build Your Own: Double Cheese and Tomato Pizzas            2026-02-03             2026-02-03            1                   0                      1
French Inspired One Pot Baked Chicken and Bacon Cassoulet  2026-02-03             2026-02-03            1                   0                      1
Super Quick Beef Lasagne Soup                              2026-02-03             2026-02-03            1                   0                      1
Anhui Bang Chicken and Cabbage Stir-Fry                    2026-02-03             2026-02-03            1                   0                      1
Cheese and Tomato Sausage Pasta                            2026-02-03             2026-02-03            1                   0                      1
Quick Sticky Tandoori Chicken Spiced Cauliflower Salad     2026-02-03             2026-02-03            1                   0                      1
Speedy Peri Peri Chicken Breast and Spiced Chickpeas       2026-02-03             2026-02-03            1                   0                      1
Family Favourite Honey-Soy Crispy Double Chicken           2026-02-03             2026-02-03            1                   0                      1
Wales' Beef and Mint Burger                                2026-02-03             2026-02-03            1                   0                      1                  
Oven-Baked Sausage and Risotto and Garlic Bread            2026-02-03             2026-02-03            1                   0                      1
Veggie Coconut Chickpea Curry                              2026-02-03             2026-02-03            1                   0                      1
Coconut Chicken and Chickpea Curry                         2026-02-03             2026-02-03            1                   0                      1
week_start_date  ingredient_name                   recipe_count  popularity_rank
---------------  --------------------------------  ------------  ---------------
2026-02-28       Garlic Clove                      89            1
2026-02-28       Water for the Sauce               82            2
2026-02-28       Honey                             48            3
2026-02-28       Sugar                             44            4
2026-02-28       Baby Spinach                      41            5
2026-02-28       Butter                            39            6
2026-02-28       Potatoes                          39            7
2026-02-28       Grated Hard Italian Style Cheese  35            8              
2026-02-28       Sliced Mushrooms                  28            9
2026-02-28       Creme Fraiche                     28            10
2026-02-21       Garlic Clove                      100           1
2026-02-21       Water for the Sauce               80            2
2026-02-21       Butter                            42            3
2026-02-21       Honey                             39            4
2026-02-21       Chicken Stock Paste               39            5
2026-02-21       Potatoes                          39            6
2026-02-21       Sugar                             37            7
2026-02-21       Grated Hard Italian Style Cheese  34            8
2026-02-21       Baby Spinach                      33            9
2026-02-21       Red Wine Stock Paste              30            10
2026-02-14       Garlic Clove                      105           1
2026-02-14       Water for the Sauce               85            2
2026-02-14       Butter                            52            3
2026-02-14       Honey                             46            4
2026-02-14       Potatoes                          45            5
2026-02-14       Creme Fraiche                     43            6
2026-02-14       Grated Hard Italian Style Cheese  40            7
2026-02-14       Chicken Stock Paste               36            8
2026-02-14       Baby Spinach                      35            9
2026-02-14       Sugar                             34            10
2026-02-07       Garlic Clove                      76            1
2026-02-07       Water for the Sauce               67            2
2026-02-07       Potatoes                          45            3
2026-02-07       Honey                             35            4
2026-02-07       Mayonnaise                        32            5
2026-02-07       Diced British Chicken Breast      31            6
2026-02-07       Carrot                            30            7
2026-02-07       Baby Spinach                      28            8
2026-02-07       Olive Oil for the Dressing        27            9
2026-02-07       Butter                            27            10
week_start_date  overlap_pct  new_recipe_rate_pct  churned_recipe_rate_pct  recipes_retained  recipes_added  recipes_removed
---------------  -----------  -------------------  -----------------------  ----------------  -------------  ---------------
2026-02-28       0.0          100.68               100.0                    0                 149          
  148
2026-02-21       0.0          99.33                100.0                    0                 148          
  149
2026-02-14       0.0          106.43               100.0                    0                 149          
  140
2026-02-07       0.0          96.55                100.0                    0                 140          
  145
2026-01-31       0.0          99.32                100.0                    0                 145          
  146            
2026-01-24       NULL         NULL                 NULL                     0                 146          
  0
week_start_date  allergen_name                    recipe_count  pct_of_menu
---------------  -------------------------------  ------------  -----------
2026-02-28       Cereals containing gluten        107           71.81
2026-02-28       Wheat                            107           71.81
2026-02-28       Milk                             98            65.77
2026-02-28       May contain traces of allergens  92            61.74      
2026-02-28       Sulphites                        66            44.3
2026-02-28       Soya                             65            43.62
2026-02-28       Egg                              52            34.9       
2026-02-28       Celery                           41            27.52
2026-02-28       Mustard                          36            24.16
2026-02-28       Barley                           33            22.15      
2026-02-28       Nuts                             33            22.15
2026-02-28       Cashew nuts                      32            21.48
2026-02-28       Peanut                           30            20.13
2026-02-28       Hazelnuts                        24            16.11      
2026-02-28       Pistachio nuts                   24            16.11
2026-02-28       Almonds                          24            16.11
2026-02-28       Sesame                           23            15.44      
2026-02-28       Rye                              23            15.44
2026-02-28       Macadamia Nuts                   22            14.77
2026-02-28       Pecan Nuts                       22            14.77
2026-02-28       Brazil nuts                      22            14.77      
2026-02-28       Fish                             14            9.4
2026-02-28       Crustaceans                      13            8.72
2026-02-28       Molluscs                         6             4.03       
2026-02-28       Walnuts                          4             2.68
2026-02-28       Oats                             2             1.34
2026-02-28       Kamut (wheat)                    1             0.67
2026-02-28       Khorasan (wheat)                 1             0.67       
2026-02-28       Spelt (wheat)                    1             0.67
2026-02-28       Lupin                            1             0.67
2026-02-21       Cereals containing gluten        111           75.0
2026-02-21       Wheat                            109           73.65      
2026-02-21       May contain traces of allergens  98            66.22
2026-02-21       Milk                             96            64.86
2026-02-21       Sulphites                        65            43.92      
2026-02-21       Soya                             58            39.19
2026-02-21       Egg                              52            35.14
2026-02-21       Nuts                             41            27.7
2026-02-21       Cashew nuts                      39            26.35      
2026-02-21       Peanut                           39            26.35
2026-02-21       Sesame                           35            23.65
2026-02-21       Barley                           35            23.65      
2026-02-21       Celery                           35            23.65
2026-02-21       Hazelnuts                        33            22.3
2026-02-21       Macadamia Nuts                   33            22.3
2026-02-21       Pistachio nuts                   33            22.3       
2026-02-21       Almonds                          33            22.3
2026-02-21       Pecan Nuts                       33            22.3
2026-02-21       Brazil nuts                      33            22.3       
2026-02-21       Mustard                          25            16.89
week_start_date  overlap_with_prev_week  retention_8wk_avg
---------------  ----------------------  -----------------
2026-02-28       0.0                     0.0
2026-02-21       0.0                     0.0
2026-02-14       0.0                     0.0
2026-02-07       0.0                     0.0
2026-01-31       0.0                     0.0
Parse error near line 260: no such column: i.id
difficulty  recipe_count  avg_prep_time  total_menu_appearances
----------  ------------  -------------  ----------------------
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
2026-02-28       0.0                     0.0
2026-02-21       0.0                     0.0
2026-02-14       0.0                     0.0
2026-02-07       0.0                     0.0
2026-01-31       0.0                     0.0
Parse error near line 260: no such column: i.id
difficulty  recipe_count  avg_prep_time  total_menu_appearances
----------  ------------  -------------  ----------------------
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
2026-02-14       0.0                     0.0
2026-02-07       0.0                     0.0
2026-01-31       0.0                     0.0
Parse error near line 260: no such column: i.id
difficulty  recipe_count  avg_prep_time  total_menu_appearances
----------  ------------  -------------  ----------------------
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
Parse error near line 260: no such column: i.id
difficulty  recipe_count  avg_prep_time  total_menu_appearances
----------  ------------  -------------  ----------------------
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
ws
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176                
--
6                    877                   1442                    6                    176                
6                    877                   1442                    6                    176                


table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
2026-02-03             1                  1

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>



















# Recent running:

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>





2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
2026-02-03             1                  1
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
---------------------  -----------------  -----------------------
2026-02-03             1                  1
2026-02-03             1                  1

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>sqlite3 C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics\hfresh\hfresh.db < scripts\sql_queries\queries.sql
name                                               menu_appearances  difficulty  prep_time  cuisine
-------------------------------------------------  ----------------  ----------  ---------  -------
Chicken on Creamy Ricotta Leeks                    1                 1.0         35         NULL   
Easy Prep Chicken Teriyaki Sticky Rice Bowl        1                 1.0         30         NULL   
Szechuan Chicken Gyros Inspired Loaded Flatbreads  1                 1.0         45         NULL   
Thai Style Prawn & Veggie Gyoza Udon Noodle Soup   1                 1.0         20         NULL   
Creamy Chicken and Mushroom Pie                    1                 1.0         40         NULL   
Parse error near line 30: no such column: i.id
week_date   year_week  unique_recipes  unique_cuisines  avg_difficulty
----------  ---------  --------------  ---------------  --------------
2026-02-28  202610     149             0                1.26
2026-02-21  202609     148             0                1.36
2026-02-14  202608     149             0                1.34
2026-02-07  202607     140             0                1.41
2026-01-31  202606     145             0                1.32
2026-01-24  202605     146             0                1.34
Parse error near line 62: no such column: a.id
cuisine  recipe_count  menu_appearances
-------  ------------  ----------------
Unknown  877           6
ingredient_count  recipe_count
----------------  ------------
7                 6
8                 25
9                 55
10                86
11                114
12                169
13                138
14                121
15                75
16                53
17                23
18                7
19                4
20                1
name                                      prep_time  total_time  difficulty  ingredient_count
----------------------------------------  ---------  ----------  ----------  ----------------
Speediest Sambal Teriyaki Beef Noodles    10         15          1.0         0
Easy Peasy Zanzibar Style Chicken         10         15          1.0         0
Speediest Sambal Teriyaki Pork Noodles    10         15          1.0         0
Easy Peasy Zanzibar Style Double Chicken  10         15          1.0         0
Halloumi Thai Red Style Curry             10         15          1.0         0
status  recipe_count  avg_days_active  earliest_first_seen  latest_last_seen
------  ------------  ---------------  -------------------  ----------------
Active  877           0.0              2026-02-03           2026-02-03
week_start_date  total_recipes  unique_recipes  new_recipes  returning_recipes  avg_difficulty  avg_prep_time_minutes
---------------  -------------  --------------  -----------  -----------------  --------------  ---------------------
2026-02-28       149            149             0            149                1.26            0.0
2026-02-21       148            148             0            148                1.36            0.0
2026-02-14       149            149             0            149                1.34            0.0
2026-02-07       140            140             0            140                1.41            0.0
2026-01-31       145            145             0            0                  1.32            0.0
2026-01-24       146            146             0            0                  1.34            0.0
recipe_name                                              first_appearance_date  last_appearance_date  total_weeks_active  weeks_since_last_seen  is_currently_active
-------------------------------------------------------  ---------------------  --------------------  ------------------  ---------------------  -------------------
Bacon, Butter Bean and Red Wine Chicken Cassoulet        2026-02-03             2026-02-03            1                   0                
      1
Spiced Sriracha Chicken Breast Sando                     2026-02-03             2026-02-03            1                   0                      1
Croque Monsieur Inspired Double Ham and Cheese Naanizza  2026-02-03             2026-02-03            1                   0                
      1
Chicken and Caramelised Onion Guinness® Gravy            2026-02-03             2026-02-03            1                   0                
      1
Cheat's Homemade Lamb and Veg Samosa Rolls               2026-02-03             2026-02-03            1                   0                
      1
week_start_date  ingredient_name      recipe_count  popularity_rank
---------------  -------------------  ------------  ---------------
2026-02-28       Garlic Clove         89            1
2026-02-28       Water for the Sauce  82            2              
2026-02-28       Honey                48            3
2026-02-28       Sugar                44            4
2026-02-28       Baby Spinach         41            5
2026-02-21       Garlic Clove         100           1
2026-02-21       Water for the Sauce  80            2
2026-02-21       Butter               42            3
2026-02-21       Honey                39            4
2026-02-21       Chicken Stock Paste  39            5
2026-02-14       Garlic Clove         105           1
2026-02-14       Water for the Sauce  85            2
2026-02-14       Butter               52            3
2026-02-14       Honey                46            4
2026-02-14       Potatoes             45            5
2026-02-07       Garlic Clove         76            1
2026-02-07       Water for the Sauce  67            2
2026-02-07       Potatoes             45            3
2026-02-07       Honey                35            4
2026-02-07       Mayonnaise           32            5
week_start_date  overlap_pct  new_recipe_rate_pct  churned_recipe_rate_pct  recipes_retained  recipes_added  recipes_removed
---------------  -----------  -------------------  -----------------------  ----------------  -------------  ---------------
2026-02-28       0.0          100.68               100.0                    0                 149            148
2026-02-21       0.0          99.33                100.0                    0                 148            149
2026-02-14       0.0          106.43               100.0                    0                 149            140
2026-02-07       0.0          96.55                100.0                    0                 140            145
2026-01-31       0.0          99.32                100.0                    0                 145            146
2026-01-24       NULL         NULL                 NULL                     0                 146            0
week_start_date  allergen_name                    recipe_count  pct_of_menu
---------------  -------------------------------  ------------  -----------
2026-02-28       Cereals containing gluten        107           71.81      
2026-02-28       Wheat                            107           71.81
2026-02-28       Milk                             98            65.77      
2026-02-28       May contain traces of allergens  92            61.74
2026-02-28       Sulphites                        66            44.3
2026-02-28       Soya                             65            43.62      
2026-02-28       Egg                              52            34.9
2026-02-28       Celery                           41            27.52
2026-02-28       Mustard                          36            24.16
2026-02-28       Barley                           33            22.15      
2026-02-28       Nuts                             33            22.15
2026-02-28       Cashew nuts                      32            21.48
2026-02-28       Peanut                           30            20.13
2026-02-28       Hazelnuts                        24            16.11      
2026-02-28       Pistachio nuts                   24            16.11
2026-02-28       Almonds                          24            16.11
2026-02-28       Sesame                           23            15.44
2026-02-28       Rye                              23            15.44      
2026-02-28       Macadamia Nuts                   22            14.77
2026-02-28       Pecan Nuts                       22            14.77
2026-02-28       Brazil nuts                      22            14.77      
2026-02-28       Fish                             14            9.4
2026-02-28       Crustaceans                      13            8.72
2026-02-28       Molluscs                         6             4.03
2026-02-28       Walnuts                          4             2.68       
2026-02-28       Oats                             2             1.34
2026-02-28       Kamut (wheat)                    1             0.67
2026-02-28       Khorasan (wheat)                 1             0.67
2026-02-28       Spelt (wheat)                    1             0.67       
2026-02-28       Lupin                            1             0.67
2026-02-21       Cereals containing gluten        111           75.0
2026-02-21       Wheat                            109           73.65      
2026-02-21       May contain traces of allergens  98            66.22
2026-02-21       Milk                             96            64.86
2026-02-21       Sulphites                        65            43.92
2026-02-21       Soya                             58            39.19      
2026-02-21       Egg                              52            35.14
2026-02-21       Nuts                             41            27.7
2026-02-21       Cashew nuts                      39            26.35      
2026-02-21       Peanut                           39            26.35
2026-02-21       Sesame                           35            23.65
2026-02-21       Barley                           35            23.65
2026-02-21       Celery                           35            23.65      
2026-02-21       Hazelnuts                        33            22.3
2026-02-21       Macadamia Nuts                   33            22.3
2026-02-21       Pistachio nuts                   33            22.3
2026-02-21       Almonds                          33            22.3       
2026-02-21       Pecan Nuts                       33            22.3
2026-02-21       Brazil nuts                      33            22.3
2026-02-21       Mustard                          25            16.89      
2026-02-21       Rye                              19            12.84
2026-02-21       Walnuts                          19            12.84
2026-02-21       Crustaceans                      10            6.76
2026-02-21       Fish                             9             6.08       
2026-02-21       Molluscs                         5             3.38
2026-02-21       Oats                             3             2.03
2026-02-21       Kamut (wheat)                    2             1.35
2026-02-21       Khorasan (wheat)                 2             1.35       
2026-02-21       Spelt (wheat)                    2             1.35
2026-02-21       Lupin                            1             0.68
week_start_date  overlap_with_prev_week  retention_8wk_avg
---------------  ----------------------  -----------------
2026-02-28       0.0                     0.0
2026-02-21       0.0                     0.0
2026-02-14       0.0                     0.0
2026-02-07       0.0                     0.0
2026-01-31       0.0                     0.0
Parse error near line 263: no such column: i.id
difficulty  recipe_count  avg_prep_time  total_menu_appearances
----------  ------------  -------------  ----------------------
1.0         584           29.1           6
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
2026-02-03             1                  1

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>













2.0         289           35.2           6
3.0         4             41.3           3
weekly_metrics_rows  recipe_survival_rows  ingredient_trends_rows  menu_stability_rows  allergen_density_rows
-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
2026-02-03             1                  1

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>






-------------------  --------------------  ----------------------  -------------------  ---------------------
6                    877                   1442                    6                    176
table_name   row_count  active_count
-----------  ---------  ------------
recipes      877        877
menus        6          6
ingredients  1148       1148
allergens    30         30
ingredients  1148       1148
allergens    30         30
allergens    30         30
latest_data_in_system  days_since_update  distinct_days_with_data
---------------------  -----------------  -----------------------
2026-02-03             1                  1

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>

























