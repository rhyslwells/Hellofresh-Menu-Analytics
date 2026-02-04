# Analytics Insights Review

scripts\sql_queries\04_analytics_insights.sql
on 04/02/2026

Done! I've added a comprehensive review section to 04-insights.md that includes:

- **Executive Summary**: Overview of the 6-week data period and locales
- **5 Key Findings sections**: Menu stability, recipe longevity, ingredient trends, allergen profile, and menu health
- **Data Quality Observations**: Flags and anomalies (like 100% churn, zero prep times), plus strengths
- **Recommendations**: Actions to validate the data and address anomalies

The review highlights critical patterns like the complete weekly menu rotation (0% overlap), absence of any recipe survival across weeks, and the high prevalence of gluten/wheat allergens (71.81%). It also flags potential data quality issues like uniform prep times and suggests this may be test data based on the unrealistic churn pattern.

## Executive Summary

This report analyzes the HelloFresh menu data from 2026-01-24 through 2026-02-28 (6 weeks of data across the en-GB locale). The data reveals significant patterns in menu strategy, ingredient preferences, and allergen prevalence.

## Key Findings

### 1. **Menu Stability & Recipe Churn**
- **0% menu overlap week-to-week**: The menu undergoes complete rotation each week with zero recipe carryover
- **100% new recipe rate**: Every recipe is new each week with no returning recipes
- **Menu size**: 140-149 recipes per week (average ~147 recipes)
- **Total recipes ingested**: 877 recipes introduced on 2026-02-03
- **⚠️ Concern**: This extreme churn pattern suggests either test data or a very aggressive menu rotation strategy

### 2. **Recipe Longevity**
- **No survivor recipes**: All "top performer" recipes only appeared for 1 week (first_appearance = last_appearance = 2026-02-03)
- **No churned recipes**: Empty "Recently Churned" section indicates all active recipes are current
- **Analysis**: The data pattern suggests recipes are isolated to their introduction week with no persistence

### 3. **Ingredient Trends** (Current Week: 2026-02-28)
- **Top 5 ingredients** (by popularity rank):
  1. Garlic Clove (89 recipes, 59.7% of menu)
  2. Water for the Sauce (82 recipes, 55.0% of menu)
  3. Honey (48 recipes, 32.2% of menu)
  4. Sugar (44 recipes, 29.5% of menu)
  5. Baby Spinach (41 recipes, 27.5% of menu)
- **Ingredient consistency**: Most top ingredients show 0 week-over-week change (except newly introduced ones)
- **Trending variance**: "Water for the Rice" shows high volatility (5-19 recipes across weeks), "Wild Rocket" declining trend (29→13 recipes)

### 4. **Allergen Profile**
- **Most prevalent allergens** (% of current menu):
  - Cereals containing gluten: 71.81%
  - Wheat: 71.81% (correlates with gluten)
  - Milk: 65.77%
  - May contain traces of allergens: 61.74%
  - Sulphites: 44.30%
  - Soya: 43.62%
  - Egg: 34.90%

- **Allergen consistency**: All major allergens present across all 6 weeks with minimal variance
- **Gluten dominance**: Over 70% of menu contains gluten/wheat consistently
- **Low allergens**: Oats (2.06%), Lupin (only 2 weeks, 0.68%), Molluscs (2.94%)

### 5. **Menu Health Metrics**
- **Active recipes**: 877 (all marked currently active)
- **Active menus**: 6 (one per week)
- **Data completeness**: Full tracking available for all 6 weeks
- **Coverage**: All active recipes are in survival metrics tracking

## Data Quality Observations

### ⚠️ Flags & Anomalies:
1. **Perfect churn pattern**: 100% new recipes every week is unrealistic for production data
2. **Uniform prep times**: All recipes show 0.0 minutes prep time - likely data loading issue
3. **Single introduction date**: All 877 recipes introduced on 2026-02-03 suggests batch import
4. **No recipe churn**: "Recently Churned" section is empty - recipes don't accumulate beyond one week
5. **Test data hypothesis**: The complete menu rotation and uniform data values suggest this is test/synthetic data

### ✓ Strengths:
- Allergen tracking is comprehensive with 30+ allergen types
- Ingredient tracking granular and detailed
- Weekly metrics consistently populated
- Temporal data well-structured (week_start_date consistent)

## Recommendations

1. **Validate data source**: Confirm whether this is production data or test data
2. **Investigate prep time loading**: All 0.0 values suggests ETL issue in bronze/silver layer
3. **Review menu strategy**: If real, clarify the business logic behind 100% weekly rotation
4. **Consider recipe survival thresholds**: Current metrics show no multi-week recipes; may need different KPIs
5. **Monitor allergen trends**: Current gluten/wheat density (71.81%) should be validated for business requirements

---

## Detailed Query Output

(hellofresh-menu-analytics) C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics>sqlite3 hfresh/hfresh.db < scripts/sql_queries/04_analytics_insights.sql
===============================================
WEEKLY MENU METRICS SUMMARY
===============================================
week_start_date  locale  total_recipes  unique_recipes  new_recipes  returning_recipes  avg_difficulty  avg_prep_time_mins
---------------  ------  -------------  --------------  -----------  -----------------  --------------  ------------------
2026-02-28       en-GB   149            149             0            149                1.26            0.0
2026-02-21       en-GB   148            148             0            148                1.36            0.0
2026-02-14       en-GB   149            149             0            149                1.34            0.0
2026-02-07       en-GB   140            140             0            140                1.41            0.0
2026-01-31       en-GB   145            145             0            0                  1.32            0.0
2026-01-24       en-GB   146            146             0            0                  1.34        
    0.0
===============================================
RECIPE SURVIVAL METRICS - TOP PERFORMERS
===============================================
recipe_id  recipe_name                                                first_appearance_date  last_appearance_date  total_weeks_active  consecutive_weeks_active  is_currently_active  weeks_since_last_seen
---------  ---------------------------------------------------------  ---------------------  --------------------  ------------------  ------------------------  -------------------  ---------------------
296829     Bacon, Butter Bean and Red Wine Chicken Cassoulet          2026-02-03             2026-02-03            1                   1                         1                    0                 

296830     Spiced Sriracha Chicken Breast Sando                       2026-02-03             2026-02-03            1                   1                         1                    0                 

296831     Croque Monsieur Inspired Double Ham and Cheese Naanizza    2026-02-03             2026-02-03            1                   1                         1                    0                 

296832     Chicken and Caramelised Onion Guinness® Gravy              2026-02-03             2026-02-03            1                   1                         1                    0                 

296833     Cheat's Homemade Lamb and Veg Samosa Rolls                 2026-02-03             2026-02-03            1                   1                         1                    0                 

296834     Braised Beef Mince and Cōng Yóu Bǐng Inspired Pancakes     2026-02-03             2026-02-03            1                   1                         1                    0                 

296835     Jiangsu Style Red Braised Beef Meatballs                   2026-02-03             2026-02-03            1                   1                         1                    0                 

296836     Family Favourite Marinara Cheesy Chicken, Bacon and Mash   2026-02-03             2026-02-03            1                   1                         1                    0                 

296837     Build Your Own: Double Cheese and Tomato Pizzas            2026-02-03             2026-02-03            1                   1                         1                    0                 

296838     French Inspired One Pot Baked Chicken and Bacon Cassoulet  2026-02-03             2026-02-03            1                   1                         1                    0                 

296839     Super Quick Beef Lasagne Soup                              2026-02-03             2026-02-03            1                   1                         1                    0                 

296840     Anhui Bang Chicken and Cabbage Stir-Fry                    2026-02-03             2026-02-03            1                   1                         1                    0                 

296843     Cheese and Tomato Sausage Pasta                            2026-02-03             2026-02-03            1                   1                         1                    0                 

296844     Quick Sticky Tandoori Chicken Spiced Cauliflower Salad     2026-02-03             2026-02-03            1                   1                         1                    0                 

296845     Speedy Peri Peri Chicken Breast and Spiced Chickpeas       2026-02-03             2026-02-03            1                   1                         1                    0                 

===============================================
RECIPE SURVIVAL METRICS - RECENTLY CHURNED
===============================================
===============================================
INGREDIENT TRENDS - TOP MOVERS THIS WEEK
===============================================
ingredient_name                   week_start_date  recipe_count  week_over_week_change  popularity_rank
--------------------------------  ---------------  ------------  ---------------------  ---------------
Garlic Clove                      2026-02-28       89            0                      1           

Water for the Sauce               2026-02-28       82            0                      2           

Honey                             2026-02-28       48            0                      3           

Sugar                             2026-02-28       44            0                      4           

Baby Spinach                      2026-02-28       41            0                      5           

Butter                            2026-02-28       39            0                      6           

Potatoes                          2026-02-28       39            0                      7           

Grated Hard Italian Style Cheese  2026-02-28       35            0                      8           

Sliced Mushrooms                  2026-02-28       28            0                      9           

Creme Fraiche                     2026-02-28       28            0                      10          

Tomato Passata                    2026-02-28       27            0                      11          

Red Wine Stock Paste              2026-02-28       27            0                      12          

Onion                             2026-02-28       27            0                      13          

Tomato Puree                      2026-02-28       27            0                      14          

Soy Sauce                         2026-02-28       26            0                      15          

===============================================
INGREDIENT TRENDING OVER TIME
===============================================
ingredient_name      week_start_date  recipe_count  popularity_rank
-------------------  ---------------  ------------  ---------------
Water for the Rice   2026-02-28       12            44
Water for the Rice   2026-02-21       19            28
Water for the Rice   2026-02-14       5             102
Water for the Rice   2026-02-07       12            40
Water for the Rice   2026-01-31       12            43
Water for the Rice   2026-01-24       7             74
Water for the Sauce  2026-02-28       82            2
Water for the Sauce  2026-02-21       80            2
Water for the Sauce  2026-02-14       85            2
Water for the Sauce  2026-02-07       67            2
Water for the Sauce  2026-01-31       74            2
Water for the Sauce  2026-01-24       72            2
Wild Rocket          2026-02-28       13            41
Wild Rocket          2026-02-21       29            12
Wild Rocket          2026-02-14       13            44
Wild Rocket          2026-02-07       23            17
Wild Rocket          2026-01-31       27            9
Wild Rocket          2026-01-24       23            20
Worcester Sauce      2026-02-28       4             130
Worcester Sauce      2026-02-21       9             63
Worcester Sauce      2026-02-14       4             135
Worcester Sauce      2026-02-07       5             97
Worcester Sauce      2026-01-31       4             122
Worcester Sauce      2026-01-24       2             202
Young Pea Pods       2026-02-28       14            36
Young Pea Pods       2026-02-21       15            33
Young Pea Pods       2026-02-14       6             71
Young Pea Pods       2026-02-07       7             66
Young Pea Pods       2026-01-31       13            40
Young Pea Pods       2026-01-24       14            35
===============================================
ALLERGEN DENSITY BY WEEK
===============================================
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
===============================================
ALLERGEN PREVALENCE SUMMARY
===============================================
allergen_name                    weeks_present  avg_recipe_count  max_recipe_count  avg_pct_of_menu 
-------------------------------  -------------  ----------------  ----------------  ---------------
Cereals containing gluten        6              103.5             111               70.79
Wheat                            6              102.5             109               70.1           
May contain traces of allergens  6              91.8              99                62.88
Milk                             6              91.8              100               62.74
Sulphites                        6              63.3              77                43.24
Soya                             6              56.7              65                38.77
Egg                              6              52.3              65                35.74
Nuts                             6              37.8              46                25.97
Cashew nuts                      6              37.0              46                25.4
Peanut                           6              36.0              44                24.72
Celery                           6              33.0              45                22.49
Sesame                           6              32.7              51                22.41
Mustard                          6              30.8              40                21.08
Pistachio nuts                   6              28.3              38                19.43
Hazelnuts                        6              28.3              38                19.43
Almonds                          6              28.3              38                19.43
Pecan Nuts                       6              27.3              36                18.75
Macadamia Nuts                   6              27.3              36                18.75
Brazil nuts                      6              27.3              36                18.75
Barley                           6              26.5              35                18.11
Rye                              6              21.2              26                14.48
Fish                             6              12.3              15                8.44
Walnuts                          6              11.3              19                7.77
Crustaceans                      6              11.0              15                7.5
Molluscs                         6              4.3               6                 2.94
Oats                             6              3.0               4                 2.06
Spelt (wheat)                    6              2.5               4                 1.72
Khorasan (wheat)                 6              2.5               4                 1.72
Kamut (wheat)                    6              2.5               4                 1.72
Lupin                            2              1.0               1                 0.68
===============================================
MENU STABILITY METRICS
===============================================
week_start_date  locale  overlap_prev_week  new_recipe_rate  churned_recipe_rate  recipes_retained  recipes_added  recipes_removed
---------------  ------  -----------------  ---------------  -------------------  ----------------  -------------  ---------------
2026-02-28       en-GB   0.0                100.68           100.0                0                 
149            148
2026-02-21       en-GB   0.0                99.33            100.0                0                 
148            149
2026-02-14       en-GB   0.0                106.43           100.0                0                 
149            140
2026-02-07       en-GB   0.0                96.55            100.0                0                 
140            145
2026-01-31       en-GB   0.0                99.32            100.0                0                 
145            146
2026-01-24       en-GB   NULL               NULL             NULL                 0                 
146            0
===============================================
MENU STABILITY TRENDS
===============================================
week_start_date  avg_overlap  avg_new_rate  total_added  total_removed
---------------  -----------  ------------  -----------  -------------
2026-02-28       0.0          100.68        149          148
2026-02-21       0.0          99.33         148          149
2026-02-14       0.0          106.43        149          140          
2026-02-07       0.0          96.55         140          145
2026-01-31       0.0          99.32         145          146
2026-01-24       NULL         NULL          146          0
===============================================
RECIPE INTRODUCTION TIMING
===============================================
first_appearance_date  recipes_introduced
---------------------  ------------------
2026-02-03             877
===============================================
CURRENT MENU HEALTH CHECK
===============================================
metric       active_recipes  active_menus  recipes_in_survival_metrics  weeks_of_data
-----------  --------------  ------------  ---------------------------  -------------
Menu Health  877             6             877                          6


