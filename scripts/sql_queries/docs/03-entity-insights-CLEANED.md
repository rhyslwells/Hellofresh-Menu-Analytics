# Entity Relationships & Data Quality Analysis

## Overview
Entity relationships are complete and consistent. All recipes have ingredients, allergens, and tags; no orphaned records. Strong cross-entity linkage supports analytics integrity.

---

## Key Findings

### Recipe Composition (Sample: 20 active recipes)
- **Ingredient counts:** 10–17 per recipe (median ~13)
- **Allergen exposure:** 1–14 per recipe (median ~5)
- **Tag coverage:** 2–7 per recipe (consistent categorization)
- **Menu distribution:** All sampled recipes appear in exactly 1 menu

✅ **Assessment:** Relationships well-formed and diverse.

---

### Top Ingredients

**Most common ingredients (top 10):**
| Ingredient | Recipes | Notes |
|-----------|---------|-------|
| Garlic Clove | 558 | Staple across cuisines |
| Water for the Sauce | 460 | Common preparation component |
| Honey | 259 | Sweetener/glaze |
| Potatoes | 249 | Carb base |
| Butter | 220 | Cooking fat |
| Grated Hard Italian Style Cheese | 198 | Dairy staple |
| Baby Spinach | 198 | Vegetable staple |
| Sugar | 195 | Sweetener |
| Chicken Stock Paste | 183 | Base flavoring |
| Creme Fraiche | 173 | Dairy/sauce base |

**⚠️ Issue:** All ingredients lack `family` classification (API limitation)  
**Implication:** Cannot segment by ingredient category (e.g., proteins, vegetables, pantry)

---

### Allergen Coverage

**High prevalence (~70% of recipes):**
- Cereals containing gluten: **70.81%** (621 recipes)
- Wheat: **70.13%** (615 recipes)
- Milk: **62.83%** (551 recipes)
- May contain traces of allergens: **62.83%** (551 recipes)

**Moderate coverage (25–45%):**
- Sulphites: 43.33% (380 recipes)
- Soya: 38.77% (340 recipes)
- Egg: 35.80% (314 recipes)
- Nuts: 25.88% (227 recipes)
- Cashew nuts: 25.31% (222 recipes)
- Peanut: 24.63% (216 recipes)

✅ **Assessment:** Allergen distribution is plausible for HelloFresh UK portfolio (bread, dairy-heavy cuisine). High gluten/wheat/milk prevalence expected given British meal preferences.

---

### Recipe Difficulty Distribution

| Difficulty | Count | % | Avg Prep Time |
|-----------|-------|---|---------------|
| Easy (0–1) | 584 | 66.6% | 29.1 mins |
| Medium (1–2) | 289 | 32.9% | 35.2 mins |
| Hard (2–3) | 4 | 0.5% | 41.3 mins |

✅ **Assessment:** Skew toward easy recipes expected (consumer preference). Very few hard recipes (4/877 = 0.5%) confirms HelloFresh targeting casual cooks. Prep time increases with difficulty — sensible correlation.

---

### Recipe-Ingredient Many-to-Many Links

**Sample: Roasted Butternut Squash and Ditali Halloumi Pasta Salad (13 ingredients)**

| Ingredient | Position | Quantity | Unit |
|-----------|----------|----------|------|
| Greek Style Natural Yoghurt | 0 | NULL | NULL |
| Butternut Squash | 1 | NULL | NULL |
| Halloumi | 2 | NULL | NULL |
| ... | ... | NULL | NULL |

✅ **Position tracking:** Ingredients ordered sequentially (0–indexed) — supports recipe step instructions  
❌ **Quantity/Unit:** All NULL — **API does not provide measured quantities**  
**Implication:** Cannot compute nutrition per serving or cost-per-serving without external enrichment

---

### Menu Composition

| Week | Year-Week | Start Date | Recipe Count | Ingestion Date |
|-----|-----------|------------|--------------|----------------|
| 1 | 202610 | 2026-02-28 | 149 | 2026-02-03 |
| 2 | 202609 | 2026-02-21 | 148 | 2026-02-03 |
| 3 | 202608 | 2026-02-14 | 149 | 2026-02-03 |
| 4 | 202607 | 2026-02-07 | 140 | 2026-02-03 |
| 5 | 202606 | 2026-01-31 | 145 | 2026-02-03 |
| 6 | 202605 | 2026-01-24 | 146 | 2026-02-03 |

✅ **Consistency:** 140–149 recipes per menu (stable offering)  
⚠️ **Single snapshot:** All menus ingested on 2026-02-03 — represents point-in-time state, not evolution

---

### Cuisine Breakdown

**Result:** Empty — **HelloFresh API does not classify recipes by cuisine**

Despite recipe names including ethnic descriptors ("Chinese Style," "Indian," "Italian"), no structured cuisine field exists in the source payload.

---

### Recipe Ingredient Usage Extremes

| Recipe | Ingredient Count |
|--------|------------------|
| Ultimate Chicken Tikka and Rice | 20 |
| Ultimate Spanish Style Chicken and Chorizo Paella | 19 |
| Breaded Herby Chicken and Garlic-Lemon Butter Sauce | 19 |
| Cheesy Ratatouille and Bacon Galette | 19 |
| Ultimate Matar Saag Paneer and Garlic Naan | 19 |
| **Median** | **~13** |

Range: 10–20 ingredients, typical 13–15 for complex dishes.

---

## Data Gaps & Mitigation

| Gap | Status | Source | Recommendation |
|-----|--------|--------|-----------------|
| Ingredient `family` | ❌ Missing | API doesn't provide | **Option 1:** Accept NULLs; filter analytics to unclassified data. **Option 2:** Enrich via external taxonomy (e.g., FDC database). **Option 3:** Infer from ingredient names (regex/ML). |
| Recipe `cuisine` | ❌ Missing | API doesn't provide | **Option 1:** Accept NULLs; use recipe names/tags for inference. **Option 2:** Train classifier on recipe names + ingredients. **Option 3:** Manually tag subset + propagate. |
| Ingredient `quantity/unit` | ❌ Missing | API doesn't provide | **Accept NULLs** — track ingredient *presence* only, not amounts. Adequate for popularity/allergen analysis. |
| Allergen `type` field | NULL (stored) | API may provide; not extracted | Low priority — allergen name already descriptive. |

---

## Data Integrity Summary

✅ **All relational constraints met** (no orphaned FK references)  
✅ **10,953 recipe-ingredient links** consistently populated  
✅ **All recipes have ≥1 allergen** (foundational allergen data)  
✅ **All recipes have ≥1 tag** (functional categorization)  
✅ **SCD Type 2 tracking functional** (first_seen, last_seen, is_active columns populated)  
✅ **No duplicate recipe-ingredient pairs** within a week  
✅ **Ingredient position tracking** enables ordering/recipe flow analysis

---

## Recommendations & Next Steps

### ✅ Proceed Immediately
1. **Run analytics** — all core metrics are computable:
   - Difficulty distribution per week
   - Average prep time by recipe/difficulty
   - Allergen prevalence trends
   - Ingredient popularity ranking
   - Menu composition stability
   
2. **Weekly monitoring** — set up SCD deactivation tracking:
   - Monitor `is_active` flags for recipe rotation
   - Track ingredient shelf-life (when they disappear)

### 🔄 Optional Enrichment (Future)
1. **Ingredient family** — create manual lookup table or integrate external taxonomy
2. **Recipe cuisine** — build classifier from recipe names + ingredients
3. **Ingredient quantities** — scrape recipe PDF URLs or request from API team

### 📊 Analytics Ready-to-Build
- Weekly menu evolution (recipe churn, retention)
- Allergen density per week (% of menu with gluten, dairy, etc.)
- Recipe difficulty trends
- Ingredient popularity cycles
- Difficulty-prep-time correlation

---

## Conclusion

**Data is production-ready.** All core relationships and metrics are intact. Missing enrichment fields (family, cuisine, quantities) are API limitations, not pipeline defects. Current schema supports robust analytics; enhancements are optional and non-blocking.
