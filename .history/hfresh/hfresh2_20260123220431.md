
## 1. Core principle

Treat the API as a **slowly evolving upstream system** and your database as the **system of record**.

You will:

1. Pull data on a fixed cadence (weekly).
2. Store immutable raw snapshots.
3. Derive stable, analytics-ready tables.
4. Build downstream projects that *depend on historical change*, not just current state.

This gives you time, versioning, and reproducibility.

---

## 2. Data ingestion design

### 2.1 Pull strategy (weekly)

**Cadence:** once per week
**Scope per pull:**

* Recipes
* Menus
* Reference data (ingredients, allergens, tags, labels)

Even if reference data “rarely changes”, pull it anyway to detect drift.

### 2.2 API extraction pattern

For each endpoint:

* Paginate exhaustively
* Respect rate limits
* Persist raw responses verbatim

Key metadata to attach per pull:

* $pull_date$
* $endpoint$
* $page$
* $locale$

---

## 3. Database layering

Use a **three-layer model**. This maps cleanly to Obsidian notes and blog narratives.

---

### 3.1 Bronze layer — raw snapshots

**Purpose:** reproducibility and audit

**Schema (example):**

```
bronze_api_responses
- pull_date
- endpoint
- locale
- payload_json
```

Properties:

* Append-only
* Never updated
* One row per API page

This allows:

* Backfills
* Reprocessing with new logic
* Debugging upstream changes

---

### 3.2 Silver layer — normalized entities

**Purpose:** stable relational representation

Example tables:

* `recipes`
* `ingredients`
* `recipe_ingredient_bridge`
* `allergens`
* `recipe_allergen_bridge`
* `menus`
* `menu_recipe_bridge`

Key design decision:

* Primary keys include **source ID**
* Add:

  * $first_seen_date$
  * $last_seen_date$
  * $is_active$

This enables **slowly changing dimensions** without heavy machinery.

---

### 3.3 Gold layer — analytical facts

**Purpose:** projects and consumption

Examples:

* Weekly menu composition
* Recipe reuse metrics
* Ingredient overlap scores
* Allergen density per menu

These tables are:

* Recomputed each pull
* Derived only from silver

---

## 4. Weekly update mechanics

Each weekly run follows the same deterministic steps:

1. Fetch all endpoints
2. Append to bronze
3. Rebuild silver:

   * Detect new entities
   * Update $last_seen_date$
   * Mark missing entities inactive
4. Recompute gold

No in-place mutation of bronze. Minimal mutation of silver.

---

## 5. First downstream project (recommended)

Build a project that **only works because the data evolves weekly**.

### Project: Recipe and menu drift analysis

**Questions:**

* How stable are menus week-to-week?
* Which recipes persist longest?
* Which ingredients are increasingly common?

**Core metrics:**

* $recipe_survival_weeks$
* $menu_overlap_ratio$
* $ingredient_trend_slope$

This forces you to:

* Use time explicitly
* Validate historical correctness
* Trust your ingestion logic
---

## 7. Technology-agnostic but realistic stack

You can implement this with:

* Python for ingestion
* SQL for transformation
* Any analytical database

The important part is **design discipline**, not tooling.

---


