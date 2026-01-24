# HelloFresh Data Platform Blueprint
## Databricks Implementation

---

## 1. Project Overview

**Objective:** Build a production-ready data platform that ingests HelloFresh API data weekly, maintains historical snapshots, and generates automated analytical reports demonstrating data evolution over time.

**Key Principle:** Treat the API as a slowly evolving upstream system. Your Databricks lakehouse is the system of record.

---

## 2. Architecture

### 2.1 Three-Layer Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Databricks Delta Tables)                      │
│ • Raw API responses                                          │
│ • Immutable snapshots                                        │
│ • Full audit trail                                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Databricks Delta Tables)                      │
│ • Normalized relational schema                               │
│ • Slowly changing dimensions                                 │
│ • Active/inactive tracking                                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Databricks Delta Tables)                        │
│ • Analytical aggregations                                    │
│ • Weekly metrics                                             │
│ • Trend calculations                                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ REPORTS (Git Repository: hfresh/output/reports/)            │
│ • Weekly markdown reports with embedded charts               │
│ • README.md (latest report via GitHub Action)               │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

1. **Weekly Ingestion Job** (Databricks Job)
   - Fetches all API endpoints
   - Writes to Bronze Delta tables
   
2. **Silver Transformation** (Databricks Notebook)
   - Normalizes entities
   - Tracks entity lifecycle
   - Updates slowly changing dimensions

3. **Gold Analytics** (Databricks Notebook)
   - Computes weekly metrics
   - Generates trend analysis
   - Creates comparative analytics

4. **Report Generation** (Databricks Notebook → Git)
   - Produces charts (overwritten weekly)
   - Generates markdown report
   - Commits to Git repository

---

## 3. Databricks Implementation Details

### 3.1 Bronze Layer Schema

**Database:** `hfresh_bronze`

**Tables:**

```sql
-- Raw API responses
CREATE TABLE hfresh_bronze.api_responses (
  pull_date DATE NOT NULL,
  endpoint STRING NOT NULL,
  locale STRING,
  page_number INT,
  payload STRING,  -- JSON string
  ingestion_timestamp TIMESTAMP,
  CONSTRAINT pk_bronze PRIMARY KEY (pull_date, endpoint, locale, page_number)
) USING DELTA
PARTITIONED BY (pull_date, endpoint);
```

**Write Pattern:** Append-only, never update

### 3.2 Silver Layer Schema

**Database:** `hfresh_silver`

**Core Tables:**

```sql
-- Recipes
CREATE TABLE hfresh_silver.recipes (
  recipe_id STRING PRIMARY KEY,
  name STRING,
  slug STRING,
  headline STRING,
  description STRING,
  difficulty INT,
  prep_time_minutes INT,
  total_time_minutes INT,
  servings INT,
  image_url STRING,
  -- SCD tracking
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN,
  -- Metadata
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA;

-- Ingredients
CREATE TABLE hfresh_silver.ingredients (
  ingredient_id STRING PRIMARY KEY,
  name STRING,
  slug STRING,
  type STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN
) USING DELTA;

-- Recipe-Ingredient Bridge
CREATE TABLE hfresh_silver.recipe_ingredients (
  recipe_id STRING,
  ingredient_id STRING,
  quantity DECIMAL(10,2),
  unit STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN,
  CONSTRAINT pk_recipe_ing PRIMARY KEY (recipe_id, ingredient_id)
) USING DELTA;

-- Allergens
CREATE TABLE hfresh_silver.allergens (
  allergen_id STRING PRIMARY KEY,
  name STRING,
  slug STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN
) USING DELTA;

-- Recipe-Allergen Bridge
CREATE TABLE hfresh_silver.recipe_allergens (
  recipe_id STRING,
  allergen_id STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN,
  CONSTRAINT pk_recipe_allergen PRIMARY KEY (recipe_id, allergen_id)
) USING DELTA;

-- Menus
CREATE TABLE hfresh_silver.menus (
  menu_id STRING PRIMARY KEY,
  week_start_date DATE,
  week_end_date DATE,
  locale STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN
) USING DELTA;

-- Menu-Recipe Bridge
CREATE TABLE hfresh_silver.menu_recipes (
  menu_id STRING,
  recipe_id STRING,
  position INT,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN,
  CONSTRAINT pk_menu_recipe PRIMARY KEY (menu_id, recipe_id)
) USING DELTA;

-- Tags
CREATE TABLE hfresh_silver.tags (
  tag_id STRING PRIMARY KEY,
  name STRING,
  slug STRING,
  type STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN
) USING DELTA;

-- Labels
CREATE TABLE hfresh_silver.labels (
  label_id STRING PRIMARY KEY,
  name STRING,
  slug STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  is_active BOOLEAN
) USING DELTA;
```

### 3.3 Gold Layer Schema

**Database:** `hfresh_gold`

```sql
-- Weekly menu composition metrics
CREATE TABLE hfresh_gold.weekly_menu_metrics (
  week_start_date DATE,
  locale STRING,
  total_recipes INT,
  unique_recipes INT,
  new_recipes INT,
  returning_recipes INT,
  avg_difficulty DECIMAL(3,2),
  avg_prep_time_minutes DECIMAL(5,2),
  CONSTRAINT pk_weekly_metrics PRIMARY KEY (week_start_date, locale)
) USING DELTA;

-- Recipe lifecycle metrics
CREATE TABLE hfresh_gold.recipe_survival_metrics (
  recipe_id STRING,
  recipe_name STRING,
  first_appearance_date DATE,
  last_appearance_date DATE,
  total_weeks_active INT,
  consecutive_weeks_active INT,
  weeks_since_last_seen INT,
  is_currently_active BOOLEAN,
  CONSTRAINT pk_recipe_survival PRIMARY KEY (recipe_id)
) USING DELTA;

-- Ingredient trend analysis
CREATE TABLE hfresh_gold.ingredient_trends (
  ingredient_id STRING,
  ingredient_name STRING,
  week_start_date DATE,
  recipe_count INT,
  week_over_week_change INT,
  popularity_rank INT,
  CONSTRAINT pk_ingredient_trends PRIMARY KEY (ingredient_id, week_start_date)
) USING DELTA;

-- Menu stability metrics
CREATE TABLE hfresh_gold.menu_stability_metrics (
  week_start_date DATE,
  locale STRING,
  overlap_with_prev_week DECIMAL(5,2),  -- percentage
  new_recipe_rate DECIMAL(5,2),
  churned_recipe_rate DECIMAL(5,2),
  CONSTRAINT pk_menu_stability PRIMARY KEY (week_start_date, locale)
) USING DELTA;

-- Allergen density analysis
CREATE TABLE hfresh_gold.allergen_density (
  week_start_date DATE,
  allergen_id STRING,
  allergen_name STRING,
  recipe_count INT,
  percentage_of_menu DECIMAL(5,2),
  CONSTRAINT pk_allergen_density PRIMARY KEY (week_start_date, allergen_id)
) USING DELTA;
```

---

## 4. Implementation Components

### 4.1 Databricks Notebooks

#### Notebook 1: `01_bronze_ingestion`

**Purpose:** Fetch API data and write to Bronze layer

**Key Operations:**
- Authenticate to HelloFresh API
- Paginate through all endpoints (recipes, menus, ingredients, allergens, tags, labels)
- Respect 60 req/min rate limit
- Write raw JSON to `hfresh_bronze.api_responses`
- Add metadata: pull_date, endpoint, locale, page_number

**Parameters:**
- `pull_date` (default: current date)
- `locale` (default: "en-GB")

#### Notebook 2: `02_silver_normalization`

**Purpose:** Transform Bronze → Silver with SCD tracking

**Key Operations:**
- Parse JSON from Bronze
- Extract entities and relationships
- Implement SCD Type 2 logic:
  - Set `first_seen_date` for new entities
  - Update `last_seen_date` for existing entities
  - Mark missing entities as `is_active = FALSE`
- Use MERGE statements for idempotent updates

#### Notebook 3: `03_gold_analytics`

**Purpose:** Generate analytical tables

**Key Operations:**
- Compute weekly aggregations
- Calculate trends and changes
- Generate survival analysis
- Produce stability metrics

#### Notebook 4: `04_report_generation`

**Purpose:** Create markdown reports with charts

**Key Operations:**
- Query Gold layer for latest metrics
- Generate charts using matplotlib/plotly:
  - Menu overlap trends
  - Recipe survival distribution
  - Ingredient popularity over time
  - Allergen density heatmap
- Save charts to `/dbfs/FileStore/hfresh/charts/` (overwrite)
- Generate markdown report with embedded chart references
- Write to Git repository: `hfresh/output/reports/weekly_report_{date}.md`

### 4.2 Databricks Job Configuration

**Job Name:** `HelloFresh_Weekly_Pipeline`

**Schedule:** Weekly (e.g., every Monday at 2 AM UTC)

**Tasks:**
1. **Task 1:** Bronze Ingestion
   - Notebook: `01_bronze_ingestion`
   - Cluster: Job compute (single node)
   
2. **Task 2:** Silver Normalization
   - Notebook: `02_silver_normalization`
   - Depends on: Task 1
   
3. **Task 3:** Gold Analytics
   - Notebook: `03_gold_analytics`
   - Depends on: Task 2
   
4. **Task 4:** Report Generation
   - Notebook: `04_report_generation`
   - Depends on: Task 3

---

## 5. Git Repository Structure

```
hfresh/
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingestion.py
│       ├── 02_silver_normalization.py
│       ├── 03_gold_analytics.py
│       └── 04_report_generation.py
├── output/
│   ├── charts/                    # Overwritten weekly
│   │   ├── menu_overlap.png
│   │   ├── recipe_survival.png
│   │   ├── ingredient_trends.png
│   │   └── allergen_density.png
│   └── reports/
│       ├── weekly_report_2026-01-23.md
│       ├── weekly_report_2026-01-30.md
│       └── ...
├── .github/
│   └── workflows/
│       └── update_readme.yml      # GitHub Action
└── README.md                       # Symlinked to latest report
```

---

## 6. GitHub Action for README Update

**File:** `.github/workflows/update_readme.yml`

```yaml
name: Update README with Latest Report

on:
  push:
    paths:
      - 'output/reports/weekly_report_*.md'

jobs:
  update-readme:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
        
      - name: Find latest report
        id: latest
        run: |
          LATEST=$(ls -t output/reports/weekly_report_*.md | head -n 1)
          echo "latest_report=$LATEST" >> $GITHUB_OUTPUT
          
      - name: Copy to README
        run: |
          cp ${{ steps.latest.outputs.latest_report }} README.md
          
      - name: Commit changes
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "github-actions[bot]"
          git add README.md
          git commit -m "Update README with latest weekly report" || echo "No changes to commit"
          git push
```

---

## 7. Report Template

**File:** `weekly_report_{date}.md`

```markdown
# HelloFresh Data Analysis Report
## Week of {week_start_date}

**Generated:** {timestamp}  
**Data Source:** HelloFresh API  
**Analysis Period:** {earliest_data} to {latest_data}

---

## Executive Summary

- **Total Recipes This Week:** {total_recipes}
- **New Recipes Introduced:** {new_recipes}
- **Recipes Removed:** {churned_recipes}
- **Menu Stability:** {overlap_percentage}% overlap with previous week

---

## 1. Menu Evolution

![Menu Overlap Trends](../charts/menu_overlap.png)

### Key Findings
- Week-over-week recipe overlap: {current_overlap}%
- Historical average overlap: {avg_overlap}%
- Trend: {increasing/decreasing/stable}

---

## 2. Recipe Lifecycle Analysis

![Recipe Survival Distribution](../charts/recipe_survival.png)

### Longest-Running Recipes
| Recipe | Weeks Active | Status |
|--------|--------------|--------|
| {recipe_1} | {weeks_1} | {active/inactive} |
| {recipe_2} | {weeks_2} | {active/inactive} |

---

## 3. Ingredient Trends

![Ingredient Popularity Over Time](../charts/ingredient_trends.png)

### Trending Ingredients
- **Rising:** {ingredient_1} (+{change}%)
- **Falling:** {ingredient_2} (-{change}%)

---

## 4. Allergen Analysis

![Allergen Density Heatmap](../charts/allergen_density.png)

### Allergen Coverage
- Most common allergen: {allergen_1} ({percentage}% of recipes)
- Allergen-free recipes: {count} ({percentage}%)

---

## Data Quality Notes

- **Total API calls:** {api_calls}
- **Successful responses:** {success_rate}%
- **Bronze layer size:** {bronze_size} MB
- **Silver layer size:** {silver_size} MB

---

*This report demonstrates historical data analysis capabilities using the HelloFresh API medallion architecture.*
```

---

## 8. Key Design Decisions

### 8.1 Why Databricks Delta Tables Instead of Local Files?

| Aspect | Local Files | Databricks Delta |
|--------|-------------|------------------|
| **Scalability** | Limited by disk | Petabyte-scale |
| **ACID transactions** | None | Full support |
| **Time travel** | Manual versioning | Built-in |
| **Schema evolution** | Manual | Automatic |
| **Query performance** | Slow for large datasets | Optimized with Z-ordering |
| **Concurrent access** | File locks | Multi-reader/writer |

### 8.2 Why Weekly Cadence?

- Matches HelloFresh menu publication cycle
- Allows meaningful trend analysis
- Respects API rate limits (60/min)
- Provides sufficient data for SCD tracking

### 8.3 Why Overwrite Charts but Keep Reports?

- **Charts:** Visual representations can be regenerated from data
- **Reports:** Historical narrative documents showing analysis evolution
- **Storage:** Keeps repository size manageable

---

## 9. Success Metrics

### Phase 1: Foundation (Weeks 1-4)
- ✅ Bronze layer ingesting all endpoints
- ✅ Silver layer with complete SCD tracking
- ✅ Gold layer with 5+ analytical tables
- ✅ First automated report generated

### Phase 2: Insights (Weeks 5-12)
- ✅ Recipe survival analysis showing meaningful patterns
- ✅ Menu stability metrics demonstrating value
- ✅ Ingredient trends validated against business knowledge
- ✅ Reports demonstrating historical vs. recent comparisons

### Phase 3: Productionization (Weeks 13+)
- ✅ Zero-touch weekly execution
- ✅ Automated alerts for data quality issues
- ✅ Dashboard consumption of Gold tables
- ✅ Stakeholder engagement with reports

---

## 10. Next Steps

1. **Set up Databricks workspace**
   - Create databases (bronze, silver, gold)
   - Configure cluster compute
   
2. **Implement Bronze ingestion**
   - Configure API credentials in Databricks Secrets
   - Build pagination logic
   - Test with single endpoint
   
3. **Build Silver transformations**
   - Start with recipes table
   - Implement SCD logic
   - Add remaining entities
   
4. **Develop Gold analytics**
   - Begin with simple aggregations
   - Add trend calculations
   
5. **Create report generation**
   - Design chart templates
   - Build markdown generation
   - Set up Git integration
   
6. **Schedule and monitor**
   - Configure Databricks Job
   - Set up alerts
   - Document runbook

---

## 11. Reference Documentation

- **HelloFresh API:** https://console.hfresh.info/docs
- **Rate Limit:** 60 requests/minute
- **Authentication:** Bearer token (store in Databricks Secrets)
- **Pagination:** 10-200 results per page (use 50)

---

**Document Version:** 1.0  
**Last Updated:** 2026-01-24  
**Owner:** Data Engineering Team