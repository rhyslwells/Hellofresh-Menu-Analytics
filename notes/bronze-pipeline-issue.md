## Summary of Issues Found - Bronze & Silver Pipeline

### 1. **Path/Configuration Issues** ✅ FIXED
- **Problem:** After moving tests to bronze-tests, import paths in test runners were incorrect
- **Cause:** Looking for 1_bronze.py in `tests/scripts/` instead of project root
- **Solution:** Updated path calculation to go up 3 levels instead of 2

---

### 2. **Documentation Issues** ✅ FIXED
- **Problem:** Excessive redundant meta-documentation (7 files about documentation cleanup)
- **Cause:** Multiple "cleanup summary" and "before/after" files explaining the same thing
- **Solution:** Consolidated to QUICK_START.md and INDEX.md, marked others deprecated

---

### 3. **Bronze Layer (1_bronze.py)** - Working ✓
**What works:**
- ✓ Temporal selection (--week, --start-date/--end-date)
- ✓ ISO week calculations (including year boundaries)
- ✓ API connectivity
- ✓ Data stored in SQLite

**Issues found:**
- ⚠️ **API Limitation:** Only returns current/future menus, not historical data
- ⚠️ **Data Opacity:** Data stored as raw JSON in `payload` column - hard to query directly
- ⚠️ **Discovery:** Users don't see what weeks/data they have without parsing JSON

---

### 4. **Silver Layer (2_silver.py)** - Incremental Processing Issue ⚠️
**What works:**
- ✓ Transforms Bronze JSON into normalized tables
- ✓ Creates proper schema (menus, recipes, ingredients, allergens, tags, etc.)
- ✓ Implements SCD Type 2 tracking

**Critical issues:**
- ⚠️ **Partial Processing:** Only processes latest `pull_date` incrementally
  - Example: Had 228 records from 2026-01-26 and 6 from 2026-02-02
  - Silver only processed 2026-02-02, ignored older data
  - Result: 7 menus instead of expected ~13-14

- ⚠️ **No Reprocessing:** No clear way to:
  - Reprocess historical data
  - Do a full refresh
  - Handle data from multiple pull_dates

- ⚠️ **Idempotency Unclear:** Behavior when re-running same date is not documented

- ⚠️ **Silent Data Loss Risk:** If Bronze has multiple pull_dates, only latest is processed

---

### 5. **Data Flow Issue**
```
Bronze (Raw JSON) ✓
    ↓
Silver (Normalized) ⚠️ Only processes latest pull
    ↓
Gold (Analytics) - Can't build on incomplete Silver
```

---

## Recommendations

1. **Investigate Silver Logic:**
   - Why only process latest `pull_date`?
   - Add flag for full reprocessing: `--reprocess-all`
   - Document incremental vs full logic

2. **Bronze/Silver Reconciliation:**
   - Check if 228 records from 2026-01-26 are already in Silver (from previous runs)
   - Verify no data loss occurred

3. **Add Monitoring:**
   ```bash
   # Quick health check
   sqlite3 hfresh/hfresh.db "
   SELECT 'Bronze' as layer, COUNT(*) FROM api_responses
   UNION ALL
   SELECT 'Silver (menus)', COUNT(*) FROM menus;
   "
   ```

4. **Document:**
   - When to run full reprocess
   - How Silver handles multiple pull_dates
   - Expected row counts after each stage