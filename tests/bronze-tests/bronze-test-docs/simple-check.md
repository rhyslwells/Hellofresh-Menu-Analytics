## Simplest Way to Run Bronze.py for a Previous Week

### Option 1: Run for a Specific Week (ISO format)
```bash
python scripts/1_bronze.py --week 2026-W01
```



### Option 2: Run for a Date Range
```bash
python scripts/1_bronze.py --start-date 2026-01-26 --end-date 2026-04-01
```

### Option 3: Run for Any Date Within a Week
```bash
python scripts/1_bronze.py --week 2026-01-03
```
(Automatically gets Monday-Sunday bounds)

## To View Results in Database

After running, query the database:

```bash
# Open SQLite CLI
sqlite3 hfresh/hfresh.db

# Check records in bronze layer
SELECT COUNT(*) FROM api_responses WHERE pull_date = '2026-01-26';

# See recent pulls
SELECT DISTINCT pull_date, COUNT(*) as record_count 
FROM api_responses 
GROUP BY pull_date 
ORDER BY pull_date DESC 
LIMIT 5;

# View specific week's data
SELECT pull_date, endpoint, locale, page_number 
FROM api_responses 
WHERE pull_date = '2026-01-26' 
LIMIT 10;
```

## Quickest One-Liner
```bash
python scripts/1_bronze.py --week 2026-W05 && sqlite3 hfresh/hfresh.db "SELECT COUNT(*) FROM api_responses WHERE pull_date = '2026-01-26';"
```

**Key Points:**
- Must set `HELLOFRESH_API_KEY` environment variable first
- Run from project root directory
- Data goes into hfresh.db 
- Bronze table: `api_responses`
- Each row = one API response (raw JSON in `payload` column)