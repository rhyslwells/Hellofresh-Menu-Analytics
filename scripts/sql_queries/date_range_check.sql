-- Check date range of data in hfresh.db
-- Shows earliest and latest dates across multiple sources:
-- - api_responses: Raw API pulls
-- - menus: Processed menu data (start_date)
-- - weekly_menu_metrics: Aggregated weekly metrics (week_start_date)
-- sqlite3 C:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics\hfresh\hfresh.db < scripts\sql_queries\date_range_check.sql

SELECT 
  'api_responses' as source,
  MIN(pull_date) as earliest_date,
  MAX(pull_date) as latest_date,
  COUNT(*) as total_records
FROM api_responses

UNION ALL

SELECT 
  'menus' as source,
  MIN(start_date) as earliest_date,
  MAX(start_date) as latest_date,
  COUNT(*) as total_records
FROM menus

UNION ALL

SELECT 
  'weekly_menu_metrics' as source,
  MIN(week_start_date) as earliest_date,
  MAX(week_start_date) as latest_date,
  COUNT(*) as total_records
FROM weekly_menu_metrics;

-- Optional: See breakdown by pull_date
-- SELECT pull_date, COUNT(*) as record_count 
-- FROM api_responses 
-- GROUP BY pull_date 
-- ORDER BY pull_date DESC;
