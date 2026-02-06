Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/4_weekly_report.py#1-1), lines 1 to 50

Searched for regex `def main|argparse|sys.argv|click` (`**/scripts/4_weekly_report.py`), 1 result

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/4_weekly_report.py#950-950), lines 950 to 1002

Searched for regex `def get_latest_week` (`**/scripts/4_weekly_report.py`), 1 result

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/4_weekly_report.py#77-77), lines 77 to 100

Read [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/4_weekly_report.py#30-30), lines 30 to 45

Replacing 21 lines with 38 lines in [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/scripts/4_weekly_report.py)


```

```
Now let me update the workflow to accept a report date input: 

Replacing 14 lines with 18 lines in [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/.github/workflows/pipeline.yml)


```

```


Replacing 2 lines with 7 lines in [](file:///c%3A/Users/RhysL/Desktop/Hellofresh-Menu-Analytics/.github/workflows/pipeline.yml)


```

```
Done! I've updated both files to support generating reports for specific dates:

**Changes to 4_weekly_report.py:**
- Added `argparse` import
- Added `--date` command-line parameter that accepts dates in YYYY-MM-DD format
- If `--date` is provided, it uses that date; otherwise it falls back to the latest week in the database

**Changes to pipeline.yml:**
- Added `report-date` input parameter
- Updated the report generation step to pass the date when provided

**Usage:**
Now you can trigger the workflow with:
- **Latest week report**: Set `report-only: true` (no date needed)
- **Specific date report**: Set `report-only: true` and `report-date: 2026-01-31` (or any date in YYYY-MM-DD format)

Examples:
- `report-date: 2026-01-31`
- `report-date: 2026-01-24`
- `report-date: 2026-02-07`

Made changes.