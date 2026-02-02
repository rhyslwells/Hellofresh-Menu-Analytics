# Quick Start: Testing Temporal Selection

Testing the temporal selection feature in `scripts/1_bronze.py` - enables historical data ingestion.

## Run Tests (Pick One)

```bash
# ✅ Recommended - works everywhere
python tests/bronze-tests/run_tests.py

# Alternative with pytest
pytest tests/bronze-tests/test_temporal_selection.py -v
```

## Expected Output

```
Ran 29 tests in 0.024s
OK
```

## Run Specific Tests

```bash
# Test date parsing only
python -m unittest tests.bronze_tests.run_tests.TestParseISODate -v

# Test week boundaries
python -m unittest tests.bronze_tests.run_tests.TestGetWeekBounds -v

# Test temporal argument parsing
python -m unittest tests.bronze_tests.run_tests.TestParseTemporalArguments -v

# Single test method
python -m unittest tests.bronze_tests.run_tests.TestParseISODate.test_valid_date -v
```

## Test Coverage

- **parse_iso_date()** - Date parsing (YYYY-MM-DD format)
- **get_week_bounds()** - Week boundary calculation from any date
- **parse_temporal_arguments()** - Central router for temporal selection
- **Integration tests** - Cross-function consistency

## Documentation

| Need | File |
|------|------|
| Complete guide | [README.md](../README.md) |
| Implementation | [../../../scripts/1_bronze.py](../../../scripts/1_bronze.py) |

## Troubleshooting

**"ModuleNotFoundError: No module named 'scripts'"**
```bash
# Run from project root
cd /path/to/Hellofresh-Menu-Analytics
python tests/bronze-tests/run_tests.py
```

**"pytest: command not found"**
```bash
# Use unittest instead (no install needed)
python tests/run_tests.py

# Or install pytest
pip install pytest
```

## What Gets Tested

✅ Valid date formats (2026-01-15)  
✅ Invalid date formats (slashes, bad dates)  
✅ Week 01 edge cases (year boundaries)  
✅ All weekdays (Mon-Sun)  
✅ Leap year handling  
✅ Date ranges (start > end validation)  
✅ Default behavior (next week)  
✅ Error messages  

## Performance

- Total execution: ~25ms
- Per test: ~0.8ms
- No external dependencies (unittest version)
- No network calls or file I/O
