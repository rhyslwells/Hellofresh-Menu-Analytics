# Quick Start: Testing Bronze Layer

Testing the Bronze layer functionality in `scripts/1_bronze.py` - includes temporal selection for historical data ingestion.

## Run Tests (Pick One)

```bash
# ✅ Recommended - works everywhere
python tests/bronze-tests/test_bronze.py

# Alternative with pytest
pytest tests/bronze-tests/test_bronze.py -v
```

## Expected Output

```
Ran 50+ tests in 0.024s
OK
```

## Run Specific Tests

```bash
# Test date parsing only
python -m unittest tests.bronze-tests.test_bronze.TestParseISODate -v

# Test week boundaries
python -m unittest tests.bronze-tests.test_bronze.TestGetWeekBounds -v

# Test temporal argument parsing
python -m unittest tests.bronze-tests.test_bronze.TestParseTemporalArguments -v

# Single test method
python -m unittest tests.bronze-tests.test_bronze.TestParseISODate.test_valid_date -v
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
python tests/bronze-tests/test_bronze.py
```

**"pytest: command not found"**
```bash
# Use unittest instead (no install needed)
python tests/bronze-tests/test_bronze.py

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
- Per test: ~0.5ms
- No external dependencies (unittest version)
- No network calls or file I/O
