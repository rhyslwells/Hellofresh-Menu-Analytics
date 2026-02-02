# Bronze Tests Documentation

Quick reference for testing `scripts/1_bronze.py` temporal selection feature.

## Quick Start

```bash
# Run all tests (from project root)
python tests/bronze-tests/run_tests.py

# Or with pytest
pytest tests/bronze-tests/test_temporal_selection.py -v
```

## Run Specific Tests

```bash
# Test date parsing
python -m unittest tests.bronze_tests.run_tests.TestParseISODate -v

# Test week boundaries  
python -m unittest tests.bronze_tests.run_tests.TestGetWeekBounds -v

# Test temporal argument parsing
python -m unittest tests.bronze_tests.run_tests.TestParseTemporalArguments -v
```

## Test Coverage

| Function | Tests | Coverage |
|----------|-------|----------|
| `parse_iso_date()` | 5 | 100% |
| `get_week_bounds()` | 6 | 100% |
| `parse_temporal_arguments()` | 16 | 95%+ |
| Integration | 2 | 100% |
| **Total** | **29** | **97%+** |

## Files

- `run_tests.py` - Main test runner (unittest, no external deps)
- `test_temporal_selection.py` - Pytest alternative
- `README.md` - Comprehensive testing guide

## Troubleshooting

**"ModuleNotFoundError"** - Run from project root:
```bash
cd /path/to/Hellofresh-Menu-Analytics
python tests/bronze-tests/run_tests.py
```

**"pytest not found"** - Use unittest instead:
```bash
python tests/bronze-tests/run_tests.py
```

## Expected Output

```
Ran 29 tests in 0.024s
OK
```
