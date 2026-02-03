# Bronze Tests Documentation

Quick reference for testing `scripts/1_bronze.py` temporal selection feature.

## Quick Start

```bash
# Run all tests (from project root)
python tests/bronze-tests/test_bronze.py

# Or with pytest
pytest tests/bronze-tests/test_bronze.py -v
```

## Run Specific Tests

```bash
# Test date parsing
python -m unittest tests.bronze-tests.test_bronze.TestParseISODate -v

# Test week boundaries  
python -m unittest tests.bronze-tests.test_bronze.TestGetWeekBounds -v

# Test temporal argument parsing
python -m unittest tests.bronze-tests.test_bronze.TestParseTemporalArguments -v
```

## Test Coverage

| Function | Tests | Coverage |
|----------|-------|----------|
| `parse_iso_date()` | 5 | 100% |
| `get_week_bounds()` | 6 | 100% |
| `parse_temporal_arguments()` | 20+ | 95%+ |
| Integration | 2+ | 100% |
| **Total** | **50+** | **97%+** |

## Files

- `test_bronze.py` - Main unified test suite (pytest + unittest)
- `README.md` - Comprehensive testing guide

## Troubleshooting

**"ModuleNotFoundError"** - Run from project root:
```bash
cd /path/to/Hellofresh-Menu-Analytics
python tests/bronze-tests/test_bronze.py
```

**"pytest not found"** - Use unittest instead:
```bash
python tests/bronze-tests/test_bronze.py
```

## Expected Output

```
Ran 50+ tests in 0.024s
OK
```
