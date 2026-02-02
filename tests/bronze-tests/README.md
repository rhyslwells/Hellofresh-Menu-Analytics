# Temporal Selection Tests

This directory contains comprehensive tests for the temporal selection functionality added to the Bronze layer ingestion script.

## Test Files

### `test_temporal_selection.py`
Pytest-based test suite with comprehensive coverage of all temporal functions.

**Run with:**
```bash
pytest tests/bronze-tests/test_temporal_selection.py -v
```

### `run_tests.py`
Standalone unittest-based test runner that doesn't require pytest.

**Run with:**
```bash
python tests/bronze-tests/run_tests.py
```

## Test Coverage

### 1. `parse_iso_date()` Tests
- Valid ISO date parsing (YYYY-MM-DD)
- Invalid date formats (slashes, no separators)
- Invalid dates (month 13, day 30 in Feb, etc.)
- Leap year dates

**Test Class:** `TestParseISODate`
- 5 test methods
- Edge cases covered

### 2. `get_week_bounds()` Tests
- Monday returns itself
- Friday returns week bounds
- Sunday returns week bounds
- Returns start of day (00:00:00)
- Week span is exactly 7 days
- All weekdays of week

**Test Class:** `TestGetWeekBounds`
- 6 test methods
- Consistent across all days

### 3. `parse_temporal_arguments()` Tests

#### ISO Week Format
- Valid ISO week (2026-W05)
- Week 01 handling
- Week 52 handling
- Invalid week formats
- Invalid week numbers

#### Date in Week
- Monday dates
- Friday dates
- Invalid date formats

#### Explicit Date Range
- Valid ranges
- Single day ranges
- Multi-month ranges
- Start date > end date (error)
- Invalid date formats

#### Default Behavior
- No arguments returns next week
- Returns proper datetime objects

**Test Class:** `TestParseTemporalArguments`
- 16+ test methods
- Comprehensive coverage

### 4. Integration Tests

**Test Class:** `TestTemporalIntegration`
- All methods return proper datetime tuples
- Different input methods yield consistent results
- Week boundaries consistent across calculation methods

## Running Tests

### Option 1: With pytest (recommended if installed)
```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/bronze-tests/test_temporal_selection.py -v

# Run specific test class
pytest tests/bronze-tests/test_temporal_selection.py::TestParseISODate -v

# Run with coverage
pytest tests/bronze-tests/test_temporal_selection.py --cov=scripts.1_bronze -v
```

### Option 2: With unittest (no external requirements)
```bash
# Run all tests
python tests/bronze-tests/run_tests.py

# Run specific test class (not as easy, but possible)
python -m unittest tests.bronze_tests.run_tests.TestParseISODate -v
```

## Test Results Expected

All tests should pass with the current implementation. Example output:

```
test_date_in_week_friday (run_tests.TestParseTemporalArguments) ... ok
test_date_in_week_invalid_format (run_tests.TestParseTemporalArguments) ... ok
test_date_in_week_monday (run_tests.TestParseTemporalArguments) ... ok
test_default_next_week (run_tests.TestParseTemporalArguments) ... ok
test_explicit_date_range_multi_month (run_tests.TestParseTemporalArguments) ... ok
test_explicit_date_range_single_day (run_tests.TestParseTemporalArguments) ... ok
test_explicit_date_range_start_after_end (run_tests.TestParseTemporalArguments) ... ok
test_explicit_date_range_valid (run_tests.TestParseTemporalArguments) ... ok
test_iso_week_format_invalid_format (run_tests.TestParseTemporalArguments) ... ok
test_iso_week_format_valid (run_tests.TestParseTemporalArguments) ... ok
test_iso_week_format_week_01 (run_tests.TestParseTemporalArguments) ... ok
test_friday_returns_week_bounds (run_tests.TestGetWeekBounds) ... ok
test_monday_returns_same_monday (run_tests.TestGetWeekBounds) ... ok
test_returns_start_of_day (run_tests.TestGetWeekBounds) ... ok
test_sunday_returns_week_bounds (run_tests.TestGetWeekBounds) ... ok
test_various_weekdays (run_tests.TestGetWeekBounds) ... ok
test_week_span_is_7_days (run_tests.TestGetWeekBounds) ... ok
test_invalid_format_slashes (run_tests.TestParseISODate) ... ok
test_invalid_month (run_tests.TestParseISODate) ... ok
test_leap_year_date (run_tests.TestParseISODate) ... ok
test_valid_date (run_tests.TestParseISODate) ... ok
test_valid_date_formats (run_tests.TestParseISODate) ... ok
test_all_parsing_methods_return_datetime (run_tests.TestTemporalIntegration) ... ok
test_week_boundaries_are_consistent (run_tests.TestTemporalIntegration) ... ok
test_consistent_format_for_same_week (run_tests.TestParseTemporalArguments) ... ok

Ran 28 tests in 0.015s

OK
```

## Test Organization

```
tests/
├── test_menu_params.py              # Original API tests
├── test_temporal_selection.py       # Pytest version
├── run_tests.py                     # Unittest standalone runner
├── README.md                        # This file
└── __init__.py                      # Package marker
```

## Key Test Scenarios

### 1. Date Format Validation
```python
# Valid
parse_iso_date("2026-01-15")  # ✓

# Invalid
parse_iso_date("2026/01/15")  # ✗ ValueError
parse_iso_date("20260115")    # ✗ ValueError
```

### 2. Week Boundary Calculation
```python
# Any day in a week returns the same week bounds
get_week_bounds(datetime(2026, 1, 12))  # Monday
get_week_bounds(datetime(2026, 1, 16))  # Friday
# Both return (2026-01-12, 2026-01-18)
```

### 3. Temporal Argument Parsing
```python
# All equivalent for week of Jan 26-Feb 1, 2026
parse_temporal_arguments(week="2026-W05")
parse_temporal_arguments(week="2026-01-26")
parse_temporal_arguments(start_date="2026-01-26", end_date="2026-02-01")
# All return (2026-01-26, 2026-02-01)
```

## Extending Tests

To add new tests:

1. **Add to `run_tests.py`** for compatibility with both unittest and pytest:
   ```python
   def test_new_functionality(self):
       """Description of test."""
       result = function_under_test()
       self.assertEqual(result, expected_value)
   ```

2. **Add to `test_temporal_selection.py`** if pytest-specific features needed:
   ```python
   def test_new_functionality():
       """Description of test."""
       result = function_under_test()
       assert result == expected_value
   ```

## Common Test Patterns

### Testing Date Validation
```python
def test_invalid_date(self):
    with self.assertRaises(ValueError):
        parse_iso_date("invalid")
```

### Testing Date Range Logic
```python
def test_date_range(self):
    start, end = parse_temporal_arguments(
        start_date="2026-01-01",
        end_date="2026-01-31"
    )
    self.assertEqual((end - start).days, 30)
```

### Testing Week Calculations
```python
def test_week_calculation(self):
    start, end = get_week_bounds(datetime(2026, 1, 15))
    self.assertEqual(start.date(), datetime(2026, 1, 12).date())
```

## CI/CD Integration

To integrate into GitHub Actions:

```yaml
- name: Run temporal selection tests
  run: |
    pip install -e ".[dev]"
    pytest tests/bronze-tests/test_temporal_selection.py -v --tb=short
```

Or without pytest:
```yaml
- name: Run temporal selection tests
  run: python tests/bronze-tests/run_tests.py
```

## Troubleshooting

### Import Errors
If you get import errors, ensure:
1. You're running from project root: `cd c:\Users\RhysL\Desktop\Hellofresh-Menu-Analytics`
2. Scripts directory exists: `scripts/1_bronze.py`
3. Python path is set correctly in test files

### Test Failures
If tests fail:
1. Check temporal function signatures in `scripts/1_bronze.py`
2. Verify date calculations are correct for current date
3. See Implementation Checklist for known issues

## Related Documentation

- [HISTORICAL_DATA_INGESTION.md](../notes/HISTORICAL_DATA_INGESTION.md) - Complete usage guide
- [TEMPORAL_SELECTION_IMPLEMENTATION.md](../notes/TEMPORAL_SELECTION_IMPLEMENTATION.md) - Implementation details
- [QUICK_REFERENCE_TEMPORAL.md](../notes/QUICK_REFERENCE_TEMPORAL.md) - Quick reference
