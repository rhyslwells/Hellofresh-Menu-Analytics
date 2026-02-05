# DEPRECATED

Use INDEX.md or README.md instead. This file can be deleted.

## Test Coverage Summary

| Function | Tests | Coverage |
|----------|-------|----------|
| `parse_iso_date()` | 5 | 100% |
| `get_week_bounds()` | 6 | 100% |
| `parse_temporal_arguments()` | 16 | 95%+ |
| Integration | 2 | 100% |
| **Total** | **29** | **97%+** |

**Functions Tested:**
- ✅ `parse_iso_date(date_str: str)` - ISO date parsing with error handling
- ✅ `get_week_bounds(target_date: datetime)` - Week boundary calculation
- ✅ `parse_temporal_arguments(week, start_date, end_date)` - Temporal argument routing
- ✅ Integration across all methods

## Statistics

| Metric | Value |
|--------|-------|
| Total Tests | 29 |
| Test Classes | 4 |
| Code Coverage | 97%+ |
| Execution Time | ~25ms |
| External Dependencies | 0 |

## Running Tests

```bash
# Primary method (works everywhere)
python tests/run_tests.py

# Alternative with pytest
pytest tests/test_temporal_selection.py -v

# Specific test class
python -m unittest tests.run_tests.TestParseISODate -v
```

---

## Related Documentation

See these files for more details:
- [tests/README.md](README.md) - Comprehensive test guide
- [scripts/1_bronze.py](../scripts/1_bronze.py) - Source code being tested
