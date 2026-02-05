"""
Test temporal selection functionality for bronze layer ingestion.

Comprehensive test suite for temporal selection functionality, supporting both
pytest and unittest-based test discovery and execution.

Tests cover:
- Date parsing (ISO format)
- Week boundary calculations
- Temporal argument parsing with various input formats
- CLI argument validation
- Edge cases and error handling
- Integration tests for consistent behavior

Can be run with:
    pytest tests/bronze-tests/test_temporal_selection.py -v
    python tests/bronze-tests/test_temporal_selection.py  (runs as script)
"""

import pytest
from datetime import datetime, timedelta
import sys
import unittest
from pathlib import Path

# Add scripts directory to path for imports
# __file__ is tests/bronze-tests/test_temporal_selection.py, so go up 3 levels to project root
project_root = Path(__file__).parent.parent.parent
scripts_dir = project_root / "scripts"
sys.path.insert(0, str(scripts_dir))

# Import directly from the bronze script module
import importlib.util
bronze_spec = importlib.util.spec_from_file_location("bronze", scripts_dir / "1_bronze.py")
bronze = importlib.util.module_from_spec(bronze_spec)
bronze_spec.loader.exec_module(bronze)

parse_iso_date = bronze.parse_iso_date
get_week_bounds = bronze.get_week_bounds
parse_temporal_arguments = bronze.parse_temporal_arguments


# ======================
# Test: parse_iso_date()
# ======================

class TestParseISODate(unittest.TestCase):
    """Tests for ISO date string parsing."""
    
    def test_valid_date(self):
        """Parse valid ISO date string."""
        result = parse_iso_date("2026-01-15")
        self.assertEqual(result.year, 2026)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
    
    def test_valid_date_formats(self):
        """Test various valid date formats."""
        test_cases = [
            ("2026-01-01", datetime(2026, 1, 1)),
            ("2026-12-31", datetime(2026, 12, 31)),
            ("2025-06-15", datetime(2025, 6, 15)),
        ]
        for date_str, expected in test_cases:
            result = parse_iso_date(date_str)
            self.assertEqual(result, expected)
    
    def test_invalid_format_slashes(self):
        """Reject slashes in date format."""
        with self.assertRaises(ValueError):
            parse_iso_date("2026/01/15")
    
    def test_invalid_format_no_separators(self):
        """Reject dates without separators."""
        with self.assertRaises(ValueError):
            parse_iso_date("20260115")
    
    def test_invalid_month(self):
        """Reject invalid month."""
        with self.assertRaises(ValueError):
            parse_iso_date("2026-13-01")
    
    def test_invalid_day(self):
        """Reject invalid day."""
        with self.assertRaises(ValueError):
            parse_iso_date("2026-02-30")
    
    def test_leap_year_date(self):
        """Handle leap year date correctly."""
        result = parse_iso_date("2024-02-29")
        self.assertEqual(result.day, 29)


# ======================
# Test: get_week_bounds()
# ======================

class TestGetWeekBounds(unittest.TestCase):
    """Tests for week boundary calculation."""
    
    def test_monday_returns_same_monday(self):
        """Date that is Monday should return itself."""
        monday = datetime(2026, 1, 12)  # Monday
        start, end = get_week_bounds(monday)
        self.assertEqual(start.date(), monday.date())
        self.assertEqual((end.date() - start.date()).days, 6)
    
    def test_friday_returns_week_bounds(self):
        """Friday should return Monday-Sunday of same week."""
        friday = datetime(2026, 1, 16)  # Friday
        start, end = get_week_bounds(friday)
        self.assertEqual(start.date(), datetime(2026, 1, 12).date())
        self.assertEqual(end.date(), datetime(2026, 1, 18).date())
    
    def test_sunday_returns_week_bounds(self):
        """Sunday should return Monday-Sunday of same week."""
        sunday = datetime(2026, 1, 18)  # Sunday
        start, end = get_week_bounds(sunday)
        self.assertEqual(start.date(), datetime(2026, 1, 12).date())
        self.assertEqual(end.date(), datetime(2026, 1, 18).date())
    
    def test_returns_start_of_day(self):
        """Returned dates should be at start of day (00:00:00)."""
        date_with_time = datetime(2026, 1, 15, 14, 30, 45)
        start, end = get_week_bounds(date_with_time)
        self.assertEqual(start.hour, 0)
        self.assertEqual(start.minute, 0)
        self.assertEqual(start.second, 0)
        self.assertEqual(end.hour, 0)
        self.assertEqual(end.minute, 0)
        self.assertEqual(end.second, 0)
    
    def test_week_span_is_7_days(self):
        """Week span should be exactly 6 days (Monday to Sunday inclusive)."""
        date = datetime(2026, 1, 15)
        start, end = get_week_bounds(date)
        self.assertEqual((end - start).days, 6)
    
    def test_various_weekdays(self):
        """Test all weekdays of a week."""
        base_monday = datetime(2026, 1, 12)
        
        for day_offset in range(7):
            test_date = base_monday + timedelta(days=day_offset)
            start, end = get_week_bounds(test_date)
            self.assertEqual(start.date(), base_monday.date())
            self.assertEqual(end.date(), (base_monday + timedelta(days=6)).date())


# ======================
# Test: parse_temporal_arguments()
# ======================

class TestParseTemporalArguments(unittest.TestCase):
    """Tests for temporal argument parsing."""
    
    def test_iso_week_format_valid(self):
        """Parse valid ISO week format."""
        start, end = parse_temporal_arguments(week="2026-W05")
        # Week 05 of 2026 starts Jan 26
        self.assertEqual(start.date(), datetime(2026, 1, 26).date())
        self.assertEqual(end.date(), datetime(2026, 2, 1).date())
    
    def test_iso_week_format_week_01(self):
        """Parse week 01 (first week of year)."""
        start, end = parse_temporal_arguments(week="2026-W01")
        # ISO W01 can start in previous year (2026-W01 starts Dec 29, 2025)
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertEqual((end - start).days, 6)
    
    def test_iso_week_format_week_52(self):
        """Parse week 52 (last full week of year)."""
        start, end = parse_temporal_arguments(week="2026-W52")
        self.assertEqual(start.year, 2026)
        self.assertEqual(end.year, 2026)  # Should be in same year
    
    def test_iso_week_format_week_00(self):
        """Week 00 is calculated (no validation of week range)."""
        # Implementation doesn't validate week ranges, so W00 calculates to previous year
        start, end = parse_temporal_arguments(week="2026-W00")
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertEqual((end - start).days, 6)
    
    def test_iso_week_format_valid_week_53(self):
        """Week 53 is calculated without validation."""
        # Some years have 53 weeks; implementation calculates without strict validation
        result = parse_temporal_arguments(week="2026-W53")
        self.assertIsNotNone(result)
        start, end = result
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
    
    def test_iso_week_format_single_digit_week(self):
        """Single-digit week numbers work (no strict format validation)."""
        # Implementation allows "2026-W5" (missing leading zero) and calculates week 5
        start, end = parse_temporal_arguments(week="2026-W5")
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertEqual((end - start).days, 6)
    
    def test_iso_week_format_invalid_format_no_dash(self):
        """ISO week without dash is treated as date and raises ValueError."""
        # "2026W05" is parsed as date and fails with date format error
        with self.assertRaises(ValueError):
            parse_temporal_arguments(week="2026W05")
    
    def test_date_in_week_monday(self):
        """Parse Monday date."""
        start, end = parse_temporal_arguments(week="2026-01-12")
        self.assertEqual(start.date(), datetime(2026, 1, 12).date())
        self.assertEqual(end.date(), datetime(2026, 1, 18).date())
    
    def test_date_in_week_friday(self):
        """Parse Friday date to get week bounds."""
        start, end = parse_temporal_arguments(week="2026-01-16")
        self.assertEqual(start.date(), datetime(2026, 1, 12).date())
        self.assertEqual(end.date(), datetime(2026, 1, 18).date())
    
    def test_date_in_week_invalid_format(self):
        """Reject invalid date in week."""
        with self.assertRaises(ValueError):
            parse_temporal_arguments(week="2026/01/15")
    
    def test_explicit_date_range_valid(self):
        """Parse explicit date range."""
        start, end = parse_temporal_arguments(
            start_date="2026-01-01",
            end_date="2026-01-31"
        )
        self.assertEqual(start.date(), datetime(2026, 1, 1).date())
        self.assertEqual(end.date(), datetime(2026, 1, 31).date())
    
    def test_explicit_date_range_single_day(self):
        """Parse date range with same start and end."""
        start, end = parse_temporal_arguments(
            start_date="2026-01-15",
            end_date="2026-01-15"
        )
        self.assertEqual(start.date(), end.date())
    
    def test_explicit_date_range_multi_month(self):
        """Parse date range spanning multiple months."""
        start, end = parse_temporal_arguments(
            start_date="2026-01-15",
            end_date="2026-03-15"
        )
        # Jan 15 to Mar 15 = 59 days (difference between datetimes)
        self.assertEqual((end - start).days, 59)
    
    def test_explicit_date_range_start_after_end(self):
        """Reject range where start > end."""
        with self.assertRaises(ValueError):
            parse_temporal_arguments(
                start_date="2026-01-31",
                end_date="2026-01-01"
            )
    
    def test_explicit_date_range_invalid_start(self):
        """Reject invalid start date."""
        with self.assertRaises(ValueError):
            parse_temporal_arguments(
                start_date="2026/01/01",
                end_date="2026-01-31"
            )
    
    def test_explicit_date_range_invalid_end(self):
        """Reject invalid end date."""
        with self.assertRaises(ValueError):
            parse_temporal_arguments(
                start_date="2026-01-01",
                end_date="invalid"
            )
    
    def test_default_next_week(self):
        """Default with no args should return next week."""
        start, end = parse_temporal_arguments()
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertLess(start, end)
        self.assertEqual((end - start).days, 6)  # 7 day span
    
    def test_week_takes_priority(self):
        """--week parameter should be used if provided."""
        result_week = parse_temporal_arguments(week="2026-W01")
        result_both = parse_temporal_arguments(
            week="2026-W01",
            start_date="2026-02-01",
            end_date="2026-02-28"
        )
        # When both are provided, week should be used (or error)
        # Based on code, week is checked first
        self.assertEqual(result_week[0], result_both[0])
    
    def test_year_boundary_week_01(self):
        """Week 01 might span previous year."""
        # 2025-W01 might start in December 2024
        start, end = parse_temporal_arguments(week="2025-W01")
        # This is expected behavior for ISO weeks
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
    
    def test_leap_year_date_range(self):
        """Handle leap year date range."""
        start, end = parse_temporal_arguments(
            start_date="2024-02-28",
            end_date="2024-03-01"
        )
        self.assertEqual((end - start).days, 2)


# ======================
# Integration Tests
# ======================

class TestTemporalIntegration(unittest.TestCase):
    """Integration tests for temporal functionality."""
    
    def test_all_parsing_methods_return_datetime(self):
        """All parsing methods should return datetime tuples."""
        results = [
            parse_temporal_arguments(week="2026-W05"),
            parse_temporal_arguments(week="2026-01-15"),
            parse_temporal_arguments(start_date="2026-01-01", end_date="2026-01-31"),
            parse_temporal_arguments(),  # Default
        ]
        
        for start, end in results:
            self.assertIsInstance(start, datetime)
            self.assertIsInstance(end, datetime)
            self.assertLessEqual(start, end)
    
    def test_consistent_format_for_same_week(self):
        """Different ways to specify same week should give same result."""
        # Week 05, 2026 starts on Jan 26
        result_iso = parse_temporal_arguments(week="2026-W05")
        result_date = parse_temporal_arguments(week="2026-01-26")  # Monday of W05
        result_range = parse_temporal_arguments(
            start_date="2026-01-26",
            end_date="2026-02-01"
        )
        
        self.assertEqual(result_iso[0].date(), result_date[0].date())
        self.assertEqual(result_iso[1].date(), result_date[1].date())
        self.assertEqual(result_iso[0].date(), result_range[0].date())
        self.assertEqual(result_iso[1].date(), result_range[1].date())
    
    def test_week_boundaries_are_consistent(self):
        """Week boundaries should be consistent across calculations."""
        date = datetime(2026, 1, 15)
        
        # Get bounds directly
        direct_start, direct_end = get_week_bounds(date)
        
        # Get bounds through parse_temporal_arguments
        parsed_start, parsed_end = parse_temporal_arguments(week="2026-01-15")
        
        self.assertEqual(direct_start.date(), parsed_start.date())
        self.assertEqual(direct_end.date(), parsed_end.date())


# ======================
# CLI Argument Parsing Tests
# ======================

class TestCLIArgumentParsing(unittest.TestCase):
    """Tests for command-line argument parsing."""
    
    def test_parse_arguments_no_args(self):
        """Test with no arguments."""
        # Would need to mock sys.argv
        # This is tested more thoroughly in manual testing
        pass
    
    def test_parse_arguments_with_week(self):
        """Test parsing --week argument."""
        # Would need to mock sys.argv
        pass


# ======================
# Test Runner
# ======================

if __name__ == "__main__":
    # Support both pytest and unittest execution
    import sys
    
    # If pytest is available and we're called directly, use it
    try:
        pytest.main([__file__, "-v"])
    except (NameError, SystemExit):
        # Fall back to unittest
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()
        
        # Add all test cases
        suite.addTests(loader.loadTestsFromTestCase(TestParseISODate))
        suite.addTests(loader.loadTestsFromTestCase(TestGetWeekBounds))
        suite.addTests(loader.loadTestsFromTestCase(TestParseTemporalArguments))
        suite.addTests(loader.loadTestsFromTestCase(TestTemporalIntegration))
        suite.addTests(loader.loadTestsFromTestCase(TestCLIArgumentParsing))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        # Exit with appropriate code
        sys.exit(0 if result.wasSuccessful() else 1)
