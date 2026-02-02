#!/usr/bin/env python
"""
Standalone test runner for temporal selection functionality.
Can be run without pytest if needed.

Usage:
    python tests/run_tests.py                    # Run all tests
    python tests/run_tests.py -v                 # Verbose output
    python tests/run_tests.py TestParseISODate   # Run specific test class
"""

import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta
import importlib.util

# Add scripts directory to path
# __file__ is tests/bronze-tests/run_tests.py, so go up 3 levels to project root
project_root = Path(__file__).parent.parent.parent
scripts_dir = project_root / "scripts"
sys.path.insert(0, str(scripts_dir))

# Import bronze script functions
bronze_spec = importlib.util.spec_from_file_location("bronze", scripts_dir / "1_bronze.py")
bronze = importlib.util.module_from_spec(bronze_spec)
bronze_spec.loader.exec_module(bronze)

parse_iso_date = bronze.parse_iso_date
get_week_bounds = bronze.get_week_bounds
parse_temporal_arguments = bronze.parse_temporal_arguments


# ======================
# Test Cases
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
    
    def test_invalid_month(self):
        """Reject invalid month."""
        with self.assertRaises(ValueError):
            parse_iso_date("2026-13-01")
    
    def test_leap_year_date(self):
        """Handle leap year date correctly."""
        result = parse_iso_date("2024-02-29")
        self.assertEqual(result.day, 29)


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
    
    def test_iso_week_format_week_00(self):
        """Week 00 is calculated (no validation of week range)."""
        # Implementation doesn't validate week ranges, so W00 calculates to previous year
        start, end = parse_temporal_arguments(week="2026-W00")
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertEqual((end - start).days, 6)
    
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
        self.assertEqual((end - start).days, 59)
    
    def test_explicit_date_range_start_after_end(self):
        """Reject range where start > end."""
        with self.assertRaises(ValueError):
            parse_temporal_arguments(
                start_date="2026-01-31",
                end_date="2026-01-01"
            )
    
    def test_default_next_week(self):
        """Default with no args should return next week."""
        start, end = parse_temporal_arguments()
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)
        self.assertLess(start, end)
        self.assertEqual((end - start).days, 6)
    
    def test_consistent_format_for_same_week(self):
        """Different ways to specify same week should give same result."""
        result_iso = parse_temporal_arguments(week="2026-W05")
        result_date = parse_temporal_arguments(week="2026-01-26")
        result_range = parse_temporal_arguments(
            start_date="2026-01-26",
            end_date="2026-02-01"
        )
        
        self.assertEqual(result_iso[0].date(), result_date[0].date())
        self.assertEqual(result_iso[1].date(), result_date[1].date())
        self.assertEqual(result_iso[0].date(), result_range[0].date())
        self.assertEqual(result_iso[1].date(), result_range[1].date())


class TestTemporalIntegration(unittest.TestCase):
    """Integration tests for temporal functionality."""
    
    def test_all_parsing_methods_return_datetime(self):
        """All parsing methods should return datetime tuples."""
        results = [
            parse_temporal_arguments(week="2026-W05"),
            parse_temporal_arguments(week="2026-01-15"),
            parse_temporal_arguments(start_date="2026-01-01", end_date="2026-01-31"),
            parse_temporal_arguments(),
        ]
        
        for start, end in results:
            self.assertIsInstance(start, datetime)
            self.assertIsInstance(end, datetime)
            self.assertLessEqual(start, end)
    
    def test_week_boundaries_are_consistent(self):
        """Week boundaries should be consistent across calculations."""
        date = datetime(2026, 1, 15)
        
        direct_start, direct_end = get_week_bounds(date)
        parsed_start, parsed_end = parse_temporal_arguments(week="2026-01-15")
        
        self.assertEqual(direct_start.date(), parsed_start.date())
        self.assertEqual(direct_end.date(), parsed_end.date())


# ======================
# Test Runner
# ======================

if __name__ == "__main__":
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(loader.loadTestsFromTestCase(TestParseISODate))
    suite.addTests(loader.loadTestsFromTestCase(TestGetWeekBounds))
    suite.addTests(loader.loadTestsFromTestCase(TestParseTemporalArguments))
    suite.addTests(loader.loadTestsFromTestCase(TestTemporalIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
