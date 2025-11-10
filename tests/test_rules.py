"""Unit tests for PA tanks classification and bucket logic."""
import pytest
import pandas as pd

from src.ingest.pa_tanks import (
    classify_diesel_like,
    classify_active_like,
    get_capacity_bucket,
    clean_capacity,
    DIESEL_LIKE_CODES,
    NON_DIESEL_CODES,
    ACTIVE_STATUS
)


class TestDieselClassification:
    """Test diesel-like product code classification."""
    
    def test_all_diesel_like_codes(self):
        """Test that all diesel-like codes return True deterministically."""
        for code in DIESEL_LIKE_CODES:
            assert classify_diesel_like(code) is True, f"{code} should be diesel-like"
            assert classify_diesel_like(code.lower()) is True, f"{code.lower()} should be diesel-like (case insensitive)"
    
    def test_all_non_diesel_codes(self):
        """Test that all non-diesel codes return False deterministically."""
        for code in NON_DIESEL_CODES:
            assert classify_diesel_like(code) is False, f"{code} should NOT be diesel-like"
            assert classify_diesel_like(code.lower()) is False, f"{code.lower()} should NOT be diesel-like (case insensitive)"
    
    def test_diesel_like_codes(self):
        """Test that diesel-like codes return True."""
        assert classify_diesel_like("DIESL") is True
        assert classify_diesel_like("BIDSL") is True
        assert classify_diesel_like("HO") is True
        assert classify_diesel_like("KERO") is True
        assert classify_diesel_like("diesl") is True  # Case insensitive
    
    def test_non_diesel_codes(self):
        """Test that non-diesel codes return False."""
        assert classify_diesel_like("GAS") is False
        assert classify_diesel_like("AVGAS") is False
        assert classify_diesel_like("JET") is False
        assert classify_diesel_like("ETHNL") is False
        assert classify_diesel_like("OTHER") is False
    
    def test_none_and_empty(self):
        """Test None and empty values return False."""
        assert classify_diesel_like(None) is False
        assert classify_diesel_like("") is False
        assert classify_diesel_like("   ") is False


class TestActiveClassification:
    """Test active status classification."""
    
    def test_all_active_status_codes(self):
        """Test that all active status codes return True deterministically."""
        for status in ACTIVE_STATUS:
            assert classify_active_like(status) is True, f"{status} should be active"
            assert classify_active_like(status.lower()) is True, f"{status.lower()} should be active (case insensitive)"
    
    def test_active_status(self):
        """Test that 'C' status returns True."""
        assert classify_active_like("C") is True
        assert classify_active_like("c") is True  # Case insensitive
    
    def test_inactive_status(self):
        """Test that non-'C' status returns False."""
        assert classify_active_like("T") is False
        assert classify_active_like("I") is False
        assert classify_active_like("X") is False
        assert classify_active_like("") is False
    
    def test_none_and_empty(self):
        """Test None and empty values return False."""
        assert classify_active_like(None) is False
        assert classify_active_like("") is False


class TestCapacityBucket:
    """Test capacity bucket logic."""
    
    def test_bucket_20k_plus(self):
        """Test 20K+ bucket."""
        assert get_capacity_bucket(20000) == "20K+"
        assert get_capacity_bucket(25000) == "20K+"
        assert get_capacity_bucket(100000) == "20K+"
    
    def test_bucket_10k_20k(self):
        """Test 10K-20K bucket."""
        assert get_capacity_bucket(10000) == "10K-20K"
        assert get_capacity_bucket(15000) == "10K-20K"
        assert get_capacity_bucket(19999) == "10K-20K"
    
    def test_bucket_5k_10k(self):
        """Test 5K-10K bucket."""
        assert get_capacity_bucket(5000) == "5K-10K"
        assert get_capacity_bucket(7500) == "5K-10K"
        assert get_capacity_bucket(9999) == "5K-10K"
    
    def test_bucket_1k_5k(self):
        """Test 1K-5K bucket."""
        assert get_capacity_bucket(1000) == "1K-5K"
        assert get_capacity_bucket(2500) == "1K-5K"
        assert get_capacity_bucket(4999) == "1K-5K"
    
    def test_bucket_under_1k(self):
        """Test <1K bucket."""
        assert get_capacity_bucket(999) == "<1K"
        assert get_capacity_bucket(500) == "<1K"
        assert get_capacity_bucket(0) == "<1K"
        assert get_capacity_bucket(None) == "<1K"
    
    def test_edge_cases(self):
        """Test edge cases for bucket boundaries."""
        # Boundary values
        assert get_capacity_bucket(999) == "<1K"
        assert get_capacity_bucket(1000) == "1K-5K"
        assert get_capacity_bucket(4999) == "1K-5K"
        assert get_capacity_bucket(5000) == "5K-10K"
        assert get_capacity_bucket(9999) == "5K-10K"
        assert get_capacity_bucket(10000) == "10K-20K"
        assert get_capacity_bucket(19999) == "10K-20K"
        assert get_capacity_bucket(20000) == "20K+"


class TestCapacityCleaning:
    """Test capacity string cleaning."""
    
    def test_numeric_extraction(self):
        """Test extracting numbers from strings."""
        assert clean_capacity("10000") == 10000.0
        assert clean_capacity("10,000") == 10000.0
        assert clean_capacity("10,000.5") == 10000.5
        assert clean_capacity("10000 gallons") == 10000.0
        assert clean_capacity("Capacity: 10,000") == 10000.0
    
    def test_none_and_empty(self):
        """Test None and empty values."""
        assert clean_capacity(None) is None
        assert clean_capacity("") is None
        assert clean_capacity("   ") is None
    
    def test_invalid_values(self):
        """Test invalid values return None."""
        assert clean_capacity("N/A") is None
        assert clean_capacity("Unknown") is None

