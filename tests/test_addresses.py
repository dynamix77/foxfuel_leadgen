"""Unit tests for address normalization."""
import pytest

from src.utils.addresses import (
    normalize_address,
    create_street_key,
    parse_address
)


class TestAddressNormalization:
    """Test address normalization functions."""
    
    def test_basic_normalization(self):
        """Test basic address normalization."""
        result = normalize_address(
            "123 Main St",
            "Suite 100",
            "Philadelphia",
            "PA",
            "19101",
            "USA"
        )
        assert result == "123 Main St, Suite 100, Philadelphia, PA, 19101, USA"
    
    def test_minimal_address(self):
        """Test with minimal components."""
        result = normalize_address(
            "123 Main St",
            city="Philadelphia",
            state="PA"
        )
        assert result == "123 Main St, Philadelphia, PA, USA"
    
    def test_empty_components(self):
        """Test with empty/None components."""
        result = normalize_address(
            "123 Main St",
            None,
            "Philadelphia",
            "PA",
            None
        )
        assert result == "123 Main St, Philadelphia, PA, USA"
    
    def test_street_key_creation(self):
        """Test street key creation."""
        key1 = create_street_key("123 Main Street")
        key2 = create_street_key("123 Main St")
        assert key1 == key2  # Should normalize similarly
        
        key3 = create_street_key("456 Oak Avenue")
        assert key3 != key1  # Different streets
    
    def test_street_key_normalization(self):
        """Test that street key normalizes punctuation."""
        key1 = create_street_key("123 Main St.")
        key2 = create_street_key("123 Main St")
        assert key1 == key2
    
    def test_parse_address(self):
        """Test address parsing with usaddress."""
        parsed = parse_address("123 Main St, Philadelphia, PA 19101")
        assert isinstance(parsed, dict)
        # usaddress may return various fields, just check it doesn't crash

