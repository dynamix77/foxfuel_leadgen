"""Unit tests for NAICS local ingestion and classification."""
import pytest
import pandas as pd
import duckdb
from pathlib import Path

from src.ingest.naics_local import (
    normalize_naics_code,
    classify_sector,
    ingest_naics_local
)
from src.entity.merge import merge_naics_signals
from src.config import settings


class TestNAICSNormalization:
    """Test NAICS code normalization."""
    
    def test_normalize_naics(self):
        """Test NAICS code normalization to 6 digits."""
        assert normalize_naics_code("484110") == "484110"
        assert normalize_naics_code("4841") == "004841"
        assert normalize_naics_code("484") == "000484"
        assert normalize_naics_code("4841-10") == "484110"
        assert normalize_naics_code("4841.10") == "484110"
        assert normalize_naics_code(None) is None
        assert normalize_naics_code("") is None


class TestSectorClassification:
    """Test sector classification logic."""
    
    def test_fleet_exact_match(self):
        """Test Fleet classification with exact NAICS prefix."""
        sector, conf, notes = classify_sector("484110", "General Freight Trucking")
        assert sector == "Fleet and Transportation"
        assert conf == 100
    
    def test_healthcare_exact_match(self):
        """Test Healthcare classification with exact NAICS prefix."""
        sector, conf, notes = classify_sector("622110", "General Medical and Surgical Hospitals")
        assert sector == "Healthcare"
        assert conf == 100
    
    def test_education_keyword_match(self):
        """Test Education classification with keyword match."""
        sector, conf, notes = classify_sector(None, "Third Street School District Bus Depot")
        assert sector == "Education"
        assert conf == 70
    
    def test_construction_prefix(self):
        """Test Construction classification."""
        sector, conf, notes = classify_sector("236220", "Commercial and Institutional Building Construction")
        assert sector == "Construction"
        assert conf == 100
    
    def test_data_center_exact(self):
        """Test Data Center exact match."""
        sector, conf, notes = classify_sector("518210", "Data Processing, Hosting, and Related Services")
        assert sector == "Utilities and Data Centers"
        assert conf == 100
    
    def test_unknown(self):
        """Test Unknown classification."""
        sector, conf, notes = classify_sector("999999", "Unknown Company")
        assert sector == "Unknown"
        assert conf == 0


class TestNAICSMerge:
    """Test NAICS merge into entities."""
    
    def test_merge_within_radius(self):
        """Test that NAICS row within 150m matches entity."""
        # Create test entities
        entity_df = pd.DataFrame({
            'facility_id': ['TEST001'],
            'facility_name': ['Test Facility'],
            'latitude': [40.0],
            'longitude': [-75.0],
            'address': ['123 Main St'],
            'city': ['Philadelphia'],
            'state': ['PA'],
            'zip': ['19101']
        })
        
        # Create test NAICS data in DuckDB
        conn = duckdb.connect(settings.duckdb_path)
        conn.execute("CREATE TABLE IF NOT EXISTS signals (signal_id VARCHAR, entity_id VARCHAR, signal_type VARCHAR, signal_value VARCHAR, source VARCHAR, created_at TIMESTAMP)")
        conn.execute("DELETE FROM signals WHERE signal_type IN ('sector', 'sector_confidence')")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_naics_local (
                business_name VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip VARCHAR,
                county VARCHAR,
                naics_code VARCHAR,
                naics_title VARCHAR,
                sector_primary VARCHAR,
                sector_confidence INTEGER,
                subsector_notes VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                source VARCHAR
            )
        """)
        
        # Insert test NAICS row (within 150m - approximately 0.0014 degrees)
        conn.execute("""
            INSERT INTO raw_naics_local VALUES
            ('Test Facility', '123 Main St', 'Philadelphia', 'PA', '19101', 'Philadelphia',
             '484110', 'Trucking', 'Fleet and Transportation', 100, 'Test', 
             40.001, -75.0, 'naics_local')
        """)
        conn.close()
        
        # Merge
        result_df = merge_naics_signals(entity_df)
        
        # Verify match
        assert result_df.iloc[0]['sector_primary'] == 'Fleet and Transportation'
        assert result_df.iloc[0]['sector_confidence'] == 100
        assert result_df.iloc[0]['naics_code'] == '484110'
        
        # Verify signals table integrity: no sector_confidence signal type
        conn_check = duckdb.connect(settings.duckdb_path)
        signals_df = conn_check.execute("SELECT * FROM signals WHERE signal_type = 'sector_confidence'").df()
        assert len(signals_df) == 0, "signals table should not contain sector_confidence signal_type"
        
        # Verify sector signal exists
        sector_signals = conn_check.execute("SELECT * FROM signals WHERE signal_type = 'sector'").df()
        assert len(sector_signals) > 0, "signals table should contain sector signal_type"
        
        conn_check.close()

