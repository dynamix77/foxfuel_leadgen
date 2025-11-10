"""QA report generation module."""
import logging
import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
from src.config import settings

logger = logging.getLogger(__name__)


def generate_qa_report():
    """
    Generate QA report with coverage, code mapping, and sector composition.
    
    Writes to ./out/qa_report_YYYYMMDD_HHMM.csv
    """
    logger.info("Generating QA report...")
    
    conn = duckdb.connect(settings.duckdb_path)
    
    # Load data
    try:
        entities_df = conn.execute("SELECT * FROM raw_pa_tanks").df()
        if not entities_df.empty and 'facility_id' in entities_df.columns:
            entities_df['facility_id'] = entities_df['facility_id'].astype(str)
    except Exception:
        logger.error("Could not load raw_pa_tanks table")
        return
    
    try:
        scores_df = conn.execute("SELECT * FROM lead_score").df()
        if not scores_df.empty and 'entity_id' in scores_df.columns:
            scores_df['entity_id'] = scores_df['entity_id'].astype(str)
    except Exception:
        scores_df = pd.DataFrame()
        logger.warning("No lead_score table found")
    
    try:
        naics_df = conn.execute("SELECT * FROM raw_naics_local").df()
    except Exception:
        naics_df = pd.DataFrame()
        logger.warning("No NAICS data found")
    
    report_sections = []
    
    # Section A: Coverage
    report_sections.append("=== SECTION A: COVERAGE ===")
    
    if not entities_df.empty:
        coverage_data = []
        for county in entities_df['county'].dropna().unique():
            county_df = entities_df[entities_df['county'] == county]
            total = len(county_df)
            diesel = county_df['is_diesel_like'].sum()
            active = county_df['is_active_like'].sum()
            geocoded = county_df['latitude'].notna().sum()
            diesel_pct = (diesel / total * 100) if total > 0 else 0
            active_pct = (active / total * 100) if total > 0 else 0
            geocode_pct = (geocoded / total * 100) if total > 0 else 0
            
            # NAICS sector coverage
            if 'sector_primary' in county_df.columns:
                with_sector = county_df['sector_primary'].notna().sum()
                sector_pct = (with_sector / total * 100) if total > 0 else 0
            else:
                with_sector = 0
                sector_pct = 0
            
            coverage_data.append({
                'county': county,
                'total_sites': total,
                'diesel_like': diesel,
                'diesel_like_pct': round(diesel_pct, 1),
                'active_like': active,
                'active_like_pct': round(active_pct, 1),
                'geocoded': geocoded,
                'geocode_pct': round(geocode_pct, 1),
                'with_sector': with_sector,
                'sector_pct': round(sector_pct, 1)
            })
        
        coverage_df = pd.DataFrame(coverage_data)
        report_sections.append("County Coverage:")
        report_sections.append(coverage_df.to_string(index=False))
        report_sections.append("")
    
    # Section B: Code mapping sanity
    report_sections.append("=== SECTION B: CODE MAPPING SANITY ===")
    
    if not entities_df.empty:
        # SUBSTANCE_CODE → diesel_like crosstab
        if 'product_code' in entities_df.columns and 'is_diesel_like' in entities_df.columns:
            substance_crosstab = pd.crosstab(
                entities_df['product_code'],
                entities_df['is_diesel_like'],
                margins=True
            )
            report_sections.append("SUBSTANCE_CODE → diesel_like crosstab:")
            report_sections.append(substance_crosstab.to_string())
            report_sections.append("")
        
        # STATUS_CODE → active_like crosstab
        if 'status_code' in entities_df.columns and 'is_active_like' in entities_df.columns:
            status_crosstab = pd.crosstab(
                entities_df['status_code'],
                entities_df['is_active_like'],
                margins=True
            )
            report_sections.append("STATUS_CODE → active_like crosstab:")
            report_sections.append(status_crosstab.to_string())
            report_sections.append("")
    
    # Section C: Sector composition
    report_sections.append("=== SECTION C: SECTOR COMPOSITION ===")
    
    if not entities_df.empty:
        # Merge with scores
        if not scores_df.empty:
            merged = entities_df.merge(scores_df, left_on='facility_id', right_on='entity_id', how='left')
        else:
            merged = entities_df.copy()
            merged['score'] = None
        
        # Get sector from signals if not on entity
        if 'sector_primary' not in merged.columns:
            try:
                signals_sector = conn.execute("""
                    SELECT entity_id, signal_value as sector_primary
                    FROM signals
                    WHERE signal_type = 'sector'
                """).df()
                if not signals_sector.empty:
                    merged = merged.merge(signals_sector, left_on='facility_id', right_on='entity_id', how='left', suffixes=('', '_sig'))
                    if 'sector_primary_sig' in merged.columns:
                        merged['sector_primary'] = merged['sector_primary'].fillna(merged['sector_primary_sig'])
            except Exception:
                pass
        
        sector_data = []
        total_entities = len(merged)
        
        if 'sector_primary' in merged.columns:
            for sector in merged['sector_primary'].dropna().unique():
                sector_df = merged[merged['sector_primary'] == sector]
                count = len(sector_df)
                pct = (count / total_entities * 100) if total_entities > 0 else 0
                avg_score = sector_df['score'].mean() if 'score' in sector_df.columns else None
                
                sector_data.append({
                    'sector_primary': sector,
                    'count': count,
                    'pct_of_total': round(pct, 1),
                    'avg_score': round(avg_score, 1) if pd.notna(avg_score) else None
                })
        
        if sector_data:
            sector_df = pd.DataFrame(sector_data)
            report_sections.append("Entities by sector_primary:")
            report_sections.append(sector_df.to_string(index=False))
        else:
            report_sections.append("No sector assignments found")
    
    # Write report
    report_text = "\n".join(report_sections)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    report_path = settings.out_dir / f"qa_report_{timestamp}.csv"
    
    # Write as CSV-friendly format (sections as rows)
    report_rows = []
    for line in report_text.split('\n'):
        if line.strip():
            report_rows.append({'section': line})
    
    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(report_path, index=False, encoding='utf-8')
    
    logger.info(f"QA report written to {report_path}")
    
    # Also write summary stats
    summary_path = settings.out_dir / f"qa_summary_{timestamp}.csv"
    summary_data = []
    
    if not entities_df.empty:
        summary_data.append({'metric': 'total_entities', 'value': len(entities_df)})
        summary_data.append({'metric': 'diesel_like', 'value': entities_df['is_diesel_like'].sum()})
        summary_data.append({'metric': 'active_like', 'value': entities_df['is_active_like'].sum()})
        summary_data.append({'metric': 'geocoded', 'value': entities_df['latitude'].notna().sum()})
        if 'sector_primary' in entities_df.columns:
            summary_data.append({'metric': 'with_sector', 'value': entities_df['sector_primary'].notna().sum()})
    
    if not scores_df.empty:
        summary_data.append({'metric': 'tier_a', 'value': len(scores_df[scores_df['tier'] == 'Tier A'])})
        summary_data.append({'metric': 'tier_b', 'value': len(scores_df[scores_df['tier'] == 'Tier B'])})
        summary_data.append({'metric': 'tier_c', 'value': len(scores_df[scores_df['tier'] == 'Tier C'])})
        summary_data.append({'metric': 'park', 'value': len(scores_df[scores_df['tier'] == 'Park'])})
    
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(summary_path, index=False, encoding='utf-8')
        logger.info(f"QA summary written to {summary_path}")
    
    conn.close()

