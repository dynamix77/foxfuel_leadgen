"""Entity normalization module."""
import logging
import pandas as pd
from src.utils.addresses import normalize_address, create_street_key

logger = logging.getLogger(__name__)


def normalize_entities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize entity fields using address utilities.
    
    Args:
        df: Input DataFrame with entity data
    
    Returns:
        DataFrame with normalized fields
    """
    logger.info(f"Normalizing {len(df)} entities...")
    
    # Create normalized address field
    if 'address' in df.columns and 'city' in df.columns:
        df['normalized_address'] = df.apply(
            lambda row: normalize_address(
                row.get('address'),
                row.get('address_2'),
                row.get('city'),
                row.get('state'),
                row.get('zip'),
                'USA'
            ),
            axis=1
        )
        
        # Create street key for matching
        df['street_key'] = df['normalized_address'].apply(create_street_key)
    
    logger.info("Entity normalization complete")
    return df

