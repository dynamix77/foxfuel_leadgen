"""File I/O utilities for CSV and XLSX."""
import pandas as pd
from pathlib import Path
from typing import Union
import logging

logger = logging.getLogger(__name__)


def read_data_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Read CSV or XLSX file into DataFrame.
    
    Args:
        file_path: Path to CSV or XLSX file
    
    Returns:
        DataFrame with file contents
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is not supported
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    suffix = file_path.suffix.lower()
    
    try:
        if suffix == ".csv":
            df = pd.read_csv(file_path, low_memory=False)
        elif suffix == ".xlsx":
            # New Excel format - use openpyxl
            df = pd.read_excel(file_path, engine="openpyxl")
        elif suffix == ".xls":
            # Old Excel format - use xlrd
            try:
                df = pd.read_excel(file_path, engine="xlrd")
            except Exception as xlrd_error:
                # If xlrd fails, try openpyxl in case file is misnamed
                logger.warning(f"xlrd failed for {file_path}, trying openpyxl: {xlrd_error}")
                try:
                    df = pd.read_excel(file_path, engine="openpyxl")
                except Exception:
                    # Re-raise original xlrd error
                    raise xlrd_error
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
        
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


def write_preview_csv(df: pd.DataFrame, output_path: Union[str, Path], max_rows: int = 1000):
    """
    Write a preview CSV with first N rows.
    
    Args:
        df: DataFrame to write
        output_path: Output file path
        max_rows: Maximum number of rows to write
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    preview_df = df.head(max_rows)
    preview_df.to_csv(output_path, index=False)
    logger.info(f"Wrote preview CSV with {len(preview_df)} rows to {output_path}")

