"""Utility for automatically renaming files in maps_extractor directory."""
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def rename_with_timestamp(file_path: Path, prefix: str = "", suffix: str = "") -> Path:
    """
    Rename a file with timestamp to avoid overwrites.
    
    Args:
        file_path: Path to file to rename
        prefix: Optional prefix for new filename
        suffix: Optional suffix before extension
    
    Returns:
        New file path
    """
    if not file_path.exists():
        return file_path
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Build new filename
    stem = file_path.stem
    extension = file_path.suffix
    
    if prefix:
        new_stem = f"{prefix}_{stem}_{timestamp}"
    elif suffix:
        new_stem = f"{stem}_{suffix}_{timestamp}"
    else:
        new_stem = f"{stem}_{timestamp}"
    
    new_path = file_path.parent / f"{new_stem}{extension}"
    
    # If timestamped name also exists, add counter
    counter = 1
    original_new_path = new_path
    while new_path.exists():
        new_stem_with_counter = f"{new_stem}_{counter}"
        new_path = file_path.parent / f"{new_stem_with_counter}{extension}"
        counter += 1
    
    # Rename file
    file_path.rename(new_path)
    logger.info(f"Renamed {file_path.name} -> {new_path.name}")
    
    return new_path


def auto_rename_maps_extractor_files(directory: Path, pattern: str = "organizations.csv") -> list[Path]:
    """
    Automatically rename files matching pattern in maps_extractor directory.
    
    Args:
        directory: Directory to scan
        pattern: Filename pattern to match (default: "organizations.csv")
    
    Returns:
        List of renamed file paths
    """
    directory = Path(directory)
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return []
    
    renamed_files = []
    
    # Find all files matching pattern
    matching_files = list(directory.glob(pattern))
    
    # Also check for exact match
    exact_match = directory / pattern
    if exact_match.exists() and exact_match not in matching_files:
        matching_files.append(exact_match)
    
    for file_path in matching_files:
        # Skip if already has timestamp pattern (YYYYMMDD_HHMMSS)
        if "_" in file_path.stem and len(file_path.stem.split("_")[-1]) == 15:
            # Check if last part looks like timestamp
            last_part = file_path.stem.split("_")[-1]
            if last_part.isdigit() and len(last_part) == 15:
                logger.debug(f"Skipping {file_path.name} - already has timestamp")
                continue
        
        try:
            new_path = rename_with_timestamp(file_path)
            renamed_files.append(new_path)
        except Exception as e:
            logger.error(f"Failed to rename {file_path}: {e}")
    
    return renamed_files

