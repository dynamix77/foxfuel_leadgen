"""Job to automatically rename Maps Extractor files with timestamps."""
import argparse
import logging
from pathlib import Path
from src.config import settings
from src.utils.file_rename import auto_rename_maps_extractor_files

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for renaming maps extractor files."""
    parser = argparse.ArgumentParser(
        description="Rename Maps Extractor CSV files with timestamps to avoid overwrites"
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="./data/maps_extractor",
        help="Directory containing Maps Extractor files (default: ./data/maps_extractor)"
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="organizations.csv",
        help="Filename pattern to rename (default: organizations.csv)"
    )
    
    args = parser.parse_args()
    
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Scanning {directory} for files matching '{args.pattern}'...")
    
    renamed_files = auto_rename_maps_extractor_files(directory, args.pattern)
    
    if renamed_files:
        logger.info(f"Renamed {len(renamed_files)} file(s):")
        for file_path in renamed_files:
            logger.info(f"  - {file_path.name}")
    else:
        logger.info("No files found to rename")
    
    return renamed_files


if __name__ == "__main__":
    main()

