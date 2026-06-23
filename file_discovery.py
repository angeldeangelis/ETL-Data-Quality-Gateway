import logging
from pathlib import Path
from typing import List

# ==========================================
# 1. ENTERPRISE LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataQualityGateway_Discovery")

def get_valid_csv_files(source_dir: Path) -> List[Path]:
    """
    Scans the target directory and returns a validated list of CSV file paths.
    Acts as a first-pass filter to prevent non-tabular payloads from crashing the pipeline.
    
    Args:
        source_dir (Path): The root directory containing raw data payloads.
        
    Returns:
        List[Path]: A list of validated Path objects pointing to CSV files.
    """
    valid_files = []
    
    # Pre-flight check: Ensure source directory integrity
    if not source_dir.exists() or not source_dir.is_dir():
        logger.error(f"CRITICAL: Raw data directory missing or invalid -> {source_dir}")
        return valid_files
        
    logger.info(f"Initializing payload scan in directory: {source_dir.name}...")
    
    # Directory traversal and strict extension filtering
    for file_path in source_dir.iterdir():
        
        # Enforce lowercase suffix check to handle user-input anomalies (e.g., .CSV vs .csv)
        if file_path.is_file() and file_path.suffix.lower() == ".csv":
            logger.info(f"Validated CSV payload: {file_path.name}")
            valid_files.append(file_path)
            
        elif file_path.is_file():
            logger.warning(f"Ignored non-compliant file format: {file_path.name}")
            
    return valid_files

# ==========================================
# 2. STANDALONE EXECUTION / TESTING
# ==========================================
if __name__ == "__main__":
    # Resolve dynamic absolute paths to ensure cross-platform environment stability
    BASE_DIR = Path(__file__).resolve().parent
    raw_data_folder = BASE_DIR / "raw_data"
    
    # Execute file discovery module
    discovered_files = get_valid_csv_files(raw_data_folder)
    
    logger.info(f"=== DISCOVERY COMPLETE: {len(discovered_files)} payloads queued for ETL ingestion ===")