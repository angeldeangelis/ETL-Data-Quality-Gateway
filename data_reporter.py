import json
import logging
from pathlib import Path

# ==========================================
# ENTERPRISE LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataQualityGateway_Reporter")

def generate_health_report(file_name: str, total_rows: int, missing_rows: int, duplicate_rows: int) -> dict:
    """
    Compiles data quality analytics for an ingested payload and generates an audit manifest.
    Triggers operational thresholds alerts if data corruption indexes cross business limits.
    
    Args:
        file_name (str): Name of the processed target file.
        total_rows (int): Initial row count before data quality steps.
        missing_rows (int): Total rows dropped due to NULL constraints.
        duplicate_rows (int): Total rows dropped due to Primary Key duplication.
        
    Returns:
        dict: A structured metadata ledger ready for JSON serialization.
    """
    logger.info(f"Compiling analytical data quality metrics for: {file_name}")
    
    # Prevent ZeroDivisionError on empty source files
    valid_rows = total_rows - (missing_rows + duplicate_rows)
    integrity_rate = (valid_rows / total_rows) * 100 if total_rows > 0 else 0.0
    
    # Constructing the enterprise metadata schema
    report = {
        "source_payload": file_name,
        "metrics": {
            "total_records_received": total_rows,
            "valid_records_retained": valid_rows,
            "null_records_purged": missing_rows,
            "duplicate_records_purged": duplicate_rows
        },
        "quality_metrics": {
            "data_integrity_rate": round(integrity_rate, 2)
        },
        "status": "PASS" if integrity_rate >= 80.0 else "ALERT"
    }
    
    # OPERATIONAL ALERTS BREAKPOINT
    # Strict 80% SLA threshold enforcement for automated operational workflows
    if report["status"] == "ALERT":
        logger.error(
            f"🚨 CRITICAL DATA QUALITY INCIDENT: Payload '{file_name}' "
            f"failed SLA criteria with a dangerous integrity rate of {report['quality_metrics']['data_integrity_rate']}%."
        )
    else:
        logger.info(f"✅ Data quality SLA verified for {file_name}: {report['quality_metrics']['data_integrity_rate']}% compliance.")
        
    return report

def save_audit_log(report: dict, output_dir: Path):
    """
    Persists the data quality report object into local disk storage as a standardized JSON ledger.
    """
    output_dir.mkdir(exist_ok=True)
    source_name = Path(report["source_payload"]).stem
    log_path = output_dir / f"audit_{source_name}.json"
    
    try:
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        logger.info(f"Audit registry successfully materialized to disk: {log_path.name}")
    except Exception as e:
        logger.error(f"Operational Failure: Unable to write data quality ledger. Exception: {str(e)}")

if __name__ == "__main__":
    # Isolated unit testing execution block
    BASE_DIR = Path(__file__).resolve().parent
    AUDIT_FOLDER = BASE_DIR / "audit_logs"
    
    # Simulating the metadata feedback of a corrupted invoice payload
    mock_report = generate_health_report(
        file_name="facturas_abril.csv",
        total_rows=6,
        missing_rows=2,
        duplicate_rows=1
    )
    
    save_audit_log(mock_report, AUDIT_FOLDER)