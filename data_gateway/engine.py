"""
data_gateway/engine.py
Core Execution Layer - Industrial OOP Pipeline Orchestrator.
"""

import logging
from pathlib import Path
from data_gateway.exceptions import SchemaViolationException

# Isolating the logger registry for infrastructure audit tracking
logger = logging.getLogger("DataGateway.Engine")

class EnterpriseETLEngine:
    """
    Enterprise-grade pipeline engine responsible for governing the execution
    lifecycle of data extraction, state validation, and system persistence.
    """
    
    def __init__(self, base_dir: Path):
        """
        Initializes the processing engine and materializes the physical path topology.
        
        Args:
            base_dir (Path): The absolute root path of the project workspace.
        """
        # Encapsulating State: Binding system coordinates to the object instance
        self.base_dir = base_dir
        self.raw_dir = base_dir / "raw_data"
        self.clean_dir = base_dir / "processed_data"
        self.dlq_dir = base_dir / "quarantine_storage"
        
        logger.info("EnterpriseETLEngine instance successfully materialized in RAM.")

    def discover_payloads(self) -> list[Path]:
        """
        Scans the ingress directory for target CSV payloads using the internal state.
        
        Returns:
            list[Path]: A collection of validated absolute paths pointing to CSV files.
        """
        # Defensive validation constraint to isolate directory execution anomalies
        if not self.raw_dir.exists():
            logger.warning(f"Ingress directory target does not exist in filesystem: {self.raw_dir}")
            return []
        
        # High-performance list comprehension utilizing optimized glob filtering at byte level
        return [f for f in self.raw_dir.glob("*.csv") if f.is_file()]