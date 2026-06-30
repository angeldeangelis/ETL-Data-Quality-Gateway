"""
data_gateway/engine.py
Core Execution Layer - Structural Integration and DLQ Router.
"""

import logging
import pandas as pd
from pathlib import Path
from data_gateway.exceptions import SchemaViolationException, DataQualitySLAException
from data_gateway.strategies.base import ValidationStrategy

logger = logging.getLogger("DataGateway.Engine")

class EnterpriseETLEngine:
    """
    Orchestration engine that governs the ingestion lifecycle, applying
    abstract grammatical contracts to filter and route data streams.
    """

    def __init__(self, base_dir: Path, strategy: ValidationStrategy, db_manager=None):
        self.base_dir = Path(base_dir)
        self.strategy = strategy  # Contract injection (Sector II / Strategy Pattern)
        self.db_manager = db_manager  # Persistence layer injection (Sector IV)
        
        # Stable system dimensions (Coordinates)
        self.raw_dir = self.base_dir / "raw_data"
        self.clean_dir = self.base_dir / "processed_data"
        self.dlq_dir = self.base_dir / "quarantine_storage"
        
        # Defensive persistence: Self-materialize the physical environment
        self._initialize_infrastructure()

    def _initialize_infrastructure(self) -> None:
        """Ensures all physical directories exist before execution."""
        for directory in [self.raw_dir, self.clean_dir, self.dlq_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def discover_payloads(self) -> list[Path]:
        """Scans the ingress coordinate for raw data streams."""
        if not self.raw_dir.exists():
            return []
        return [f for f in self.raw_dir.glob("*.csv") if f.is_file()]

    def process_stream(self, file_path: Path) -> None:
        """
        Ingests a single stream, validates its grammar via the injected strategy,
        and splits the dataset into clean and quarantine subspaces.
        """
        logger.info(f"Processing stream target: {file_path.name}")
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Critical byte-stream corruption reading {file_path.name}: {str(e)}")
            return

        # 1. Syntactic Judgment (Column Contract Validation)
        if not all(col in df.columns for col in self.strategy.required_columns):
            # The file violated the general grammar: ejected entirely to the DLQ
            destination = self.dlq_dir / f"CRITICAL_SCHEMA_{file_path.name}"
            file_path.rename(destination)
            raise SchemaViolationException(f"Stream {file_path.name} dropped. Critical schema mismatch.")

        # 2. Semantic Judgment (Vector Projection / Boolean Mask)
        pk = self.strategy.primary_key
        
        # A row is corrupt if its Primary Key is null OR if it is duplicated
        is_corrupt = df[pk].isna() | df.duplicated(subset=[pk], keep='first')

        # Bifurcation into independent subspaces
        clean_df = df[~is_corrupt]
        quarantine_df = df[is_corrupt]

        # 3. Persistence in output subspaces
        if not clean_df.empty:
            clean_output = self.clean_dir / f"clean_{file_path.name}"
            clean_df.to_csv(clean_output, index=False)
            logger.info(f"Persisted {len(clean_df)} clean vectors to {clean_output.name}")

            # Corporate injection into PostgreSQL
            if self.db_manager:
                table_target = f"stg_{self.strategy.primary_key.split('_')[0]}"
                self.db_manager.insert_clean_data(clean_df, table_name=table_target)

        if not quarantine_df.empty:
            dlq_output = self.dlq_dir / f"quarantine_{file_path.name}"
            quarantine_df.to_csv(dlq_output, index=False)
            logger.warning(f"DLQ Alert: Isolated {len(quarantine_df)} corrupt vectors to {dlq_output.name}")

        # Channel cleanup: Delete the original raw file once processed
        file_path.unlink()