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

    def __init__(self, base_dir: Path, strategy: ValidationStrategy):
        self.base_dir = Path(base_dir)
        self.strategy = strategy  # Inyección del contrato (Sector II)
        
        # Dimensiones estables del sistema (Coordenadas)
        self.raw_dir = self.base_dir / "raw_data"
        self.clean_dir = self.base_dir / "processed_data"
        self.dlq_dir = self.base_dir / "quarantine_storage"
        
        # Persistencia defensiva: Auto-materializar el entorno físico
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
        and splits the dataset into clean and quarantine subplaces.
        """
        logger.info(f"Processing stream target: {file_path.name}")
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Critical byte-stream corruption reading {file_path.name}: {str(e)}")
            return

        # 1. Juicio Sintáctico (Validación del Contrato de Columnas)
        if not all(col in df.columns for col in self.strategy.required_columns):
            # El archivo violó la gramática general: se eyecta completo al DLQ
            destination = self.dlq_dir / f"CRITICAL_SCHEMA_{file_path.name}"
            file_path.rename(destination)
            raise SchemaViolationException(f"Stream {file_path.name} dropped. Critical schema mismatch.")

        # 2. Juicio Semántico (Proyección Vectorial / Máscara Booleana)
        pk = self.strategy.primary_key
        
        # Una fila es corrupta si su Clave Primaria es nula O si está duplicada
        is_corrupt = df[pk].isna() | df.duplicated(subset=[pk], keep='first')

        # Bifurcación en subespacios independientes
        clean_df = df[~is_corrupt]
        quarantine_df = df[is_corrupt]

        # 3. Persistencia en los subespacios de salida
        if not clean_df.empty:
            clean_output = self.clean_dir / f"clean_{file_path.name}"
            clean_df.to_csv(clean_output, index=False)
            logger.info(f"Persisted {len(clean_df)} clean vectors to {clean_output.name}")

        if not quarantine_df.empty:
            dlq_output = self.dlq_dir / f"quarantine_{file_path.name}"
            quarantine_df.to_csv(dlq_output, index=False)
            logger.warning(f"DLQ Alert: Isolated {len(quarantine_df)} corrupt vectors to {dlq_output.name}")

        # Limpieza del canal: Eliminar el archivo crudo original una vez procesado
        file_path.unlink()