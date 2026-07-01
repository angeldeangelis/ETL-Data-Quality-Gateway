"""
data_gateway/database.py
Persistence Layer - Agnostic PostgreSQL Transaction Manager.
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, inspect

logger = logging.getLogger("DataGateway.Database")

class PostgreSQLManager:
    """
    Governs the physical connection and atomic ingestion of clean data vectors
    into the database server without compromising memory integrity.
    """

    def __init__(self, connection_uri: str):
        # The SQLAlchemy engine handles the connection pooling automatically
        self.engine = create_engine(connection_uri)

    def insert_clean_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Executes an atomic insert of the dataframe into the specified SQL table.
        If the table does not exist, it materializes it based on the dataframe structure.
        """
        try:
            # .to_sql is a high-speed method that transforms the matrix into SQL inserts
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",  # If the table exists, append new vectors to the end
                index=False          # Do not persist the dataframe row index as a column
            )
            logger.info(f"Database Transaction Success: Injected {len(df)} rows into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Database Ingestion Collapse on table '{table_name}': {str(e)}")
            # Reraise the exception so the main orchestrator knows the persistence layer failed
            raise e