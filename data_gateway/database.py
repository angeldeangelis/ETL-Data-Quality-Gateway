import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger("DataGateway.Database")

def _pg_upsert_method(conflict_keys: list[str]):
    """
    Generates an insertion method compatible with pandas.to_sql 'method' argument.
    Injects a native PostgreSQL 'ON CONFLICT DO UPDATE' or 'DO NOTHING' clause.
    """
    def chunk_upsert(table, conn, keys, data_iter):
        # Convert pandas data iterator rows into dictionaries mapped by column names
        records = [dict(zip(keys, row)) for row in data_iter]
        
        # 'table.table' exposes the native SQLAlchemy Table object managed by pandas
        insert_stmt = insert(table.table).values(records)
        
        # Dynamically map columns to update, excluding target conflict keys
        update_dict = {
            k: insert_stmt.excluded[k]
            for k in keys
            if k not in conflict_keys
        }
        
        # Execute DO UPDATE if updateable columns exist, otherwise fallback to DO NOTHING
        if update_dict:
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_keys,
                set_=update_dict
            )
        else:
            upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_keys)
            
        conn.execute(upsert_stmt)
        
    return chunk_upsert


class PostgreSQLManager:
    """
    Governs the physical connection and atomic ingestion layers
    into the database server without compromising memory integrity.
    """
    
    def __init__(self, connection_uri: str):
        # SQLAlchemy engine inherently manages the underlying connection pooling
        self.engine = create_engine(connection_uri)
        
    def prepare_db_schema(self) -> None:
        """
        Guarantees the physical schema exists in PostgreSQL with the mandatory
        PRIMARY KEY constraint required for idempotent UPSERT operations.
        """
        query = """
        CREATE TABLE IF NOT EXISTS stg_transaction (
            transaction_id VARCHAR(100) PRIMARY KEY,
            product_name VARCHAR(255),
            revenue NUMERIC(18, 2)
        );
        """
        try:
            # engine.begin() opens a transaction block and auto-commits if no exceptions occur
            with self.engine.begin() as conn:
                conn.execute(text(query))
            logger.info("Database Schema Guard: 'stg_transaction' table verified/created with PRIMARY KEY.")
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {e}")
            raise e
        
    def insert_clean_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Executes a high-speed atomic insert of the dataframe into the target table.
        Optimized via batching methods.
        """
        if df.empty:
            logger.warning(f"Execution bypassed: Provided DataFrame for table '{table_name}' is empty.")
            return
            
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=10000,
                method="multi"  # Leverages multi-row insertion for high-speed performance
            )
            logger.info(f"Database Transaction Successful: Appended data to '{table_name}'.")
        except Exception as e:
            logger.error(f"Database Ingestion Collapse on table '{table_name}': {e}")
            raise e

    def upsert_clean_data(self, df: pd.DataFrame, table_name: str, conflict_keys: list[str]) -> None:
        """
        Executes an atomic UPSERT (ON CONFLICT DO UPDATE) leveraging a custom 
        pandas-to-sqlalchemy dialect injector. Guarantees data idempotency.
        """
        if df.empty:
            logger.warning(f"Execution bypassed: Provided DataFrame for upsert on '{table_name}' is empty.")
            return
            
        try:
            # Reuses to_sql while injecting the PostgreSQL conflict resolution strategy
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",  # Allows pandas to materialize the table if missing
                index=False,
                chunksize=5000,      # Controlled chunksize to guarantee transaction stability
                method=_pg_upsert_method(conflict_keys)
            )
            logger.info(f"Database Upsert Transaction Successful for table: '{table_name}'.")
        except Exception as e:
            logger.error(f"Database Ingestion (Upsert) Collapsed for table '{table_name}': {e}")
            raise e