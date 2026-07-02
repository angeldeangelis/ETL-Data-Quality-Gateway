"""
main.py
System Ignition Component - Enterprise Production Gateway.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv # Only load_dotenv is needed to handle secrets
from data_gateway.engine import EnterpriseETLEngine
from data_gateway.strategies.financial import FinancialValidationStrategy
from data_gateway.database import PostgreSQLManager
from data_gateway.exceptions import DataGatewayException

# Load environment variables from the hidden .env file into system memory
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def main():
    workspace = Path(__file__).parent
    
    # Securely rebuild the URI pulling strings from system environment variables
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    
    # Materialize Components in RAM
    db_manager = PostgreSQLManager(connection_uri=DB_URI)
    financial_law = FinancialValidationStrategy()
    
    # Coupling the structural chassis
    engine = EnterpriseETLEngine(base_dir=workspace, strategy=financial_law, db_manager=db_manager)
    
    # Lifecycle execution
    payloads = engine.discover_payloads()
    print(f"\n🚀 System Active. Discovered {len(payloads)} streams to process.\n")
    
    for stream in payloads:
        try:
            engine.process_stream(stream)
        except DataGatewayException as error:
            print(f"⚠️ Isolated System Exception handled successfully: {error}")
        except Exception as unexpected:
            print(f"🚨 Unhandled infrastructure collapse: {unexpected}")

if __name__ == "__main__":
    main()