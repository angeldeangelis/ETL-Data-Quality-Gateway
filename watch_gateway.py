"""
watch_gateway.py
Event-Driven Automation Sentinel - Real-Time File System Observer.
"""

import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from data_gateway.engine import EnterpriseETLEngine
from data_gateway.strategies.financial import FinancialValidationStrategy
from data_gateway.database import PostgreSQLManager

# 1. System Ignition & Context Configuration
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DataGateway.Automation")

# 2. Define the Asynchronous Event Interceptor
class RawDataPayloadHandler(FileSystemEventHandler):
    """Intercepts file creation events inside the landing directory."""
    
    def __init__(self, engine: EnterpriseETLEngine):
        self.engine = engine

    def on_created(self, event):
        # Disregard directory creation events
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Guard clause: Intercept only target data payloads (.csv)
        if file_path.suffix.lower() == '.csv':
            logger.info(f"✨ Automated trigger activated! New payload detected: '{file_path.name}'")
            
            # Small cooldown to guarantee OS has finished writing the stream buffer to disk
            time.sleep(0.5) 
            
            try:
                # Command the orchestration engine to scan and swallow the new stream
                payloads = self.engine.discover_payloads()
                for stream in payloads:
                    self.engine.process_stream(stream)
            except Exception as system_error:
                logger.error(f"🚨 Event loop failed to ingest target stream payload: {system_error}")

# 3. Main Operational Life-Cycle Execution
def main():
    workspace = Path(__file__).parent
    raw_dir = workspace / "raw_data"
    
    # Securely rebuild database credentials from system environments
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    
    # Instantiate structural system blocks in RAM
    db_manager = PostgreSQLManager(connection_uri=DB_URI)
    financial_law = FinancialValidationStrategy()
    
    # DDL Guard: Run schema validation before entering the endless monitoring loop
    db_manager.prepare_db_schema()
    
    # Link blocks into the execution chassis
    engine = EnterpriseETLEngine(base_dir=workspace, strategy=financial_law, db_manager=db_manager)
    
    # Configure and spin up the hardware-level observer thread
    event_handler = RawDataPayloadHandler(engine)
    observer = Observer()
    observer.schedule(event_handler, path=str(raw_dir), recursive=False)
    
    print("\n" + "="*60)
    print(f"📡 SENTINEL ONLINE: Watching '{raw_dir.name}/' directory in real-time...")
    print("The engine will automatically capture and upsert incoming payloads.")
    print("Press Ctrl+C to shut down the monitoring supervisor safely.")
    print("="*60 + "\n")
    
    observer.start()
    try:
        while True:
            time.sleep(1) # Keeps the primary thread active without taxing CPU cycles
    except KeyboardInterrupt:
        logger.info("🛑 Initiating graceful shutdown sequence for the automation sentinel...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()