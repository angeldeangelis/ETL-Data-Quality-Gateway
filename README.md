## 📡 Event-Driven Architecture & Idempotent Ingestion (2026 Update)

The system has evolved from a sequential batch pipeline into a reactive, event-driven data engine protected against data duplication.

### 1. Sentinel Observer (`watch_gateway.py`)
* **Real-Time Monitoring:** Asynchronous monitoring of the `raw_data/` directory leveraging the `watchdog` library.
* **Automated Trigger:** Immediate absorption and ignition of the ETL lifecycle the exact millisecond a new data payload (`.csv`) lands.

### 2. Atomic & Idempotent Persistence Layer (`PostgreSQLManager`)
* **Idempotency Guarantee:** Replaced blind inserts with a native PostgreSQL **UPSERT** strategy (`ON CONFLICT DO UPDATE`).
* **Schema Guard (DDL Guard):** Automated initialization of physical `PRIMARY KEY` constraints on the `transaction_id` column to guarantee conflict arbitration without compromising financial data consistency.