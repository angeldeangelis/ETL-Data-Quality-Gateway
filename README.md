# Automated Financial Data Infrastructure & Sentinel Observer

## Executive Summary
Enterprise-grade data infrastructure engineered for high-fidelity financial data processing. This system implements a **Sentinel Observer pattern** to ensure continuous monitoring and real-time validation of asset price streams, effectively bridging the gap between raw data ingestion and analytical readiness.

The architecture prioritizes **Data Integrity** and **Fault Tolerance** through strict schema guarding and a decoupled lifecycle management approach.

---

## Architectural Pillars

### 1. The Sentinel Observer
The core of the system is the `Sentinel Observer`, an intelligent monitoring layer that decouples data flow from execution logic. By utilizing abstract validation contracts, the Sentinel provides:
* **Proactive Validation:** Intercepts anomalies before they propagate to the processing pipeline.
* **Decoupled Orchestration:** Isolates the observation layer from the execution layer, allowing for independent scaling and failure handling.

### 2. Data Lifecycle Management
The infrastructure implements a robust multi-stage storage strategy to maintain data quality:
* **`raw_data`**: Immutable landing zone for ingested market data.
* **`processed_data`**: Validated, clean datasets ready for quantitative analysis.
* **`quarantine_storage`**: Dedicated isolation environment for corrupted or anomalous data frames, preventing pipeline pollution while preserving forensic data for audit.

### 3. PostgreSQL & Schema Guards
Built for production-level durability. The integration with PostgreSQL is managed through custom interface managers that enforce strict data integrity protocols, ensuring that only schema-compliant data persists in the storage layer.

---

## Technical Highlights
* **Event-Driven Architecture**: Utilizes `watchdog` to monitor the file system in real-time, enabling immediate ingestion and validation triggers.
* **Strategy Pattern**: Implementation of `FinancialValidationStrategy` to decouple specific financial logic from the core ETL engine.
* **Object-Oriented Design**: Engineered with a modular `EnterpriseETLEngine` and custom exception hierarchies for graceful failure handling.
* **Zero-Trust Ingestion**: Every incoming data packet is treated as untrusted and subjected to validation before entering the `processed_data` state.

---

## Project Structure
The repository is modularized to support Separation of Concerns (SoC) and maintainability, with logic encapsulated within functional modules:

```text
├── data_gateway/           # Primary infrastructure module
│   ├── strategies/         # Abstract validation contracts and execution logic
│   ├── database.py         # PostgreSQL manager with schema guards
│   ├── engine.py           # Core ingestion/processing engine
│   └── exceptions.py       # Custom exception hierarchies
├── processed_data/         # High-fidelity datasets (Post-validation)
├── quarantine_storage/     # Anomaly isolation and forensic log storage
├── raw_data/               # Immutable ingestion landing zone
├── main.py                 # System entry point and orchestrator
└── watch_gateway.py        # Sentinel Observer implementation

Tech Stack
Language: Python 3.10+

Database: PostgreSQL (with SQLAlchemy/Psycopg2 for ORM/Connector)

Real-time Monitoring: watchdog (Event-driven file system monitoring)

Design Patterns: Sentinel Observer, Strategy Pattern, Singleton, Event-Driven

Infrastructure: Data Gateway, Zero-Trust Validation Protocols

Quick Start
Clone the repository:

Bash
git clone [https://github.com/angeldeangelis/ETL-Data-Quality-Gateway.git](https://github.com/angeldeangelis/ETL-Data-Quality-Gateway.git)
cd ETL-Data-Quality-Gateway
Setup virtual environment:

Bash
# Create the virtual environment
python -m venv .venv

# Activate the environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate
Install dependencies:

Bash
pip install -r requirements.txt
Initialize the Environment:
Configure your database.ini with your local PostgreSQL credentials to allow the data gateway to establish the connection.

Execution:

Bash
python main.py
