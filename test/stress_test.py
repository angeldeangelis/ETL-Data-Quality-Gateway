"""
generate_stress_data.py
High-volume synthetic data generator for ETL stress testing.
"""

import os
import uuid
import random
import pandas as pd
from pathlib import Path

def create_heavy_payload():
    print("🚀 Fabricating massive test payload...")

    # 1. Configure volume (100,000 unique transactions)
    total_records = 100000
    products = [
        "Cloud Gateway Pro", "Data Pipeline Ingestor",
        "Enterprise Security Shield", "AI Analytics Node",
        "Quantum Ledger Core"
    ]

    # Generate clean synthetic data
    print("⚙️ Generating 100,000 unique records...")
    base_data = {
        "transaction_id": [f"TXN-{uuid.uuid4().hex[:12]}".upper() for _ in range(total_records)],
        "product_name": [random.choice(products) for _ in range(total_records)],
        "revenue": [round(random.uniform(49.99, 7500.50), 2) for _ in range(total_records)]
    }

    df = pd.DataFrame(base_data)

    # 2. Inject collisions (5,000 duplicates to test UPSERT logic)
    print("✨ Injecting 5,000 key collisions (existing keys, altered amounts)...")
    duplicates = df.head(5000).copy()
    # Deliberately modify revenue to track it in PostgreSQL
    duplicates["revenue"] = 99999.99

    # Concatenate base file with malicious duplicates
    final_df = pd.concat([df, duplicates], ignore_index=True)

    # Shuffle the dataframe so duplicates are scattered
    print("🔀 Shuffling the dataframe...")
    final_df = final_df.sample(frac=1).reset_index(drop=True)

    # 3. Target Drop Zone
    # Saves into the 'raw_data' folder as specified in your architecture
    target_path = Path(__file__).parent.parent / "raw_data" / "heavy_stress_payload.csv"

    # Ensure directory exists
    target_path.parent.mkdir(exist_ok=True)

    # Write to disk
    final_df.to_csv(target_path, index=False)
    print(f"✅ Success! File generated with {len(final_df)} rows at: {target_path}")

if __name__ == "__main__":
    create_heavy_payload()