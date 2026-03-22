"""
load.py - Load Phase
Saves cleaned DataFrame and aggregations to Parquet + CSV.
PySpark version uses Parquet (industry standard) instead of MySQL.
"""

import os
import json
from datetime import datetime


def load_to_parquet(df, output_path: str, name: str):
    path = os.path.join(output_path, "parquet", name)
    print(f"[LOAD] Saving '{name}' → Parquet: {path}")
    df.write.mode("overwrite").parquet(path)
    print(f"[LOAD] ✓ '{name}' saved as Parquet")


def load_to_csv(df, output_path: str, name: str):
    path = os.path.join(output_path, "csv", name)
    print(f"[LOAD] Saving '{name}' → CSV: {path}")
    df.coalesce(1).write.mode("overwrite").csv(path, header=True)
    print(f"[LOAD] ✓ '{name}' saved as CSV")


def save_audit_log(output_path: str, row_count: int, agg_names: list, duration: float):
    """Saves a JSON audit log — mirrors etl-telco-churn load_log.json"""
    log = {
        "pipeline": "pyspark-telco-churn",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows_loaded": row_count,
        "aggregations_saved": agg_names,
        "duration_seconds": round(duration, 2),
        "storage_format": ["Parquet", "CSV"]
    }
    log_path = os.path.join(output_path, "logs", "load_log.json")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as f:
        json.dump(log, f, indent=2)
    print(f"[LOAD] ✓ Audit log saved: {log_path}")


def load(cleaned_df, aggregations: dict, output_path: str, row_count: int, duration: float):
    """
    Main load function.
    Saves cleaned data + all aggregations to Parquet and CSV.
    """
    print("[LOAD] Starting load phase...")

    os.makedirs(output_path, exist_ok=True)

    # Save cleaned full dataset
    load_to_parquet(cleaned_df, output_path, "telco_churn_cleaned")
    load_to_csv(cleaned_df, output_path, "telco_churn_cleaned")

    # Save all aggregations
    for name, df in aggregations.items():
        load_to_parquet(df, output_path, name)
        load_to_csv(df, output_path, name)

    # Save audit log
    save_audit_log(output_path, row_count, list(aggregations.keys()), duration)

    print(f"[LOAD] ✓ All outputs saved to: {output_path}")
