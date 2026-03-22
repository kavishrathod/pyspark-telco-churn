"""
pipeline.py - PySpark Telco Churn ETL Pipeline
Entry point: Extract → Transform → Load

PySpark version of etl-telco-churn (Pandas + MySQL).
Rebuilt for scalability using Apache Spark.

Author: Kavish Rathod
"""

import os
import time

# ── Windows fix: set HADOOP_HOME programmatically ──
os.environ["HADOOP_HOME"] = os.path.dirname(os.path.abspath(__file__))
os.environ["hadoop.home.dir"] = os.environ["HADOOP_HOME"]

import time
from pyspark.sql import SparkSession
from src.extract import extract
from src.transform import transform
from src.load import load

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
INPUT_FILE  = "data/raw/telco_churn_raw.csv"
OUTPUT_DIR  = "output"


def create_spark_session():
    spark = SparkSession.builder \
        .appName("TelcoChurnETL_PySpark") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("[SPARK] ✓ SparkSession initialized\n")
    return spark


def run_pipeline():
    print("=" * 55)
    print("   PySpark ETL Pipeline — Telco Customer Churn")
    print("=" * 55)

    start = time.time()

    # ── Step 1: Spark ──
    spark = create_spark_session()

    # ── Step 2: Extract ──
    print("\n[1/3] EXTRACT PHASE")
    raw_df = extract(spark, INPUT_FILE)
    raw_df.printSchema()
    extracted_count = raw_df.count()

    # ── Step 3: Transform ──
    print("\n[2/3] TRANSFORM PHASE")
    cleaned_df, aggregations = transform(raw_df)

    # Preview key insights
    print("\n📊 Churn by Contract Type:")
    aggregations["churn_by_contract"].show()

    print("📅 Churn by Tenure Group:")
    aggregations["churn_by_tenure"].show()

    print("💳 Revenue by Payment Method:")
    aggregations["revenue_by_payment"].show()

    print("🌐 Spend by Internet Service:")
    aggregations["spend_by_internet"].show()

    # ── Step 4: Load ──
    print("\n[3/3] LOAD PHASE")
    duration = time.time() - start
    load(cleaned_df, aggregations, OUTPUT_DIR, extracted_count, duration)

    # ── Validation ──
    loaded_count = cleaned_df.count()
    status = "✅ PASSED" if loaded_count == extracted_count else "❌ FAILED"
    print(f"\n[VALIDATION] {status} — {extracted_count} rows in == {loaded_count} rows out")

    print(f"\n{'=' * 55}")
    print(f"   ✅ Pipeline Completed in {round(duration, 2)}s")
    print(f"{'=' * 55}")

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
