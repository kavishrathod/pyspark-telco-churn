"""
extract.py - Extract Phase
Reads raw Telco Churn CSV into a PySpark DataFrame
"""

def extract(spark, file_path: str):
    """
    Reads the raw telco churn CSV into a Spark DataFrame.

    Args:
        spark: SparkSession object
        file_path: Path to telco_churn_raw.csv

    Returns:
        Raw Spark DataFrame
    """
    print(f"[EXTRACT] Reading data from: {file_path}")

    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    print(f"[EXTRACT] ✓ Rows loaded     : {df.count()}")
    print(f"[EXTRACT] ✓ Columns loaded  : {len(df.columns)}")
    print(f"[EXTRACT] ✓ Columns         : {df.columns}")

    return df
