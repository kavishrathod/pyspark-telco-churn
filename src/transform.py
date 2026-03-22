"""
transform.py - Transform Phase
Mirrors the exact logic from etl-telco-churn (Pandas),
rebuilt using PySpark DataFrames for scalability.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def clean_data(df):
    """
    Step 1: Data Cleaning
    - Drops customerID (not needed for analysis)
    - Fixes TotalCharges blanks → cast to Double (nulls for blanks)
    - Imputes TotalCharges nulls with median
    - Standardizes gender column formatting
    """
    print("[TRANSFORM] Step 1: Cleaning data...")

    # Drop customerID — not needed for analysis
    df = df.drop("customerID")

    # Fix TotalCharges — blank strings become null when cast to Double
    df = df.withColumn(
        "TotalCharges",
        F.col("TotalCharges").cast(DoubleType())
    )

    # Impute TotalCharges nulls with median
    median_val = df.approxQuantile("TotalCharges", [0.5], 0.001)[0]
    df = df.fillna({"TotalCharges": median_val})

    # Standardize gender: capitalize first letter
    df = df.withColumn("gender", F.initcap(F.col("gender")))

    null_count = df.filter(F.col("TotalCharges").isNull()).count()
    print(f"[TRANSFORM] ✓ TotalCharges nulls after imputation: {null_count}")

    return df


def encode_binary_columns(df):
    """
    Step 2: Binary Encoding
    Converts Yes/No columns to 1/0
    Matches exact logic from etl-telco-churn transform.py
    """
    print("[TRANSFORM] Step 2: Encoding binary Yes/No columns...")

    yes_no_cols = [
        "Partner", "Dependents", "PhoneService",
        "PaperlessBilling", "Churn"
    ]

    for col in yes_no_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "Yes", 1).otherwise(0)
        )

    print(f"[TRANSFORM] ✓ Encoded columns: {yes_no_cols}")
    return df


def feature_engineering(df):
    """
    Step 3: Feature Engineering
    - tenure_group: buckets customers by tenure
    - avg_monthly_spend: TotalCharges / tenure (safe division)
    Mirrors etl-telco-churn feature engineering exactly.
    """
    print("[TRANSFORM] Step 3: Feature engineering...")

    # tenure_group bucketing
    df = df.withColumn(
        "tenure_group",
        F.when(F.col("tenure") <= 12, "0-1 Year")
         .when((F.col("tenure") > 12) & (F.col("tenure") <= 24), "1-2 Years")
         .when((F.col("tenure") > 24) & (F.col("tenure") <= 48), "2-4 Years")
         .otherwise("4+ Years")
    )

    # avg_monthly_spend — avoid divide by zero for tenure = 0
    df = df.withColumn(
        "avg_monthly_spend",
        F.when(
            F.col("tenure") > 0,
            F.round(F.col("TotalCharges") / F.col("tenure"), 2)
        ).otherwise(F.col("MonthlyCharges"))
    )

    print("[TRANSFORM] ✓ Added: tenure_group, avg_monthly_spend")
    return df


def aggregate_insights(df):
    """
    Step 4: Aggregations for analysis
    Returns a dict of insight DataFrames
    """
    print("[TRANSFORM] Step 4: Generating aggregations...")

    # Churn rate by Contract type
    churn_by_contract = (
        df.groupBy("Contract")
        .agg(
            F.count("Churn").alias("total_customers"),
            F.sum("Churn").alias("churned"),
            F.round(F.mean("Churn") * 100, 2).alias("churn_rate_%")
        )
        .orderBy(F.desc("churn_rate_%"))
    )

    # Churn rate by tenure group
    churn_by_tenure = (
        df.groupBy("tenure_group")
        .agg(
            F.count("Churn").alias("total_customers"),
            F.sum("Churn").alias("churned"),
            F.round(F.mean("Churn") * 100, 2).alias("churn_rate_%")
        )
        .orderBy(F.desc("churn_rate_%"))
    )

    # Revenue by Payment Method
    revenue_by_payment = (
        df.groupBy("PaymentMethod")
        .agg(
            F.round(F.sum("TotalCharges"), 2).alias("total_revenue"),
            F.round(F.avg("MonthlyCharges"), 2).alias("avg_monthly_charges"),
            F.count("*").alias("customers")
        )
        .orderBy(F.desc("total_revenue"))
    )

    # Avg monthly spend by Internet Service
    spend_by_internet = (
        df.groupBy("InternetService")
        .agg(
            F.round(F.avg("avg_monthly_spend"), 2).alias("avg_spend"),
            F.count("*").alias("customers"),
            F.round(F.mean("Churn") * 100, 2).alias("churn_rate_%")
        )
        .orderBy(F.desc("avg_spend"))
    )

    print("[TRANSFORM] ✓ Aggregations complete")

    return {
        "churn_by_contract": churn_by_contract,
        "churn_by_tenure":   churn_by_tenure,
        "revenue_by_payment": revenue_by_payment,
        "spend_by_internet":  spend_by_internet,
    }


def transform(df):
    """
    Main transform function — runs all steps in order.
    Returns (cleaned_df, aggregations_dict)
    """
    df = clean_data(df)
    df = encode_binary_columns(df)
    df = feature_engineering(df)
    aggs = aggregate_insights(df)
    print(f"[TRANSFORM] ✓ Final shape: {df.count()} rows × {len(df.columns)} columns")
    return df, aggs
