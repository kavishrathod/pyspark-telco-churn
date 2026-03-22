# ⚡ PySpark ETL Pipeline — Telco Customer Churn

A scalable **ETL pipeline built with Apache PySpark** to process **7,043 telecom customer records** for churn analysis.

> 🔗 **v1 (Pandas + MySQL):** [etl-telco-churn](https://github.com/kavishrathod/etl-telco-churn)  
> ⚡ **v2 (PySpark + Parquet):** This repository — rebuilt for big data scalability

---

## 📌 Project Overview

This pipeline demonstrates processing the same Telco Churn dataset at scale using **distributed computing** with Apache Spark — a real-world upgrade from a Pandas-based pipeline.

**What it does:**
- **Extract** — Reads 7,043 raw telco records into Spark DataFrames
- **Transform** — Cleans data, encodes features, engineers new columns, and generates 4 business insights
- **Load** — Writes outputs to **Parquet** (big data standard) and **CSV** with an audit log

---

## 🗂️ Project Structure

```
pyspark-telco-churn/
├── data/
│   └── raw/
│       └── telco_churn_raw.csv        # Raw input (7,043 records, 21 columns)
├── src/
│   ├── extract.py                     # E — Read CSV into Spark DataFrame
│   ├── transform.py                   # T — Clean, encode, engineer, aggregate
│   └── load.py                        # L — Write to Parquet + CSV + audit log
├── output/
│   ├── csv/                           # CSV outputs (human-readable, pushed to GitHub)
│   │   ├── telco_churn_cleaned/       # Full cleaned dataset
│   │   ├── churn_by_contract/
│   │   ├── churn_by_tenure/
│   │   ├── revenue_by_payment/
│   │   └── spend_by_internet/
│   ├── parquet/                       # Parquet outputs (generated locally, not in repo)
│   └── logs/
│       └── load_log.json              # Pipeline audit log
├── pipeline.py                        # Main orchestrator
├── requirements.txt
└── README.md
```

---

## ⚙️ Pipeline Overview

```
[Raw CSV: 7,043 records] → EXTRACT → TRANSFORM → LOAD → [Parquet + CSV]
```

### 1. Extract (`src/extract.py`)
- Reads CSV with `spark.read.csv()` and automatic schema inference
- Logs row and column counts

### 2. Transform (`src/transform.py`)

**Cleaning:**
- Drops `customerID` (not needed for analysis)
- Fixes `TotalCharges` blanks — casts to Double (nulls for blanks), imputes with **median via `approxQuantile`**
- Standardizes `gender` formatting with `F.initcap()`

**Binary Encoding:**
- Converts `Yes/No` → `1/0` for: `Partner`, `Dependents`, `PhoneService`, `PaperlessBilling`, `Churn`

**Feature Engineering:**
- `tenure_group` — buckets customers: `0-1 Year`, `1-2 Years`, `2-4 Years`, `4+ Years`
- `avg_monthly_spend` — `TotalCharges / tenure` (null-safe)

**Aggregations Generated:**

| Insight | Description |
|---|---|
| `churn_by_contract` | Churn rate by contract type |
| `churn_by_tenure` | Churn rate by tenure group |
| `revenue_by_payment` | Total revenue by payment method |
| `spend_by_internet` | Avg spend & churn by internet service |

### 3. Load (`src/load.py`)
- Saves cleaned data + all aggregations to **Parquet** and **CSV**
- Generates a **JSON audit log** with timestamp, row counts, and duration
- **Row-count validation** ensures no data loss

---

## 📊 Key Insights from Pipeline Run

| Metric | Value |
|---|---|
| Total Records | 7,043 |
| Churn Rate | ~26% |
| TotalCharges blanks fixed | 11 records |
| Features engineered | 2 (`tenure_group`, `avg_monthly_spend`) |
| Aggregations generated | 4 |

### Churn by Contract Type
| Contract | Customers | Churned | Churn Rate |
|---|---|---|---|
| Month-to-month | 3,875 | 1,655 | **42.71%** |
| One year | 1,473 | 166 | 11.27% |
| Two year | 1,695 | 48 | 2.83% |

### Churn by Tenure Group
| Tenure Group | Customers | Churned | Churn Rate |
|---|---|---|---|
| 0-1 Year | 2,186 | 1,037 | **47.44%** |
| 1-2 Years | 1,024 | 294 | 28.71% |
| 2-4 Years | 1,594 | 325 | 20.39% |
| 4+ Years | 2,239 | 213 | 9.51% |

### Revenue by Payment Method
| Payment Method | Total Revenue | Avg Monthly | Customers |
|---|---|---|---|
| Electronic check | ₹49,44,903 | ₹76.26 | 2,365 |
| Bank transfer | ₹47,51,069 | ₹67.19 | 1,544 |
| Credit card | ₹46,72,988 | ₹66.51 | 1,522 |
| Mailed check | ₹17,02,549 | ₹43.92 | 1,612 |

### Spend by Internet Service
| Internet Service | Avg Spend | Customers | Churn Rate |
|---|---|---|---|
| Fiber optic | ₹91.46 | 3,096 | **41.89%** |
| DSL | ₹58.12 | 2,421 | 18.96% |
| No internet | ₹21.13 | 1,526 | 7.40% |

---

## 🚀 How to Run

### 1. Prerequisites
```bash
# Check Java (required for Spark)
java -version   # Need Java 11

# Install Java 11 if needed
# Windows  → https://adoptium.net/temurin/releases/?version=11 (download .msi)
# Linux    → sudo apt install openjdk-11-jdk
# macOS    → brew install openjdk@11
```

**Windows only — additional setup required:**
1. Download `winutils.exe` and `hadoop.dll` from [cdarlint/winutils](https://github.com/cdarlint/winutils) → `hadoop-3.3.5/bin/`
2. Place both files in `C:\hadoop\bin\`
3. Set environment variable: `HADOOP_HOME = C:\hadoop`
4. Add `C:\hadoop\bin` to your system `PATH`

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
```bash
python pipeline.py
```

### Expected Output
```
[SPARK] ✓ SparkSession initialized

[1/3] EXTRACT PHASE
[EXTRACT] ✓ Rows loaded     : 7043
[EXTRACT] ✓ Columns loaded  : 21

[2/3] TRANSFORM PHASE
[TRANSFORM] Step 1: Cleaning data...
[TRANSFORM] Step 2: Encoding binary Yes/No columns...
[TRANSFORM] Step 3: Feature engineering...
[TRANSFORM] Step 4: Generating aggregations...
[TRANSFORM] ✓ Final shape: 7043 rows × 22 columns

[3/3] LOAD PHASE
[LOAD] ✓ All outputs saved

[VALIDATION] ✅ PASSED — 7043 rows in == 7043 rows out
✅ Pipeline Completed in ~12s
```

---

## 🧰 Tech Stack

| Tool | Purpose |
|---|---|
| PySpark 3.5 | Distributed data processing |
| Spark SQL Functions | Transformations & aggregations |
| `approxQuantile` | Scalable median imputation |
| Parquet | Columnar storage (industry standard) |
| Python 3.10+ | Orchestration |

---

## 💡 Key PySpark Concepts Demonstrated

- `SparkSession` with local multi-core mode
- `spark.read.csv()` with schema inference
- `withColumn`, `cast`, `fillna`, `drop`, `filter`
- `F.when().when().otherwise()` for bucketing
- `approxQuantile` for distributed median computation
- `groupBy`, `agg`, `sum`, `mean`, `count`
- `write.parquet()` and `write.csv()` with `coalesce(1)`

---

## 🔗 Related Projects

- **v1 — Pandas + MySQL:** [etl-telco-churn](https://github.com/kavishrathod/etl-telco-churn) — Same pipeline built with Pandas and loaded into MySQL

---

## 👤 Author

**Kavish Rathod**  
BE Graduate — Electronics & Telecommunication, VCET Mumbai  
[LinkedIn](https://linkedin.com/in/kavishrathod) • [GitHub](https://github.com/kavishrathod)
