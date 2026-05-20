"""
Silver Layer Pipeline:
Reads bronze parquet tables and applies:
  - Schema casting which essentially is to correct the present data types
  - Dirty value cleaning 
  - PII removal: Name, SSN dropped from attributes
  - Credit_History_Age needs to be parsed to integer months
  - Null / bad row filtering
  - Standardizing the snapshot_date as DateType
"""

import os
import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DoubleType, DateType, StringType
)

def _strip_underscores(df: DataFrame, cols: list) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"^_+|_+$", ""))
    return df


def _cast_or_null(df: DataFrame, col_name: str, dtype):
    return df.withColumn(col_name, F.col(col_name).cast(dtype))


def _parse_credit_history_months(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "credit_history_years",
        F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s+[Yy]ear", 1).cast(IntegerType())
    )
    df = df.withColumn(
        "credit_history_months_part",
        F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s+[Mm]onth", 1).cast(IntegerType())
    )
    df = df.withColumn(
        "credit_history_age_months",
        F.col("credit_history_years") * 12 + F.col("credit_history_months_part")
    )
    df = df.drop("Credit_History_Age", "credit_history_years", "credit_history_months_part")
    return df

def _drop_metadata(df: DataFrame) -> DataFrame:
    return df.drop("ingestion_timestamp", "source_file")

def _silver_clickstream(df: DataFrame) -> DataFrame:
    df = _drop_metadata(df)
    feature_cols = [f"fe_{i}" for i in range(1, 21)]
    for c in feature_cols:
        df = _cast_or_null(df, c, IntegerType())
    df = df.withColumn("snapshot_date", F.col("snapshot_date").cast(DateType()))
    df = df.filter(F.col("Customer_ID").isNotNull())
    df = df.dropDuplicates(["Customer_ID", "snapshot_date"])
    return df

def _silver_attributes(df: DataFrame) -> DataFrame:
    df = _drop_metadata(df)
    df = df.drop("Name", "SSN")
    df = df.withColumn("Age", F.col("Age").cast(IntegerType()))
    df = df.withColumn("snapshot_date", F.col("snapshot_date").cast(DateType()))
    df = df.filter((F.col("Age") >= 18) & (F.col("Age") <= 100))
    df = df.filter(F.col("Customer_ID").isNotNull())
    df = df.dropDuplicates(["Customer_ID", "snapshot_date"])
    return df

def _silver_financials(df: DataFrame) -> DataFrame:
    df = _drop_metadata(df)
    # Columns with trailing/leading underscores (dirty numeric strings)
    dirty_numeric = [
        "Annual_Income", "Num_of_Loan", "Changed_Credit_Limit",
        "Num_of_Delayed_Payment", "Outstanding_Debt", "Amount_invested_monthly",
        "Monthly_Balance"
    ]
    df = _strip_underscores(df, dirty_numeric)

    # Set Sentinel / garbage values to null
    df = df.withColumn(
        "Credit_Mix",
        F.when(F.col("Credit_Mix") == "_", None).otherwise(F.col("Credit_Mix"))
    )
    df = df.withColumn(
        "Payment_Behaviour",
        F.when(F.col("Payment_Behaviour").rlike(r"[^a-zA-Z0-9_]"), None)
         .otherwise(F.col("Payment_Behaviour"))
    )

    # Parse credit history age to months
    df = _parse_credit_history_months(df)

    # Cast numeric columns
    double_cols = [
        "Annual_Income", "Monthly_Inhand_Salary", "Changed_Credit_Limit",
        "Outstanding_Debt", "Credit_Utilization_Ratio", "Total_EMI_per_month",
        "Amount_invested_monthly", "Monthly_Balance"
    ]
    int_cols = [
        "Num_Bank_Accounts", "Num_Credit_Card", "Interest_Rate",
        "Num_of_Loan", "Delay_from_due_date", "Num_of_Delayed_Payment",
        "Num_Credit_Inquiries"
    ]
    for c in double_cols:
        df = _cast_or_null(df, c, DoubleType())
    for c in int_cols:
        df = _cast_or_null(df, c, IntegerType())


    df = df.withColumn("snapshot_date", F.col("snapshot_date").cast(DateType()))
    df = df.filter(F.col("Customer_ID").isNotNull())
    df = df.filter(F.col("Annual_Income") > 0)
    df = df.dropDuplicates(["Customer_ID", "snapshot_date"])
    return df

def _silver_loan(df: DataFrame) -> DataFrame:
    df = _drop_metadata(df)
    df = df.withColumn("loan_start_date", F.col("loan_start_date").cast(DateType()))
    df = df.withColumn("snapshot_date",   F.col("snapshot_date").cast(DateType()))
    for c in ["tenure", "installment_num"]:
        df = _cast_or_null(df, c, IntegerType())
    for c in ["loan_amt", "due_amt", "paid_amt", "overdue_amt", "balance"]:
        df = _cast_or_null(df, c, DoubleType())
    df = df.filter(F.col("Customer_ID").isNotNull())
    df = df.filter(F.col("loan_id").isNotNull())
    return df

TABLE_MAP = {
    "bronze_feature_clickstream": ("silver_feature_clickstream", _silver_clickstream),
    "bronze_feature_attributes":  ("silver_feature_attributes",  _silver_attributes),
    "bronze_feature_financials":  ("silver_feature_financials",  _silver_financials),
    "bronze_lms_loan_daily":      ("silver_lms_loan_daily",      _silver_loan),
}

def transform_to_silver(spark: SparkSession, bronze_directory: str, silver_directory: str) -> None:
    for bronze_table, (silver_table, transform_fn) in TABLE_MAP.items():
        bronze_path = os.path.join(bronze_directory, bronze_table)
        silver_path = os.path.join(silver_directory, silver_table)
        print(f"[Silver] Processing {bronze_table} -> {silver_table}")

        df = spark.read.parquet(bronze_path)
        df = transform_fn(df)

        df.write.mode("overwrite").parquet(silver_path)
        print(f"Silver Wrote {df.count()} rows to {silver_path}")
