"""
Gold Layer Pipeline:
Transforms the cleaned silver tables into the final gold tables that shall be further used for ML model training. 
1. gold_label_store
2. gold_feature_store

"""

import os
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# Building the label store 
def build_label_store(spark: SparkSession, silver_directory: str, gold_directory: str) -> DataFrame:
    loan_df = spark.read.parquet(os.path.join(silver_directory, "silver_lms_loan_daily"))

    # Getting the final installment row for each loan
    window = Window.partitionBy("loan_id").orderBy(F.col("installment_num").desc())
    final_installments = (
        loan_df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    label_df = (
        final_installments
        .withColumn(
            "is_default",
            F.when(F.col("overdue_amt") > 0, F.lit(1)).otherwise(F.lit(0))
        )
        .select(
            "loan_id",
            "Customer_ID",
            "loan_start_date",
            "tenure",
            "loan_amt",
            "is_default"
        )
    )

    output_path = os.path.join(gold_directory, "gold_label_store")
    label_df.write.mode("overwrite").parquet(output_path)
    total   = label_df.count()
    default = label_df.filter(F.col("is_default") == 1).count()
    print(f"Gold Label store: {total} loans, {default} defaults ({100*default//total}%)")
    return label_df

def _latest_snapshot_before(df: DataFrame, date_col: str, key: str) -> DataFrame:
    window = Window.partitionBy(key, "loan_start_date").orderBy(F.col("snapshot_date").desc())
    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn", "snapshot_date")
    )


def _build_attributes_features(spark: SparkSession, silver_directory: str, loan_starts: DataFrame) -> DataFrame:
    attr = spark.read.parquet(os.path.join(silver_directory, "silver_feature_attributes"))

    # Cross joining with loan_start_date to filter only the pre-loan snapshots
    attr_with_loan = attr.join(
        loan_starts.select("Customer_ID", "loan_start_date"),
        on="Customer_ID",
        how="inner"
    ).filter(F.col("snapshot_date") <= F.col("loan_start_date"))

    return _latest_snapshot_before(attr_with_loan, "snapshot_date", "Customer_ID")


def _build_financials_features(spark: SparkSession, silver_directory: str, loan_starts: DataFrame) -> DataFrame:
    fin = spark.read.parquet(os.path.join(silver_directory, "silver_feature_financials"))

    fin_with_loan = fin.join(
        loan_starts.select("Customer_ID", "loan_start_date"),
        on="Customer_ID",
        how="inner"
    ).filter(F.col("snapshot_date") <= F.col("loan_start_date"))

    fin_latest = _latest_snapshot_before(fin_with_loan, "snapshot_date", "Customer_ID")

    # Dropping free-text column as it is not important for ML
    fin_latest = fin_latest.drop("Type_of_Loan", "Payment_of_Min_Amount")

    # One-hot encoding of Credit_Mix and Payment_Behaviour 
    for val, alias in [("Good", "credit_mix_good"), ("Standard", "credit_mix_standard"), ("Bad", "credit_mix_bad")]:
        fin_latest = fin_latest.withColumn(alias, (F.col("Credit_Mix") == val).cast(IntegerType()))

    for val, alias in [
        ("High_spent_Large_value_payments",  "pb_high_large"),
        ("High_spent_Medium_value_payments", "pb_high_medium"),
        ("High_spent_Small_value_payments",  "pb_high_small"),
        ("Low_spent_Large_value_payments",   "pb_low_large"),
        ("Low_spent_Medium_value_payments",  "pb_low_medium"),
        ("Low_spent_Small_value_payments",   "pb_low_small"),
    ]:
        fin_latest = fin_latest.withColumn(alias, (F.col("Payment_Behaviour") == val).cast(IntegerType()))

    fin_latest = fin_latest.drop("Credit_Mix", "Payment_Behaviour")
    return fin_latest


def _build_clickstream_features(spark: SparkSession, silver_directory: str, loan_starts: DataFrame) -> DataFrame:
    cs = spark.read.parquet(os.path.join(silver_directory, "silver_feature_clickstream"))
    feature_cols = [f"fe_{i}" for i in range(1, 21)]

    cs_with_loan = cs.join(
        loan_starts.select("Customer_ID", "loan_start_date"),
        on="Customer_ID",
        how="inner"
    ).filter(
        (F.col("snapshot_date") <= F.col("loan_start_date")) &
        (F.col("snapshot_date") >= F.add_months(F.col("loan_start_date"), -6))
    )

    agg_exprs = []
    for c in feature_cols:
        agg_exprs.append(F.mean(c).alias(f"{c}_mean"))
        agg_exprs.append(F.stddev(c).alias(f"{c}_std"))

    cs_agg = (
        cs_with_loan
        .groupBy("Customer_ID", "loan_start_date")
        .agg(*agg_exprs)
    )
    return cs_agg

# Building the feature store 
def build_feature_store(spark: SparkSession, silver_directory: str, gold_directory: str,
                        label_df: DataFrame) -> None:
    # Loan application reference dates (one per loan to ensure no leakage)
    loan_starts = label_df.select("Customer_ID", "loan_start_date", "loan_id").distinct()

    print("[Gold] Building attribute features ...")
    attr_feat  = _build_attributes_features(spark, silver_directory, loan_starts)

    print("[Gold] Building financial features ...")
    fin_feat   = _build_financials_features(spark, silver_directory, loan_starts)

    print("[Gold] Building clickstream features ...")
    cs_feat    = _build_clickstream_features(spark, silver_directory, loan_starts)

    # Joining everything on Customer_ID and loan_start_date
    feature_df = (
        loan_starts
        .join(attr_feat, on=["Customer_ID", "loan_start_date"], how="left")
        .join(fin_feat,  on=["Customer_ID", "loan_start_date"], how="left")
        .join(cs_feat,   on=["Customer_ID", "loan_start_date"], how="left")
    )

    output_path = os.path.join(gold_directory, "gold_feature_store")
    feature_df.write.mode("overwrite").parquet(output_path)
    print(f"Gold Feature store: {feature_df.count()} rows, {len(feature_df.columns)} columns -> {output_path}")


def build_gold_tables(spark: SparkSession, silver_directory: str, gold_directory: str) -> None:
    label_df = build_label_store(spark, silver_directory, gold_directory)
    build_feature_store(spark, silver_directory, gold_directory, label_df)
