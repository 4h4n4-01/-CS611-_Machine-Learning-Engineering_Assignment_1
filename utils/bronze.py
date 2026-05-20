"""
Bronze Layer Pipeline:
Ingests all the given raw CSV files as it is into the bronze zone.
Adds the metadata columns: ingestion_timestamp, source_file.
True copy of the original data, no transformations or cleaning has been done.

"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ingest_to_bronze(spark: SparkSession, data_dir: str, bronze_directory: str) -> None:

    ingestion_ts = datetime.utcnow().isoformat()

    sources = [
        ("feature_clickstream.csv",    "bronze_feature_clickstream"),
        ("features_attributes.csv",    "bronze_feature_attributes"),
        ("features_financials.csv",    "bronze_feature_financials"),
        ("lms_loan_daily.csv",         "bronze_lms_loan_daily"),
    ]

    for filename, table_name in sources:
        filepath = os.path.join(data_dir, filename)
        print(f"Bronze Ingesting {filename} -> {table_name}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")   # keeping everything as string at bronze
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(filepath)
        )

        # Adding the metadata columns
        df = (
            df
            .withColumn("ingestion_timestamp", F.lit(ingestion_ts))
            .withColumn("source_file", F.lit(filename))
        )

        output_path = os.path.join(bronze_directory, table_name)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Bronze Wrote {df.count()} rows to {output_path}")
