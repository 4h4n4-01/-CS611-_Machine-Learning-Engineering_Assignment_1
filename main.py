# Imports 
import os
from pyspark.sql import SparkSession

# Importing the functions for each layer of the medallion architecture 
from utils.bronze import ingest_to_bronze
from utils.silver import transform_to_silver
from utils.gold   import build_gold_tables

# Folder paths for data and the datamart 
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
DATAMART    = os.path.join(BASE_DIR, "datamart")
BRONZE_DIRECTORY  = os.path.join(DATAMART, "bronze")
SILVER_DIRECTORY  = os.path.join(DATAMART, "silver")
GOLD_DIRECTORY    = os.path.join(DATAMART, "gold")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CS611_MLE_Assignment_1_Pipeline")
        .config("spark.sql.shuffle.partitions", "8")        # lightweight for local
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main():
    print("CS611 MLE: Assignment 1 - Medallion Architecture Pipeline")

    # Creating the output directories
    for d in [BRONZE_DIRECTORY, SILVER_DIRECTORY, GOLD_DIRECTORY]:
        os.makedirs(d, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Bronze layer: Raw Ingestion
        print("\n[1/3] Bronze Layer: The raw ingestion layer")
        ingest_to_bronze(spark, DATA_DIR, BRONZE_DIRECTORY)

        # Silver layer: Cleaning and Standardisation 
        print("\n[2/3] Silver Layer: For cleaning and standardisation")
        transform_to_silver(spark, BRONZE_DIRECTORY, SILVER_DIRECTORY)

        # Gold layer: Feature Store and Label Store
        print("\n[3/3] Gold Layer: Containing Feature Store and Label Store")
        build_gold_tables(spark, SILVER_DIRECTORY, GOLD_DIRECTORY)

        print("Pipeline complete. Datamart has been written to:", DATAMART)
    

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
