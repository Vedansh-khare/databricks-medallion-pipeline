"""
Bronze Layer — Raw Ingestion
=============================
Bronze holds raw data, schema-on-read, no business logic, no cleaning.The only additions are ingestion metadata (source file, ingest timestamp, batch id) for lineage. That's the audit trail — source values never get mutated here.

Design decisions:
- Schemas are explicit, not inferred. Inferred schema on JSON/CSV is a common production bug source (e.g. quantity inferred as string because one row had a stray value).
- Writes are append-only — bronze is immutable history, never overwritten.
- Partitioned by ingestion_date for efficient downstream incremental reads.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from delta import configure_spark_with_delta_pip
from pathlib import Path
import uuid

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

BATCH_ID = str(uuid.uuid4())

# ---- Explicit schemas (schema-on-read, no inference) ----

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True),  # kept as string in bronze; cast in silver
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_status", StringType(), True),
])


def get_spark():
    """
    NOTE: This runs local PySpark, not a real Databricks cluster. On actual Databricks — including the free edition — Delta ships preconfigured on the runtime, so the configure_spark_with_delta_pip step below is only needed for local/open-source Spark. Worth keeping straight in interviews: this repo runs the OSS Delta Lake stack locally, architecturally identical to Databricks' managed Delta, but not literally executing on Databricks compute.
    """
    builder = (
        SparkSession.builder
        .appName("BronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def add_ingestion_metadata(df):
    """Every bronze table gets the same lineage columns. Non-negotiable for audit."""
    return (
        df.withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_ingest_date", F.current_date())
          .withColumn("_batch_id", F.lit(BATCH_ID))
          .withColumn("_source_system", F.lit("synthetic_ecommerce"))
    )


def ingest_customers(spark):
    df = (
        spark.read
        .schema(CUSTOMERS_SCHEMA)
        .option("header", True)
        .csv(str(RAW_DIR / "customers.csv"))
    )
    df = add_ingestion_metadata(df).withColumn("_source_file", F.lit("customers.csv"))

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(str(BRONZE_DIR / "customers"))
    )
    print(f"[bronze.customers] ingested {df.count()} rows | batch={BATCH_ID}")
    return df


def ingest_products(spark):
    df = (
        spark.read
        .schema(PRODUCTS_SCHEMA)
        .option("header", True)
        .csv(str(RAW_DIR / "products.csv"))
    )
    df = add_ingestion_metadata(df).withColumn("_source_file", F.lit("products.csv"))

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(str(BRONZE_DIR / "products"))
    )
    print(f"[bronze.products] ingested {df.count()} rows | batch={BATCH_ID}")
    return df


def ingest_orders(spark):
    # JSONL source — simulates event-stream-style ingestion, distinct from the dimension CSV extracts. Deliberate to show handling of mixed source formats.
    df = (
        spark.read
        .schema(ORDERS_SCHEMA)
        .json(str(RAW_DIR / "orders.jsonl"))
    )
    df = add_ingestion_metadata(df).withColumn("_source_file", F.lit("orders.jsonl"))

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(str(BRONZE_DIR / "orders"))
    )
    print(f"[bronze.orders] ingested {df.count()} rows | batch={BATCH_ID}")
    return df


def main():
    spark = get_spark()
    print(f"=== Bronze Ingestion | batch_id={BATCH_ID} ===\n")

    ingest_customers(spark)
    ingest_products(spark)
    ingest_orders(spark)

    print("\n=== Bronze ingestion complete. No business logic applied — raw fidelity preserved. ===")
    spark.stop()


if __name__ == "__main__":
    main()
