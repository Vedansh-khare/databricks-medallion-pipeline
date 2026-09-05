"""
Silver Layer — Clean, Conform, Join
=====================================
Principle: Silver = business-usable, quality-checked, deduplicated data. This is where cleaning rules live — and where every cleaning decision is LOGGED, not silently applied. A row disappearing without a trace is a debugging nightmare six months later when someone asks "why is revenue off."

Design decisions (see README for full rationale):
- Bad rows go to quarantine tables, not /dev/null. Reversible, auditable.
- Referential integrity enforced via inner join; join-miss counts logged as a data quality signal, not swallowed.
- Dedup strategy: keep most recently ingested version per natural key.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
QUARANTINE_DIR = SILVER_DIR / "_quarantine"


def get_spark():
    """
    Local PySpark session, Delta-enabled. See bronze/ingest_bronze.py for the same note: on real Databricks this bootstrap step is unnecessary, Delta ships with the runtime.
    """
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .appName("SilverProcessing")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def clean_customers(spark):
    """
    Dedup on customer_id, keep most recent by _ingest_timestamp. Missing email is NOT dropped — email nullability is a legitimate business state (some customers signed up via phone/in-store), just flagged.
    """
    df = spark.read.format("delta").load(str(BRONZE_DIR / "customers"))

    raw_count = df.count()

    window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_timestamp").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    cleaned = (
        deduped
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("has_email", F.col("email").isNotNull())
        .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
        .filter(F.col("customer_id").isNotNull())  # customer_id is the join key, non-negotiable
    )

    dedup_dropped = raw_count - deduped.count()
    print(f"[silver.customers] raw={raw_count} deduped_removed={dedup_dropped} final={cleaned.count()}")

    (
        cleaned.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(SILVER_DIR / "customers"))
    )
    return cleaned


def clean_products(spark):
    """Products dimension is clean by construction — light pass only: dedupe + type safety."""
    df = spark.read.format("delta").load(str(BRONZE_DIR / "products"))
    cleaned = df.dropDuplicates(["product_id"]).filter(F.col("unit_price") > 0)

    (
        cleaned.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(SILVER_DIR / "products"))
    )
    print(f"[silver.products] final={cleaned.count()}")
    return cleaned


def clean_orders(spark, customers_df, products_df):
    """
    Quarantine strategy:
    - quantity <= 0        -> data entry error, quarantine
    - customer_id is null  -> can't attribute, quarantine
    - failed join to dims  -> orphaned FK, quarantine (referential integrity break)

    Everything surviving all three checks becomes silver.orders — the single source of truth for downstream Gold aggregations.
    """
    df = spark.read.format("delta").load(str(BRONZE_DIR / "orders"))
    raw_count = df.count()

    # --- Quality gate 1: invalid quantity ---
    invalid_qty = df.filter(F.col("quantity") <= 0)
    valid_qty_df = df.filter(F.col("quantity") > 0)

    # --- Quality gate 2: null customer_id ---
    null_cust = valid_qty_df.filter(F.col("customer_id").isNull())
    valid_cust_df = valid_qty_df.filter(F.col("customer_id").isNotNull())

    # --- Quality gate 3: referential integrity via inner join ---
    customers_slim = customers_df.select("customer_id", "region", "has_email")
    products_slim = products_df.select("product_id", "category", "unit_price")

    joined = (
        valid_cust_df.alias("o")
        .join(customers_slim.alias("c"), "customer_id", "inner")
        .join(products_slim.alias("p"), "product_id", "inner")
    )

    join_miss_count = valid_cust_df.count() - joined.count()

    cleaned = (
        joined
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .select(
            "order_id", "customer_id", "product_id", "quantity",
            "unit_price", "line_total", "order_date", "order_status",
            "region", "category", "has_email"
        )
    )

    # --- Write quarantine tables (union of all rejected rows, tagged with reason) ---
    quarantine = (
        invalid_qty.withColumn("_reject_reason", F.lit("invalid_quantity"))
        .unionByName(null_cust.withColumn("_reject_reason", F.lit("null_customer_id")))
    )
    quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(QUARANTINE_DIR / "orders"))

    (
        cleaned.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_status")
        .save(str(SILVER_DIR / "orders"))
    )

    print(f"[silver.orders] raw={raw_count} invalid_qty={invalid_qty.count()} "
          f"null_customer={null_cust.count()} join_misses={join_miss_count} final={cleaned.count()}")
    print(f"[silver._quarantine.orders] quarantined={quarantine.count()} rows (reasons logged, not lost)")

    return cleaned


def main():
    spark = get_spark()
    print("=== Silver Processing ===\n")

    customers = clean_customers(spark)
    products = clean_products(spark)
    clean_orders(spark, customers, products)

    print("\n=== Silver processing complete. All rejected rows quarantined with reason codes, none silently dropped. ===")
    spark.stop()


if __name__ == "__main__":
    main()
