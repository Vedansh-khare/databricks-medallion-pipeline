"""
Gold Layer — Business Marts
=============================
Principle: Gold = pre-aggregated, business-question-answering tables. Each mart answers ONE specific business question directly — no further joins or logic should be needed downstream. If a BI tool or analyst has to write complex SQL on top of Gold, the mart is under-designed.

Three marts built here, each demonstrating a distinct Spark pattern:
1. customer_revenue_mart   — standard aggregation + ranking (RANK() window)
2. category_performance    — time-series trend (LAG() window for MoM comparison)
3. customer_cohort_mart    — first-touch attribution (MIN() window) + repeat-buyer classification

All marts read ONLY from Silver — Gold never reads Bronze directly. This enforces the layering discipline: business logic lives in exactly one place.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def get_spark():
    """Local PySpark, Delta-enabled. See bronze/silver modules for the same note on Databricks-managed Delta vs local bootstrap."""
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .appName("GoldMarts")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_customer_revenue_mart(orders_df):
    """
    Answers: "who are our highest-value customers, and how do they rank?"

    Window fn used: RANK() over total revenue, partitioned by region.
    RANK (not ROW_NUMBER) deliberately — ties should share a rank, since two customers with identical revenue are genuinely tied for business purposes, not arbitrarily ordered.
    """
    completed = orders_df.filter(F.col("order_status") == "COMPLETED")

    agg = (
        completed.groupBy("customer_id", "region")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),  # rounded: raw sum accumulates float noise (e.g. 19714.710000000003)
            F.count("order_id").alias("order_count"),
            F.round(F.avg("line_total"), 2).alias("avg_order_value"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    rank_window = Window.partitionBy("region").orderBy(F.col("total_revenue").desc())

    mart = agg.withColumn("revenue_rank_in_region", F.rank().over(rank_window))

    return mart.select(
        "customer_id", "region", "total_revenue", "order_count",
        "avg_order_value", "last_order_date", "revenue_rank_in_region"
    )


def build_category_performance_mart(orders_df):
    """
    Answers: "how is each product category trending month over month?"

    Window fn used: LAG() to pull previous month's revenue into the same row, enabling MoM % change without a self-join. This is the idiomatic Spark pattern for time-series comparison — a self-join would work but is needlessly expensive and harder to read.
    """
    completed = orders_df.filter(F.col("order_status") == "COMPLETED")

    monthly = (
        completed
        .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
        .groupBy("category", "order_month")
        .agg(
            F.round(F.sum("line_total"), 2).alias("monthly_revenue"),  # rounded: avoids float accumulation noise
            F.sum("quantity").alias("units_sold"),
        )
    )

    trend_window = Window.partitionBy("category").orderBy("order_month")

    mart = (
        monthly
        .withColumn("prev_month_revenue", F.lag("monthly_revenue").over(trend_window))
        .withColumn(
            "mom_change_pct",
            F.when(
                F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") != 0),
                F.round(
                    (F.col("monthly_revenue") - F.col("prev_month_revenue"))
                    / F.col("prev_month_revenue") * 100, 2
                )
            ).otherwise(None)  # first month per category has no prior — NULL is correct, not 0
        )
    )

    return mart.select(
        "category", "order_month", "monthly_revenue", "units_sold",
        "prev_month_revenue", "mom_change_pct"
    ).orderBy("category", "order_month")


def build_customer_cohort_mart(orders_df):
    """
    Answers: "which customers are repeat buyers vs one-time, and when did each customer first convert?"

    Window fn used: MIN() over customer_id to find first_order_date without a separate groupBy + join back — single-pass window computation. Classifies repeat_buyer as a business flag, since "how many customers come back" is one of the first questions any retail stakeholder asks.
    """
    completed = orders_df.filter(F.col("order_status") == "COMPLETED")

    cohort_window = Window.partitionBy("customer_id")

    with_first_order = completed.withColumn(
        "first_order_date", F.min("order_date").over(cohort_window)
    )

    customer_summary = (
        with_first_order.groupBy("customer_id", "region", "first_order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("line_total").alias("lifetime_value"),
        )
        .withColumn(
            "repeat_buyer",
            F.when(F.col("total_orders") > 1, True).otherwise(False)
        )
        .withColumn(
            "cohort_month", F.date_trunc("month", F.col("first_order_date"))
        )
    )

    return customer_summary.select(
        "customer_id", "region", "cohort_month", "first_order_date",
        "total_orders", "lifetime_value", "repeat_buyer"
    )


def main():
    spark = get_spark()
    print("=== Gold Mart Construction ===\n")

    orders = spark.read.format("delta").load(str(SILVER_DIR / "orders"))

    revenue_mart = build_customer_revenue_mart(orders)
    (
        revenue_mart.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_DIR / "customer_revenue_mart"))
    )
    print(f"[gold.customer_revenue_mart] rows={revenue_mart.count()}")

    category_mart = build_category_performance_mart(orders)
    (
        category_mart.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_DIR / "category_performance_mart"))
    )
    print(f"[gold.category_performance_mart] rows={category_mart.count()}")

    cohort_mart = build_customer_cohort_mart(orders)
    (
        cohort_mart.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_DIR / "customer_cohort_mart"))
    )
    print(f"[gold.customer_cohort_mart] rows={cohort_mart.count()}")

    repeat_rate = (
        cohort_mart.agg(
            F.round(F.avg(F.col("repeat_buyer").cast("int")) * 100, 1).alias("repeat_rate_pct")
        ).collect()[0]["repeat_rate_pct"]
    )
    print(f"\nOverall repeat buyer rate: {repeat_rate}%")

    print("\n=== Gold marts complete. Three business questions answered directly, zero downstream joins needed. ===")
    spark.stop()


if __name__ == "__main__":
    main()
