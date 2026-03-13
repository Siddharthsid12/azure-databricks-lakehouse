"""
Gold Layer — Star Schema with SCD Type 2
==========================================
Orchestrates the construction of the dimensional model:
    1. Build dimension tables with SCD Type 2 tracking
    2. Generate date dimension
    3. Build fact table with resolved surrogate keys
    4. Write all Gold tables as Delta
"""

from pyspark.sql import SparkSession

from src.models.star_schema import (
    build_dim_customer,
    build_dim_product,
    build_dim_date,
    build_fact_orders,
)
from src.models.scd_type2 import apply_scd_type2
from src.utils.helpers import get_logger, table_path

logger = get_logger("gold.star_schema")

# Columns tracked for SCD Type 2 change detection
CUSTOMER_TRACKED_COLS = [
    "customer_name", "email", "phone", "customer_segment",
    "region", "city", "country",
]

PRODUCT_TRACKED_COLS = [
    "product_name", "product_category", "product_subcategory",
    "brand", "list_price", "cost_price", "is_active",
]


def write_gold_table(df, gold_path: str, table_name: str) -> str:
    """Write a DataFrame to the Gold layer as Delta."""
    full_path = table_path(gold_path, table_name)
    logger.info(f"Writing Gold table: {full_path}")

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(full_path)

    logger.info(f"Wrote {df.count()} rows to gold.{table_name}")
    return full_path


def run_gold_layer(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> dict:
    """
    Execute full Gold layer build: dimensions + fact table.

    Returns a dict of {table_name: row_count}.
    """
    logger.info("=" * 60)
    logger.info("GOLD LAYER — STAR SCHEMA BUILD — START")
    logger.info("=" * 60)

    results = {}

    # ── Read Silver tables ─────────────────────────────────────────
    silver_customers = spark.read.format("delta").load(
        table_path(silver_path, "customers")
    )
    silver_products = spark.read.format("delta").load(
        table_path(silver_path, "products")
    )
    silver_orders = spark.read.format("delta").load(
        table_path(silver_path, "orders")
    )

    # ── dim_date ───────────────────────────────────────────────────
    dim_date = build_dim_date(spark, "2023-01-01", "2025-12-31")
    write_gold_table(dim_date, gold_path, "dim_date")
    results["dim_date"] = dim_date.count()

    # ── dim_customer (SCD Type 2) ──────────────────────────────────
    dim_customer_staging = build_dim_customer(silver_customers)
    dim_customer = apply_scd_type2(
        spark=spark,
        source_df=dim_customer_staging,
        target_path=table_path(gold_path, "dim_customer"),
        business_key="customer_id",
        tracked_columns=CUSTOMER_TRACKED_COLS,
        surrogate_key="customer_key",
    )
    write_gold_table(dim_customer, gold_path, "dim_customer")
    results["dim_customer"] = dim_customer.count()

    # ── dim_product (SCD Type 2) ───────────────────────────────────
    dim_product_staging = build_dim_product(silver_products)
    dim_product = apply_scd_type2(
        spark=spark,
        source_df=dim_product_staging,
        target_path=table_path(gold_path, "dim_product"),
        business_key="product_id",
        tracked_columns=PRODUCT_TRACKED_COLS,
        surrogate_key="product_key",
    )
    write_gold_table(dim_product, gold_path, "dim_product")
    results["dim_product"] = dim_product.count()

    # ── fact_orders ────────────────────────────────────────────────
    # Reload dimensions from Gold (now with surrogate keys)
    dim_customer_gold = spark.read.format("delta").load(
        table_path(gold_path, "dim_customer")
    )
    dim_product_gold = spark.read.format("delta").load(
        table_path(gold_path, "dim_product")
    )

    fact_orders = build_fact_orders(
        silver_orders, dim_customer_gold, dim_product_gold
    )
    write_gold_table(fact_orders, gold_path, "fact_orders")
    results["fact_orders"] = fact_orders.count()

    logger.info("GOLD LAYER — STAR SCHEMA BUILD — COMPLETE")
    logger.info(f"Results: {results}")
    return results
