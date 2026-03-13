"""
Silver Layer — Cleaned & Validated Data
========================================
Transforms Bronze data into clean, deduplicated, type-enforced records.
Bad records are quarantined (not dropped) for auditing.

Transformations applied:
    - Data type casting & date parsing
    - Null handling for required fields
    - Deduplication on business keys
    - Standardization (trimming, lowercasing enums)
    - Bad record quarantine
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.helpers import get_logger, table_path

logger = get_logger("silver.transformation")


# ── Customers ──────────────────────────────────────────────────────────────


def transform_customers(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate customer data from Bronze.

    Returns:
        (clean_df, quarantine_df) — valid records and rejected records.
    """
    logger.info("Transforming customers: Bronze → Silver")

    # 1. Trim whitespace on all string columns
    for col_name in df.columns:
        if df.schema[col_name].dataType.simpleString() == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 2. Cast types
    df = df.withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"), "yyyy-MM-dd"),
    )

    # 3. Standardize segment values
    df = df.withColumn("segment", F.initcap(F.col("segment")))

    # 4. Create full_name
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )

    # 5. Flag bad records (null business key or null required fields)
    df = df.withColumn(
        "_is_valid",
        (
            F.col("customer_id").isNotNull()
            & F.col("email").isNotNull()
            & F.col("registration_date").isNotNull()
        ),
    )

    quarantine_df = df.filter(~F.col("_is_valid")).drop("_is_valid")
    clean_df = df.filter(F.col("_is_valid")).drop("_is_valid")

    # 6. Deduplicate — keep latest registration per customer_id
    window = Window.partitionBy("customer_id").orderBy(
        F.col("registration_date").desc()
    )
    clean_df = (
        clean_df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # 7. Add processing metadata
    clean_df = clean_df.withColumn(
        "_processed_timestamp", F.current_timestamp()
    )

    logger.info(
        f"Customers — clean: {clean_df.count()}, quarantined: {quarantine_df.count()}"
    )
    return clean_df, quarantine_df


# ── Products ───────────────────────────────────────────────────────────────


def transform_products(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate product data from Bronze.

    Returns:
        (clean_df, quarantine_df)
    """
    logger.info("Transforming products: Bronze → Silver")

    # 1. Trim strings
    for col_name in df.columns:
        if df.schema[col_name].dataType.simpleString() == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 2. Cast types
    df = (
        df.withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("cost_price", F.col("cost_price").cast("double"))
        .withColumn("launch_date", F.to_date(F.col("launch_date"), "yyyy-MM-dd"))
    )

    # 3. Calculate profit margin
    df = df.withColumn(
        "profit_margin",
        F.round(
            (F.col("unit_price") - F.col("cost_price")) / F.col("unit_price"),
            4,
        ),
    )

    # 4. Flag bad records
    df = df.withColumn(
        "_is_valid",
        (
            F.col("product_id").isNotNull()
            & F.col("name").isNotNull()
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") > 0)
        ),
    )

    quarantine_df = df.filter(~F.col("_is_valid")).drop("_is_valid")
    clean_df = df.filter(F.col("_is_valid")).drop("_is_valid")

    # 5. Deduplicate — keep latest version per product_id
    window = Window.partitionBy("product_id").orderBy(
        F.col("launch_date").desc()
    )
    clean_df = (
        clean_df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    clean_df = clean_df.withColumn(
        "_processed_timestamp", F.current_timestamp()
    )

    logger.info(
        f"Products — clean: {clean_df.count()}, quarantined: {quarantine_df.count()}"
    )
    return clean_df, quarantine_df


# ── Orders ─────────────────────────────────────────────────────────────────


def transform_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate order data from Bronze.

    Returns:
        (clean_df, quarantine_df)
    """
    logger.info("Transforming orders: Bronze → Silver")

    # 1. Trim strings
    for col_name in df.columns:
        if df.schema[col_name].dataType.simpleString() == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 2. Cast types
    df = (
        df.withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    )

    # 3. Calculate derived columns
    df = df.withColumn(
        "gross_revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2),
    ).withColumn(
        "net_revenue",
        F.round(
            F.col("quantity")
            * F.col("unit_price")
            * (1 - F.coalesce(F.col("discount_pct"), F.lit(0.0))),
            2,
        ),
    )

    # 4. Standardize order_status
    df = df.withColumn("order_status", F.lower(F.col("order_status")))

    # 5. Flag bad records
    df = df.withColumn(
        "_is_valid",
        (
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("order_date").isNotNull()
            & (F.col("quantity") > 0)
            & (F.col("discount_pct").between(0.0, 1.0) | F.col("discount_pct").isNull())
        ),
    )

    quarantine_df = df.filter(~F.col("_is_valid")).drop("_is_valid")
    clean_df = df.filter(F.col("_is_valid")).drop("_is_valid")

    # 6. Deduplicate on order_id
    clean_df = clean_df.dropDuplicates(["order_id"])

    clean_df = clean_df.withColumn(
        "_processed_timestamp", F.current_timestamp()
    )

    logger.info(
        f"Orders — clean: {clean_df.count()}, quarantined: {quarantine_df.count()}"
    )
    return clean_df, quarantine_df


# ── Writer ─────────────────────────────────────────────────────────────────


def write_silver_table(
    df: DataFrame,
    silver_path: str,
    table_name: str,
) -> str:
    """
    Write cleaned data to Silver layer using Delta merge (upsert).
    Falls back to overwrite if the table doesn't exist yet.
    """
    full_path = table_path(silver_path, table_name)
    logger.info(f"Writing Silver table: {full_path}")

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(full_path)

    logger.info(f"Wrote {df.count()} rows to silver.{table_name}")
    return full_path


def write_quarantine(
    df: DataFrame,
    quarantine_path: str,
    table_name: str,
) -> None:
    """Write quarantined (rejected) records for auditing."""
    if df.count() == 0:
        logger.info(f"No quarantine records for {table_name}")
        return

    full_path = table_path(quarantine_path, f"{table_name}_quarantine")
    logger.info(f"Writing {df.count()} quarantine records to {full_path}")

    df.write.format("delta").mode("append").save(full_path)


# ── Orchestrator ───────────────────────────────────────────────────────────


def run_silver_transformations(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> dict:
    """
    Execute full Silver layer transformation for all tables.

    Returns a dict of {table_name: {clean: N, quarantined: N}}.
    """
    logger.info("=" * 60)
    logger.info("SILVER LAYER TRANSFORMATION — START")
    logger.info("=" * 60)

    results = {}

    # ── Customers
    bronze_customers = spark.read.format("delta").load(
        table_path(bronze_path, "customers")
    )
    clean_cust, quarantine_cust = transform_customers(bronze_customers)
    write_silver_table(clean_cust, silver_path, "customers")
    write_quarantine(quarantine_cust, quarantine_path, "customers")
    results["customers"] = {
        "clean": clean_cust.count(),
        "quarantined": quarantine_cust.count(),
    }

    # ── Products
    bronze_products = spark.read.format("delta").load(
        table_path(bronze_path, "products")
    )
    clean_prod, quarantine_prod = transform_products(bronze_products)
    write_silver_table(clean_prod, silver_path, "products")
    write_quarantine(quarantine_prod, quarantine_path, "products")
    results["products"] = {
        "clean": clean_prod.count(),
        "quarantined": quarantine_prod.count(),
    }

    # ── Orders
    bronze_orders = spark.read.format("delta").load(
        table_path(bronze_path, "orders")
    )
    clean_ord, quarantine_ord = transform_orders(bronze_orders)
    write_silver_table(clean_ord, silver_path, "orders")
    write_quarantine(quarantine_ord, quarantine_path, "orders")
    results["orders"] = {
        "clean": clean_ord.count(),
        "quarantined": quarantine_ord.count(),
    }

    logger.info("SILVER LAYER TRANSFORMATION — COMPLETE")
    logger.info(f"Results: {results}")
    return results
