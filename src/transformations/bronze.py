"""
Bronze Layer — Raw Data Ingestion
==================================
Ingests raw data from source files into Delta tables with minimal transformation.
Only adds metadata columns: _ingestion_timestamp, _source_file, _batch_id.
Data is stored as-is to preserve the original source of truth.
"""

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, DateType,
)

from src.utils.helpers import get_logger, table_path

logger = get_logger("bronze.ingestion")


# ── Schema definitions for source data ────────────────────────────────────

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("shipping_country", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("launch_date", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
}


def add_metadata_columns(df: DataFrame, source_name: str) -> DataFrame:
    """
    Add standard metadata columns to track data lineage.

    Columns added:
        _ingestion_timestamp: When the data was ingested
        _source_file:         Name of the source (e.g., 'customers', 'orders')
        _batch_id:            Unique batch identifier (timestamp-based)
    """
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_name))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_csv(
    spark: SparkSession,
    file_path: str,
    source_name: str,
    schema: StructType = None,
) -> DataFrame:
    """
    Ingest a CSV file into a Bronze DataFrame.

    Reads raw CSV with schema enforcement (if provided),
    adds metadata columns, and returns the enriched DataFrame.
    """
    logger.info(f"Ingesting CSV: {file_path} (source={source_name})")

    reader = spark.read.option("header", "true").option("inferSchema", "false")
    if schema:
        reader = reader.schema(schema)

    df = reader.csv(file_path)

    row_count = df.count()
    logger.info(f"Read {row_count} rows from {source_name}")

    return add_metadata_columns(df, source_name)


def ingest_json(
    spark: SparkSession,
    file_path: str,
    source_name: str,
    schema: StructType = None,
) -> DataFrame:
    """
    Ingest a JSON file into a Bronze DataFrame.

    Reads raw JSON (supports multiLine), adds metadata columns.
    """
    logger.info(f"Ingesting JSON: {file_path} (source={source_name})")

    reader = spark.read.option("multiLine", "true")
    if schema:
        reader = reader.schema(schema)

    df = reader.json(file_path)

    row_count = df.count()
    logger.info(f"Read {row_count} rows from {source_name}")

    return add_metadata_columns(df, source_name)


def write_bronze_table(
    df: DataFrame,
    bronze_path: str,
    table_name: str,
    mode: str = "append",
) -> str:
    """
    Write a DataFrame to the Bronze layer as a Delta table.

    Uses append mode by default to preserve all historical raw data.
    Returns the full table path.
    """
    full_path = table_path(bronze_path, table_name)
    logger.info(f"Writing Bronze table: {full_path} (mode={mode})")

    (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(full_path)
    )

    row_count = df.count()
    logger.info(f"Wrote {row_count} rows to bronze.{table_name}")
    return full_path


def run_bronze_ingestion(
    spark: SparkSession,
    source_data_path: str,
    bronze_path: str,
) -> dict:
    """
    Execute full Bronze layer ingestion for all sources.

    Returns a dict of {table_name: row_count} for monitoring.
    """
    logger.info("=" * 60)
    logger.info("BRONZE LAYER INGESTION — START")
    logger.info("=" * 60)

    results = {}

    # Ingest customers (CSV)
    customers_df = ingest_csv(
        spark,
        f"{source_data_path}/customers.csv",
        "customers",
        SCHEMAS["customers"],
    )
    write_bronze_table(customers_df, bronze_path, "customers")
    results["customers"] = customers_df.count()

    # Ingest products (JSON)
    products_df = ingest_json(
        spark,
        f"{source_data_path}/products.json",
        "products",
        SCHEMAS["products"],
    )
    write_bronze_table(products_df, bronze_path, "products")
    results["products"] = products_df.count()

    # Ingest orders (CSV)
    orders_df = ingest_csv(
        spark,
        f"{source_data_path}/orders.csv",
        "orders",
        SCHEMAS["orders"],
    )
    write_bronze_table(orders_df, bronze_path, "orders")
    results["orders"] = orders_df.count()

    logger.info("BRONZE LAYER INGESTION — COMPLETE")
    logger.info(f"Results: {results}")
    return results
