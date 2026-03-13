"""
Star Schema — Dimensional Model Definitions
=============================================
Defines the Gold layer dimensional model:
    - dim_customer  (SCD Type 2)
    - dim_product   (SCD Type 2)
    - dim_date      (Generated calendar)
    - fact_orders   (Transactional grain)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src.utils.helpers import get_logger

logger = get_logger("models.star_schema")


def build_dim_date(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """
    Generate a date dimension table covering the specified range.

    Includes common calendar attributes used in analytics:
    year, quarter, month, week, day of week, is_weekend, fiscal period.
    """
    logger.info(f"Building dim_date: {start_date} to {end_date}")

    # Generate date sequence
    date_df = (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), "
            f"to_date('{end_date}'), interval 1 day)) AS full_date"
        )
    )

    dim_date = (
        date_df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False),
        )
        .withColumn(
            "fiscal_year",
            F.when(F.month("full_date") >= 7, F.year("full_date") + 1)
            .otherwise(F.year("full_date")),
        )
        .withColumn(
            "fiscal_quarter",
            F.when(F.month("full_date").between(7, 9), 1)
            .when(F.month("full_date").between(10, 12), 2)
            .when(F.month("full_date").between(1, 3), 3)
            .otherwise(4),
        )
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    )

    logger.info(f"dim_date: {dim_date.count()} rows generated")
    return dim_date


def build_dim_customer(silver_customers: DataFrame) -> DataFrame:
    """
    Build customer dimension from Silver layer.

    Selects and renames columns to follow dimensional modeling conventions.
    SCD Type 2 columns are applied separately via scd_type2 module.
    """
    logger.info("Building dim_customer from Silver")

    return (
        silver_customers
        .select(
            F.col("customer_id"),
            F.col("full_name").alias("customer_name"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("phone"),
            F.col("segment").alias("customer_segment"),
            F.col("region"),
            F.col("city"),
            F.col("country"),
            F.col("registration_date"),
        )
    )


def build_dim_product(silver_products: DataFrame) -> DataFrame:
    """
    Build product dimension from Silver layer.

    Selects and renames columns to follow dimensional modeling conventions.
    SCD Type 2 columns are applied separately via scd_type2 module.
    """
    logger.info("Building dim_product from Silver")

    return (
        silver_products
        .select(
            F.col("product_id"),
            F.col("name").alias("product_name"),
            F.col("category").alias("product_category"),
            F.col("subcategory").alias("product_subcategory"),
            F.col("brand"),
            F.col("unit_price").alias("list_price"),
            F.col("cost_price"),
            F.col("profit_margin"),
            F.col("launch_date"),
            F.col("is_active"),
        )
    )


def build_fact_orders(
    silver_orders: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """
    Build the fact_orders table by joining Silver orders with dimension keys.

    Resolves surrogate keys from dim_customer and dim_product,
    and adds the date_key for joining with dim_date.
    """
    logger.info("Building fact_orders from Silver + Dimensions")

    # Get current dimension records only (for foreign key resolution)
    current_customers = dim_customer.filter(F.col("is_current") == True).select(
        F.col("customer_key"), F.col("customer_id")
    )
    current_products = dim_product.filter(F.col("is_current") == True).select(
        F.col("product_key"), F.col("product_id")
    )

    fact = (
        silver_orders
        # Join customer dimension for surrogate key
        .join(current_customers, on="customer_id", how="left")
        # Join product dimension for surrogate key
        .join(current_products, on="product_id", how="left")
        # Create date key
        .withColumn(
            "date_key",
            F.date_format("order_date", "yyyyMMdd").cast("integer"),
        )
        # Generate fact surrogate key
        .withColumn("order_key", F.monotonically_increasing_id() + 1)
        # Select final columns
        .select(
            "order_key",
            "order_id",
            "date_key",
            "customer_key",
            "product_key",
            "order_date",
            "quantity",
            "unit_price",
            "discount_pct",
            "gross_revenue",
            "net_revenue",
            "payment_method",
            "order_status",
            "shipping_country",
        )
    )

    logger.info(f"fact_orders: {fact.count()} rows built")
    return fact
