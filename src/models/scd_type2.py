"""
Slowly Changing Dimension Type 2 — Implementation
===================================================
Handles historical tracking for dimension tables by maintaining
versioned records with effective date ranges.

When a source record changes:
    1. The current record's scd_end_date is set to today
    2. The current record's is_current flag is set to False
    3. A new record is inserted with the updated values
    4. The new record gets scd_start_date = today, is_current = True

This preserves the full history of dimension changes for accurate
point-in-time analytics.
"""

import hashlib
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.helpers import get_logger

logger = get_logger("models.scd_type2")

DEFAULT_END_DATE = "9999-12-31"


def generate_hash_key(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Generate a SHA-256 hash of specified columns for change detection.

    This hash is compared between source and target to detect
    whether a dimension record has changed.
    """
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in columns])
    return df.withColumn("_hash_key", F.sha2(concat_expr, 256))


def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    business_key: str,
    tracked_columns: list[str],
    surrogate_key: str = None,
) -> DataFrame:
    """
    Apply SCD Type 2 logic to a dimension table.

    Args:
        spark:            Active SparkSession
        source_df:        New/updated dimension records from Silver
        target_path:      Delta table path for the dimension
        business_key:     Natural key column (e.g., 'customer_id')
        tracked_columns:  Columns to monitor for changes
        surrogate_key:    Name for the surrogate key column

    Returns:
        Updated dimension DataFrame with SCD Type 2 columns.
    """
    if surrogate_key is None:
        surrogate_key = f"{business_key.replace('_id', '')}_key"

    logger.info(f"Applying SCD Type 2 on {business_key} (tracking: {tracked_columns})")

    # Add hash to source for change detection
    source_df = generate_hash_key(source_df, tracked_columns)

    # Check if target table exists
    try:
        target_df = spark.read.format("delta").load(target_path)
        target_exists = True
        logger.info(f"Existing dimension found at {target_path}")
    except Exception:
        target_exists = False
        logger.info(f"No existing dimension — initializing fresh at {target_path}")

    if not target_exists:
        # First load — initialize all records as current
        result_df = _initialize_dimension(source_df, business_key, surrogate_key)
    else:
        result_df = _merge_dimension(
            source_df, target_df, business_key, surrogate_key, tracked_columns
        )

    return result_df


def _initialize_dimension(
    source_df: DataFrame,
    business_key: str,
    surrogate_key: str,
) -> DataFrame:
    """Create initial dimension load with SCD Type 2 metadata columns."""
    return (
        source_df.withColumn(
            surrogate_key,
            F.monotonically_increasing_id() + 1,
        )
        .withColumn("scd_start_date", F.current_date())
        .withColumn("scd_end_date", F.to_date(F.lit(DEFAULT_END_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn("_scd_version", F.lit(1))
    )


def _merge_dimension(
    source_df: DataFrame,
    target_df: DataFrame,
    business_key: str,
    surrogate_key: str,
    tracked_columns: list[str],
) -> DataFrame:
    """
    Merge source into existing dimension, applying SCD Type 2 logic.

    Steps:
        1. Identify unchanged records (hash matches) — keep as-is
        2. Identify changed records — expire old, insert new version
        3. Identify new records — insert as current
    """
    # Add hash to current target records
    target_current = target_df.filter(F.col("is_current") == True)
    target_current = generate_hash_key(target_current, tracked_columns)
    target_historical = target_df.filter(F.col("is_current") == False)

    # Join source and target on business key
    joined = source_df.alias("src").join(
        target_current.alias("tgt"),
        F.col(f"src.{business_key}") == F.col(f"tgt.{business_key}"),
        "full_outer",
    )

    # ── Category 1: Unchanged records (hash matches)
    unchanged = (
        joined.filter(
            F.col(f"tgt.{business_key}").isNotNull()
            & F.col(f"src.{business_key}").isNotNull()
            & (F.col("src._hash_key") == F.col("tgt._hash_key"))
        )
        .select([F.col(f"tgt.{c}") for c in target_current.drop("_hash_key").columns])
    )

    # ── Category 2: Changed records — expire old version
    changed_old = (
        joined.filter(
            F.col(f"tgt.{business_key}").isNotNull()
            & F.col(f"src.{business_key}").isNotNull()
            & (F.col("src._hash_key") != F.col("tgt._hash_key"))
        )
        .select([F.col(f"tgt.{c}") for c in target_current.drop("_hash_key").columns])
        .withColumn("scd_end_date", F.current_date())
        .withColumn("is_current", F.lit(False))
    )

    # ── Category 2: Changed records — insert new version
    max_key = target_df.agg(F.max(surrogate_key)).collect()[0][0] or 0

    changed_new_base = joined.filter(
        F.col(f"tgt.{business_key}").isNotNull()
        & F.col(f"src.{business_key}").isNotNull()
        & (F.col("src._hash_key") != F.col("tgt._hash_key"))
    )

    # Select source columns (excluding metadata from target)
    src_columns = [c for c in source_df.columns if c != "_hash_key"]
    changed_new = (
        changed_new_base.select([F.col(f"src.{c}") for c in src_columns])
        .withColumn(surrogate_key, F.monotonically_increasing_id() + max_key + 1)
        .withColumn("scd_start_date", F.current_date())
        .withColumn("scd_end_date", F.to_date(F.lit(DEFAULT_END_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "_scd_version",
            F.lit(2),  # Simplified; production would look up actual version
        )
    )

    # ── Category 3: New records (only in source, not in target)
    new_records_base = joined.filter(
        F.col(f"tgt.{business_key}").isNull()
    )
    new_records = (
        new_records_base.select([F.col(f"src.{c}") for c in src_columns])
        .withColumn(surrogate_key, F.monotonically_increasing_id() + max_key + 10001)
        .withColumn("scd_start_date", F.current_date())
        .withColumn("scd_end_date", F.to_date(F.lit(DEFAULT_END_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn("_scd_version", F.lit(1))
    )

    # ── Union all categories
    # Ensure column alignment
    all_cols = target_df.drop("_hash_key").columns if "_hash_key" in target_df.columns else target_df.columns

    # For initial merge, align columns across all DataFrames
    result = (
        target_historical
        .unionByName(unchanged, allowMissingColumns=True)
        .unionByName(changed_old, allowMissingColumns=True)
        .unionByName(changed_new, allowMissingColumns=True)
        .unionByName(new_records, allowMissingColumns=True)
    )

    counts = {
        "unchanged": unchanged.count(),
        "expired": changed_old.count(),
        "new_versions": changed_new.count(),
        "brand_new": new_records.count(),
    }
    logger.info(f"SCD Type 2 merge results: {counts}")

    return result
