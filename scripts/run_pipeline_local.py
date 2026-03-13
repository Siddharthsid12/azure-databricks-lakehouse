"""
Local Pipeline Runner — Full End-to-End Execution
===================================================
Runs the complete Bronze → Silver → Quality Gate → Gold pipeline
using local Spark + Delta Lake. No Azure or Databricks required.

This is for local development and testing. In production, the
Databricks notebooks handle execution.

Usage:
    python scripts/run_pipeline_local.py
    python scripts/run_pipeline_local.py --skip-quality-gate
"""

import os
import sys
import shutil
import argparse
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.helpers import get_spark_session, get_logger, load_config
from src.transformations.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold import run_gold_layer
from src.data_quality.validators import DataQualityValidator, DataQualityError

logger = get_logger("pipeline.local", level="INFO")

# Local storage paths
LOCAL_BASE = "/tmp/lakehouse"
LOCAL_BRONZE = f"{LOCAL_BASE}/bronze"
LOCAL_SILVER = f"{LOCAL_BASE}/silver"
LOCAL_GOLD = f"{LOCAL_BASE}/gold"
LOCAL_QUARANTINE = f"{LOCAL_BASE}/quarantine"

SAMPLE_DATA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "sample",
)

QUALITY_RULES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "data_quality_rules.yaml",
)


def clean_local_storage():
    """Remove all local Delta tables for a fresh run."""
    if os.path.exists(LOCAL_BASE):
        shutil.rmtree(LOCAL_BASE)
        logger.info(f"Cleaned local storage: {LOCAL_BASE}")
    os.makedirs(LOCAL_BASE, exist_ok=True)


def run_quality_gate(spark) -> dict:
    """Run data quality checks on Silver layer."""
    logger.info("=" * 60)
    logger.info("DATA QUALITY GATE — START")
    logger.info("=" * 60)

    validator = DataQualityValidator(spark, rules_path=QUALITY_RULES)

    # Validate each Silver table
    silver_customers = spark.read.format("delta").load(f"{LOCAL_SILVER}/customers")
    silver_products = spark.read.format("delta").load(f"{LOCAL_SILVER}/products")
    silver_orders = spark.read.format("delta").load(f"{LOCAL_SILVER}/orders")

    all_results = []
    all_results += validator.validate_table(silver_customers, "customers")
    all_results += validator.validate_table(silver_products, "products")
    all_results += validator.validate_table(silver_orders, "orders")

    # Referential integrity
    all_results.append(validator.check_referential_integrity(
        silver_orders, silver_customers, "customer_id", "customer_id",
        "orders_customer_exists",
    ))
    all_results.append(validator.check_referential_integrity(
        silver_orders, silver_products, "product_id", "product_id",
        "orders_product_exists",
    ))

    report = validator.generate_report(all_results)

    logger.info(f"Quality Report: {report['passed']}/{report['total_checks']} passed")
    if report["critical_failures"] > 0:
        logger.warning(f"Critical failures: {report['critical_failures']}")
    if report["warnings"] > 0:
        logger.warning(f"Warnings: {report['warnings']}")

    # Assert critical checks (will raise on failure)
    validator.assert_critical_passed(all_results)

    logger.info("DATA QUALITY GATE — PASSED")
    return report


def print_gold_summary(spark):
    """Print a summary of the Gold layer star schema."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("GOLD LAYER — STAR SCHEMA SUMMARY")
    logger.info("=" * 60)

    tables = {
        "dim_date": f"{LOCAL_GOLD}/dim_date",
        "dim_customer": f"{LOCAL_GOLD}/dim_customer",
        "dim_product": f"{LOCAL_GOLD}/dim_product",
        "fact_orders": f"{LOCAL_GOLD}/fact_orders",
    }

    for name, path in tables.items():
        df = spark.read.format("delta").load(path)
        logger.info(f"  {name:20s} → {df.count():>6,} rows, {len(df.columns):>3} columns")

    # Show SCD Type 2 stats
    dim_customer = spark.read.format("delta").load(f"{LOCAL_GOLD}/dim_customer")
    current = dim_customer.filter("is_current = true").count()
    historical = dim_customer.filter("is_current = false").count()
    logger.info(f"\n  dim_customer SCD: {current} current + {historical} historical versions")

    dim_product = spark.read.format("delta").load(f"{LOCAL_GOLD}/dim_product")
    current_p = dim_product.filter("is_current = true").count()
    historical_p = dim_product.filter("is_current = false").count()
    logger.info(f"  dim_product  SCD: {current_p} current + {historical_p} historical versions")

    # Run a sample analytics query
    fact = spark.read.format("delta").load(f"{LOCAL_GOLD}/fact_orders")
    dim_cust = spark.read.format("delta").load(f"{LOCAL_GOLD}/dim_customer")

    fact.createOrReplaceTempView("fact_orders")
    dim_cust.createOrReplaceTempView("dim_customer")

    logger.info("\n  Sample Query — Revenue by Customer Segment:")
    result = spark.sql("""
        SELECT
            c.customer_segment,
            COUNT(DISTINCT f.order_id) AS orders,
            ROUND(SUM(f.net_revenue), 2) AS total_revenue,
            ROUND(AVG(f.net_revenue), 2) AS avg_order_value
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
        GROUP BY c.customer_segment
        ORDER BY total_revenue DESC
    """)
    result.show(truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Run Lakehouse pipeline locally")
    parser.add_argument(
        "--skip-quality-gate",
        action="store_true",
        help="Skip data quality checks (for debugging)",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Don't clean local storage before run",
    )
    args = parser.parse_args()

    start_time = datetime.utcnow()

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║     AZURE DATABRICKS LAKEHOUSE — LOCAL PIPELINE RUN    ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info("")

    # Clean previous run
    if not args.keep_data:
        clean_local_storage()

    # Initialize Spark
    spark = get_spark_session(app_name="LakehouseLocal", local=True)
    logger.info(f"Spark version: {spark.version}")
    logger.info("")

    # ── Stage 1: Bronze ──────────────────────────────────────────
    bronze_results = run_bronze_ingestion(spark, SAMPLE_DATA, LOCAL_BRONZE)
    logger.info("")

    # ── Stage 2: Silver ──────────────────────────────────────────
    silver_results = run_silver_transformations(
        spark, LOCAL_BRONZE, LOCAL_SILVER, LOCAL_QUARANTINE,
    )
    logger.info("")

    # ── Stage 3: Quality Gate ────────────────────────────────────
    quality_report = None
    if not args.skip_quality_gate:
        try:
            quality_report = run_quality_gate(spark)
        except DataQualityError as e:
            logger.error("Pipeline halted due to quality gate failure!")
            logger.error(str(e))
            spark.stop()
            sys.exit(1)
    else:
        logger.info("QUALITY GATE SKIPPED (--skip-quality-gate)")

    logger.info("")

    # ── Stage 4: Gold ────────────────────────────────────────────
    gold_results = run_gold_layer(spark, LOCAL_SILVER, LOCAL_GOLD)

    # ── Summary ──────────────────────────────────────────────────
    print_gold_summary(spark)

    elapsed = (datetime.utcnow() - start_time).total_seconds()

    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info(f"║  PIPELINE COMPLETE — {elapsed:.1f}s elapsed" + " " * (36 - len(f"{elapsed:.1f}")) + "║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info("")
    logger.info("Pipeline Results:")
    logger.info(f"  Bronze:  {bronze_results}")
    logger.info(f"  Silver:  {silver_results}")
    logger.info(f"  Gold:    {gold_results}")
    if quality_report:
        logger.info(f"  Quality: {quality_report['passed']}/{quality_report['total_checks']} checks passed")
    logger.info("")
    logger.info(f"Data written to: {LOCAL_BASE}/")
    logger.info("  Inspect with: spark.read.format('delta').load('/tmp/lakehouse/gold/fact_orders').show()")

    spark.stop()


if __name__ == "__main__":
    main()
