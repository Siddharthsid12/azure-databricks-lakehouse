"""
Integration Test — End-to-End Pipeline
========================================
Runs the complete Bronze → Silver → Gold pipeline using sample data
and verifies the final star schema output is correct.

Marked as 'slow' — run with: pytest tests/test_integration.py -v
Skip with: pytest -m "not slow"
"""

import os
import shutil
import pytest
from pyspark.sql import functions as F

from src.transformations.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold import run_gold_layer
from src.data_quality.validators import DataQualityValidator

# spark fixture provided by conftest.py

# Paths for integration test
INT_BASE = "/tmp/test_lakehouse"
INT_BRONZE = f"{INT_BASE}/bronze"
INT_SILVER = f"{INT_BASE}/silver"
INT_GOLD = f"{INT_BASE}/gold"
INT_QUARANTINE = f"{INT_BASE}/quarantine"

SAMPLE_DATA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "sample",
)

QUALITY_RULES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "data_quality_rules.yaml",
)


@pytest.fixture(autouse=True)
def clean_integration_path():
    """Clean integration test directory."""
    if os.path.exists(INT_BASE):
        shutil.rmtree(INT_BASE)
    os.makedirs(INT_BASE, exist_ok=True)
    yield
    if os.path.exists(INT_BASE):
        shutil.rmtree(INT_BASE)


@pytest.mark.slow
class TestEndToEndPipeline:
    """Full pipeline integration test using sample data."""

    def test_bronze_ingests_all_sources(self, spark):
        """Bronze should ingest all 3 source files."""
        results = run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)

        assert "customers" in results
        assert "products" in results
        assert "orders" in results
        assert results["customers"] > 0
        assert results["products"] > 0
        assert results["orders"] > 0

    def test_silver_cleans_and_quarantines(self, spark):
        """Silver should clean data and quarantine bad records."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        results = run_silver_transformations(
            spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE,
        )

        # Should have clean records
        for table in ["customers", "products", "orders"]:
            assert results[table]["clean"] > 0

        # Sample data has intentional bad records — some should be quarantined
        total_quarantined = sum(r["quarantined"] for r in results.values())
        assert total_quarantined > 0, "Sample data should produce quarantined records"

    def test_silver_deduplicates_customers(self, spark):
        """Duplicate customer_ids in source should be deduplicated in Silver."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)

        silver_customers = spark.read.format("delta").load(f"{INT_SILVER}/customers")
        customer_ids = [r["customer_id"] for r in silver_customers.select("customer_id").collect()]

        assert len(customer_ids) == len(set(customer_ids)), "Silver customers should have unique IDs"

    def test_silver_calculates_derived_columns(self, spark):
        """Silver orders should have gross_revenue and net_revenue."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)

        silver_orders = spark.read.format("delta").load(f"{INT_SILVER}/orders")
        assert "gross_revenue" in silver_orders.columns
        assert "net_revenue" in silver_orders.columns

        # All revenues should be positive
        negative = silver_orders.filter(
            (F.col("gross_revenue") <= 0) | (F.col("net_revenue") <= 0)
        ).count()
        assert negative == 0, "All revenue values should be positive"

    def test_gold_builds_complete_star_schema(self, spark):
        """Gold layer should contain all 4 star schema tables."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)
        results = run_gold_layer(spark, INT_SILVER, INT_GOLD)

        assert "dim_date" in results
        assert "dim_customer" in results
        assert "dim_product" in results
        assert "fact_orders" in results

        # All tables should have rows
        for table, count in results.items():
            assert count > 0, f"gold.{table} should have rows"

    def test_gold_dim_customer_has_scd_columns(self, spark):
        """dim_customer should have SCD Type 2 columns."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)
        run_gold_layer(spark, INT_SILVER, INT_GOLD)

        dim_customer = spark.read.format("delta").load(f"{INT_GOLD}/dim_customer")

        required_cols = ["customer_key", "scd_start_date", "scd_end_date", "is_current"]
        for col in required_cols:
            assert col in dim_customer.columns, f"Missing SCD column: {col}"

        # All records should be current on first load
        assert dim_customer.filter("is_current = true").count() == dim_customer.count()

    def test_gold_fact_has_surrogate_keys(self, spark):
        """fact_orders should reference dimension surrogate keys."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)
        run_gold_layer(spark, INT_SILVER, INT_GOLD)

        fact = spark.read.format("delta").load(f"{INT_GOLD}/fact_orders")

        assert "customer_key" in fact.columns
        assert "product_key" in fact.columns
        assert "date_key" in fact.columns

    def test_gold_star_schema_joins_correctly(self, spark):
        """Star schema tables should join without producing nulls on valid keys."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)
        run_gold_layer(spark, INT_SILVER, INT_GOLD)

        fact = spark.read.format("delta").load(f"{INT_GOLD}/fact_orders")
        dim_customer = spark.read.format("delta").load(f"{INT_GOLD}/dim_customer")
        dim_date = spark.read.format("delta").load(f"{INT_GOLD}/dim_date")

        # Join fact with dim_customer
        fact.createOrReplaceTempView("fact_orders")
        dim_customer.createOrReplaceTempView("dim_customer")
        dim_date.createOrReplaceTempView("dim_date")

        # Date keys should all resolve
        result = spark.sql("""
            SELECT COUNT(*) AS total,
                   COUNT(d.date_key) AS matched
            FROM fact_orders f
            LEFT JOIN dim_date d ON f.date_key = d.date_key
        """).collect()[0]

        assert result["total"] == result["matched"], "All date keys should resolve"

    def test_full_pipeline_data_quality(self, spark):
        """Quality checks should pass on Silver data after full pipeline."""
        run_bronze_ingestion(spark, SAMPLE_DATA, INT_BRONZE)
        run_silver_transformations(spark, INT_BRONZE, INT_SILVER, INT_QUARANTINE)

        validator = DataQualityValidator(spark, rules_path=QUALITY_RULES)

        silver_customers = spark.read.format("delta").load(f"{INT_SILVER}/customers")
        results = validator.validate_table(silver_customers, "customers")
        report = validator.generate_report(results)

        # After Silver cleaning, critical checks should pass
        assert report["critical_failures"] == 0, (
            f"Critical failures on cleaned data: {report['details']}"
        )
