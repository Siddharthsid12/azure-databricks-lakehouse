"""
Unit Tests — Transformation Logic
===================================
Tests for Bronze, Silver, and Gold layer transformations.
Uses a local Spark session with Delta Lake for isolation.

Run: pytest tests/test_transformations.py -v
"""

import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

# spark fixture provided by conftest.py


# ── Bronze Layer Tests ─────────────────────────────────────────────────


class TestBronzeIngestion:
    """Tests for Bronze layer ingestion logic."""

    def test_add_metadata_columns(self, spark):
        """Verify that metadata columns are added correctly."""
        from src.transformations.bronze import add_metadata_columns

        df = spark.createDataFrame(
            [("C001", "Anna")],
            ["customer_id", "first_name"],
        )
        result = add_metadata_columns(df, "customers")

        assert "_ingestion_timestamp" in result.columns
        assert "_source_file" in result.columns
        assert "_batch_id" in result.columns

        row = result.collect()[0]
        assert row["_source_file"] == "customers"
        assert row["_batch_id"] is not None

    def test_metadata_preserves_original_columns(self, spark):
        """Metadata addition should not alter original data."""
        from src.transformations.bronze import add_metadata_columns

        df = spark.createDataFrame(
            [("C001", "Anna", "anna@test.com")],
            ["customer_id", "first_name", "email"],
        )
        result = add_metadata_columns(df, "test")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["customer_id"] == "C001"
        assert row["email"] == "anna@test.com"


# ── Silver Layer Tests ─────────────────────────────────────────────────


class TestSilverTransformations:
    """Tests for Silver layer cleaning and validation."""

    def test_customers_removes_null_ids(self, spark):
        """Records with null customer_id should be quarantined."""
        from src.transformations.silver import transform_customers

        df = spark.createDataFrame(
            [
                ("C001", "Anna", "Muller", "anna@test.com", "+49-170", "Premium", "Europe", "Berlin", "Germany", "2023-01-15"),
                (None, "Bad", "Record", "bad@test.com", "+49-170", "Standard", "Europe", "Berlin", "Germany", "2023-02-20"),
            ],
            ["customer_id", "first_name", "last_name", "email", "phone", "segment", "region", "city", "country", "registration_date"],
        )

        clean, quarantine = transform_customers(df)

        assert clean.count() == 1
        assert quarantine.count() == 1
        assert clean.collect()[0]["customer_id"] == "C001"

    def test_customers_deduplicates_on_id(self, spark):
        """Duplicate customer_ids should keep only the latest record."""
        from src.transformations.silver import transform_customers

        df = spark.createDataFrame(
            [
                ("C001", "Anna", "V1", "anna@v1.com", "+49-170", "Standard", "Europe", "Berlin", "Germany", "2023-01-15"),
                ("C001", "Anna", "V2", "anna@v2.com", "+49-170", "Premium", "Europe", "Munich", "Germany", "2024-01-10"),
            ],
            ["customer_id", "first_name", "last_name", "email", "phone", "segment", "region", "city", "country", "registration_date"],
        )

        clean, _ = transform_customers(df)

        assert clean.count() == 1
        row = clean.collect()[0]
        assert row["last_name"] == "V2"  # Latest record kept

    def test_customers_creates_full_name(self, spark):
        """full_name should be concatenation of first + last name."""
        from src.transformations.silver import transform_customers

        df = spark.createDataFrame(
            [("C001", "Anna", "Muller", "anna@test.com", "+49", "Premium", "Europe", "Berlin", "Germany", "2023-01-15")],
            ["customer_id", "first_name", "last_name", "email", "phone", "segment", "region", "city", "country", "registration_date"],
        )

        clean, _ = transform_customers(df)
        assert clean.collect()[0]["full_name"] == "Anna Muller"

    def test_orders_quarantines_negative_quantity(self, spark):
        """Orders with quantity <= 0 should be quarantined."""
        from src.transformations.silver import transform_orders

        df = spark.createDataFrame(
            [
                ("ORD-001", "C001", "P001", "2024-01-05", 2, 299.99, 0.1, "credit_card", "completed", "Germany"),
                ("ORD-002", "C002", "P002", "2024-01-08", -1, 499.99, 0.0, "bank_transfer", "completed", "Norway"),
                ("ORD-003", "C003", "P003", "2024-01-12", 0, 199.99, 0.0, "paypal", "completed", "Sweden"),
            ],
            ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct", "payment_method", "order_status", "shipping_country"],
        )

        clean, quarantine = transform_orders(df)

        assert clean.count() == 1
        assert quarantine.count() == 2
        assert clean.collect()[0]["order_id"] == "ORD-001"

    def test_orders_calculates_revenue(self, spark):
        """Verify gross and net revenue calculations."""
        from src.transformations.silver import transform_orders

        df = spark.createDataFrame(
            [("ORD-001", "C001", "P001", "2024-01-05", 2, 100.0, 0.1, "credit_card", "completed", "Germany")],
            ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct", "payment_method", "order_status", "shipping_country"],
        )

        clean, _ = transform_orders(df)
        row = clean.collect()[0]

        assert row["gross_revenue"] == 200.0   # 2 * 100
        assert row["net_revenue"] == 180.0     # 200 * (1 - 0.1)

    def test_orders_quarantines_invalid_discount(self, spark):
        """Discount > 100% should be quarantined."""
        from src.transformations.silver import transform_orders

        df = spark.createDataFrame(
            [("ORD-001", "C001", "P001", "2024-01-05", 2, 100.0, 1.5, "credit_card", "completed", "Germany")],
            ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct", "payment_method", "order_status", "shipping_country"],
        )

        clean, quarantine = transform_orders(df)

        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_products_calculates_profit_margin(self, spark):
        """Profit margin should be (price - cost) / price."""
        from src.transformations.silver import transform_products

        df = spark.createDataFrame(
            [("P001", "Test Product", "Software", "Analytics", "TestBrand", 100.0, 40.0, "2023-01-01", True)],
            ["product_id", "name", "category", "subcategory", "brand", "unit_price", "cost_price", "launch_date", "is_active"],
        )

        clean, _ = transform_products(df)
        row = clean.collect()[0]

        assert row["profit_margin"] == 0.6  # (100 - 40) / 100
