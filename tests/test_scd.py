"""
Unit Tests — SCD Type 2 Logic
===============================
Tests for Slowly Changing Dimension Type 2 implementation.

Run: pytest tests/test_scd.py -v
"""

import pytest
import os
import shutil
from pyspark.sql import functions as F

from src.models.scd_type2 import apply_scd_type2, generate_hash_key

# spark and clean_tmp_delta fixtures provided by conftest.py


class TestHashKeyGeneration:
    def test_generates_hash_for_columns(self, spark):
        df = spark.createDataFrame(
            [("Anna", "Premium", "Berlin")],
            ["name", "segment", "city"],
        )
        result = generate_hash_key(df, ["name", "segment", "city"])
        assert "_hash_key" in result.columns
        assert result.collect()[0]["_hash_key"] is not None

    def test_different_data_produces_different_hash(self, spark):
        df = spark.createDataFrame(
            [("Anna", "Premium"), ("Anna", "Standard")],
            ["name", "segment"],
        )
        result = generate_hash_key(df, ["name", "segment"])
        hashes = [r["_hash_key"] for r in result.collect()]
        assert hashes[0] != hashes[1]

    def test_same_data_produces_same_hash(self, spark):
        df = spark.createDataFrame(
            [("Anna", "Premium"), ("Anna", "Premium")],
            ["name", "segment"],
        )
        result = generate_hash_key(df, ["name", "segment"])
        hashes = [r["_hash_key"] for r in result.collect()]
        assert hashes[0] == hashes[1]

    def test_handles_null_values(self, spark):
        df = spark.createDataFrame(
            [(None, "Premium"), ("Anna", None)],
            ["name", "segment"],
        )
        result = generate_hash_key(df, ["name", "segment"])
        hashes = [r["_hash_key"] for r in result.collect()]
        # Nulls should be handled (not crash) and produce different hashes
        assert hashes[0] is not None
        assert hashes[1] is not None
        assert hashes[0] != hashes[1]


class TestSCDType2InitialLoad:
    def test_initial_load_adds_scd_columns(self, spark):
        source = spark.createDataFrame(
            [("C001", "Anna", "Premium"), ("C002", "Erik", "Standard")],
            ["customer_id", "name", "segment"],
        )

        result = apply_scd_type2(
            spark=spark,
            source_df=source,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )

        assert "customer_key" in result.columns
        assert "scd_start_date" in result.columns
        assert "scd_end_date" in result.columns
        assert "is_current" in result.columns
        assert "_scd_version" in result.columns

    def test_initial_load_all_records_current(self, spark):
        source = spark.createDataFrame(
            [("C001", "Anna", "Premium"), ("C002", "Erik", "Standard")],
            ["customer_id", "name", "segment"],
        )

        result = apply_scd_type2(
            spark=spark,
            source_df=source,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )

        assert result.count() == 2
        current_count = result.filter("is_current = true").count()
        assert current_count == 2

    def test_initial_load_end_date_is_far_future(self, spark):
        source = spark.createDataFrame(
            [("C001", "Anna", "Premium")],
            ["customer_id", "name", "segment"],
        )

        result = apply_scd_type2(
            spark=spark,
            source_df=source,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
        )

        row = result.collect()[0]
        assert str(row["scd_end_date"]) == "9999-12-31"


class TestSCDType2ChangeDetection:
    def test_detects_changed_record(self, spark):
        """When a tracked column changes, old record should expire."""
        # Initial load
        initial = spark.createDataFrame(
            [("C001", "Anna", "Premium")],
            ["customer_id", "name", "segment"],
        )
        dim = apply_scd_type2(
            spark=spark,
            source_df=initial,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )
        dim.write.format("delta").mode("overwrite").save("/tmp/test_scd/dim_customer")

        # Updated load — segment changed
        updated = spark.createDataFrame(
            [("C001", "Anna", "Enterprise")],  # segment changed
            ["customer_id", "name", "segment"],
        )
        result = apply_scd_type2(
            spark=spark,
            source_df=updated,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )

        # Should have 2 records: 1 expired + 1 current
        assert result.count() == 2
        assert result.filter("is_current = true").count() == 1
        assert result.filter("is_current = false").count() == 1

        # Current record should have new segment
        current = result.filter("is_current = true").collect()[0]
        assert current["segment"] == "Enterprise"

    def test_unchanged_records_not_duplicated(self, spark):
        """If nothing changed, the dimension should remain the same size."""
        initial = spark.createDataFrame(
            [("C001", "Anna", "Premium"), ("C002", "Erik", "Standard")],
            ["customer_id", "name", "segment"],
        )
        dim = apply_scd_type2(
            spark=spark,
            source_df=initial,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )
        dim.write.format("delta").mode("overwrite").save("/tmp/test_scd/dim_customer")

        # Re-run with same data
        result = apply_scd_type2(
            spark=spark,
            source_df=initial,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )

        assert result.count() == 2
        assert result.filter("is_current = true").count() == 2

    def test_new_record_added(self, spark):
        """New business keys should be inserted as current."""
        initial = spark.createDataFrame(
            [("C001", "Anna", "Premium")],
            ["customer_id", "name", "segment"],
        )
        dim = apply_scd_type2(
            spark=spark,
            source_df=initial,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )
        dim.write.format("delta").mode("overwrite").save("/tmp/test_scd/dim_customer")

        # Add a new customer
        updated = spark.createDataFrame(
            [("C001", "Anna", "Premium"), ("C002", "Erik", "Standard")],
            ["customer_id", "name", "segment"],
        )
        result = apply_scd_type2(
            spark=spark,
            source_df=updated,
            target_path="/tmp/test_scd/dim_customer",
            business_key="customer_id",
            tracked_columns=["name", "segment"],
            surrogate_key="customer_key",
        )

        assert result.count() == 2
        assert result.filter("is_current = true").count() == 2
        ids = [r["customer_id"] for r in result.collect()]
        assert "C001" in ids
        assert "C002" in ids
