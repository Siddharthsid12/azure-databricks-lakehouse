"""
Shared pytest fixtures for all test modules.
Provides a session-scoped SparkSession with Delta Lake support.
"""

import os
import shutil
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a single SparkSession shared across all tests in the session.
    Configured with Delta Lake extensions for local testing.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("LakehouseTestSuite")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")  # Disable Spark UI in tests
        .getOrCreate()
    )

    # Set log level to reduce noise during tests
    session.sparkContext.setLogLevel("WARN")

    yield session
    session.stop()


@pytest.fixture(scope="session")
def test_data_path():
    """Path to sample test data files."""
    return os.path.join(os.path.dirname(__file__), "..", "data", "sample")


@pytest.fixture(autouse=True)
def clean_tmp_delta():
    """Clean up temporary Delta table paths before and after each test."""
    paths = ["/tmp/test_scd", "/tmp/test_lakehouse"]
    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)
    yield
    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)


@pytest.fixture
def sample_customers(spark):
    """Provide a small customer DataFrame for testing."""
    return spark.createDataFrame(
        [
            ("C001", "Anna", "Müller", "anna@test.com", "+49-170", "Premium", "Europe", "Berlin", "Germany", "2023-01-15"),
            ("C002", "Erik", "Hansen", "erik@test.com", "+47-912", "Standard", "Europe", "Oslo", "Norway", "2023-02-20"),
            ("C003", "Sofia", "Lindberg", "sofia@test.com", "+46-70", "Premium", "Europe", "Stockholm", "Sweden", "2023-03-10"),
        ],
        ["customer_id", "first_name", "last_name", "email", "phone", "segment", "region", "city", "country", "registration_date"],
    )


@pytest.fixture
def sample_products(spark):
    """Provide a small product DataFrame for testing."""
    return spark.createDataFrame(
        [
            ("P001", "Cloud Analytics", "Software", "Analytics", "DataVision", 299.99, 120.0, "2023-01-01", True),
            ("P002", "Data Suite", "Software", "ETL", "DataVision", 499.99, 200.0, "2023-02-15", True),
            ("P003", "ML Toolkit", "Software", "ML", "AIForge", 799.99, 350.0, "2023-03-01", True),
        ],
        ["product_id", "name", "category", "subcategory", "brand", "unit_price", "cost_price", "launch_date", "is_active"],
    )


@pytest.fixture
def sample_orders(spark):
    """Provide a small order DataFrame for testing."""
    return spark.createDataFrame(
        [
            ("ORD-001", "C001", "P001", "2024-01-05", 2, 299.99, 0.10, "credit_card", "completed", "Germany"),
            ("ORD-002", "C002", "P003", "2024-01-08", 1, 799.99, 0.00, "bank_transfer", "completed", "Norway"),
            ("ORD-003", "C003", "P002", "2024-01-12", 3, 499.99, 0.15, "credit_card", "completed", "Sweden"),
        ],
        ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct", "payment_method", "order_status", "shipping_country"],
    )
