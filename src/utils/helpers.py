"""
Shared utilities for the Lakehouse pipeline.
Provides config loading, logging, and Spark session management.
"""

import os
import yaml
import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Create a structured logger with consistent formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """
    Load pipeline configuration from YAML file.
    Resolves environment variables in the format ${VAR_NAME}.
    """
    with open(config_path, "r") as f:
        raw = f.read()

    # Resolve environment variables
    for key, value in os.environ.items():
        raw = raw.replace(f"${{{key}}}", value)

    return yaml.safe_load(raw)


def load_quality_rules(rules_path: str = "config/data_quality_rules.yaml") -> dict:
    """Load data quality rules from YAML configuration."""
    with open(rules_path, "r") as f:
        return yaml.safe_load(f)


def get_spark_session(
    app_name: str = "LakehousePipeline",
    config: Optional[dict] = None,
    local: bool = False,
) -> SparkSession:
    """
    Create or retrieve SparkSession.

    In Databricks, this returns the existing session.
    For local dev/testing, creates a local session with Delta support.
    """
    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
        )

    if config and "spark_config" in config.get("pipeline", {}):
        for k, v in config["pipeline"]["spark_config"].items():
            builder = builder.config(k, str(v))

    return builder.getOrCreate()


def get_current_timestamp() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def get_storage_path(config: dict, layer: str, local: bool = False) -> str:
    """
    Get the storage path for a given layer (bronze/silver/gold).

    Args:
        config: Pipeline configuration dictionary.
        layer: One of 'bronze', 'silver', 'gold', 'quarantine'.
        local: If True, return local filesystem path for testing.
    """
    section = "local" if local else "storage"
    key = f"{layer}_path"
    return config[section][key]


def table_path(base_path: str, table_name: str) -> str:
    """Construct a full Delta table path."""
    return f"{base_path}/{table_name}"
