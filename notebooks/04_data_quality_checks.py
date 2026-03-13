# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Data Quality Gate
# MAGIC
# MAGIC Runs automated quality checks between Silver and Gold layers.
# MAGIC **Critical failures block the pipeline** — this notebook must pass
# MAGIC before the Gold layer build proceeds.
# MAGIC
# MAGIC Checks include: null validation, uniqueness, referential integrity,
# MAGIC value ranges, format validation, and data freshness.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("rules_path", "/Workspace/Repos/lakehouse-project/config/data_quality_rules.yaml", "Rules Path")

environment = dbutils.widgets.get("environment")
rules_path = dbutils.widgets.get("rules_path")

STORAGE_ACCOUNT = spark.conf.get("spark.lakehouse.storage_account", "lakehousestorage")
CONTAINER = "lakehouse"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SILVER_PATH = f"{BASE_PATH}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Modules

# COMMAND ----------

import sys
import json
sys.path.insert(0, "/Workspace/Repos/lakehouse-project")

from src.data_quality.validators import DataQualityValidator
from src.utils.helpers import get_logger

logger = get_logger("notebook.quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Validator

# COMMAND ----------

validator = DataQualityValidator(spark, rules_path=rules_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks — Customers

# COMMAND ----------

silver_customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
customer_results = validator.validate_table(silver_customers, "customers")

print(f"\nCustomers: {sum(1 for r in customer_results if r.passed)}/{len(customer_results)} checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks — Products

# COMMAND ----------

silver_products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
product_results = validator.validate_table(silver_products, "products")

print(f"\nProducts: {sum(1 for r in product_results if r.passed)}/{len(product_results)} checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks — Orders

# COMMAND ----------

silver_orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
order_results = validator.validate_table(silver_orders, "orders")

print(f"\nOrders: {sum(1 for r in order_results if r.passed)}/{len(order_results)} checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity Checks

# COMMAND ----------

ri_customer = validator.check_referential_integrity(
    source_df=silver_orders,
    reference_df=silver_customers,
    source_col="customer_id",
    reference_col="customer_id",
    rule_name="orders_customer_exists",
)

ri_product = validator.check_referential_integrity(
    source_df=silver_orders,
    reference_df=silver_products,
    source_col="product_id",
    reference_col="product_id",
    rule_name="orders_product_exists",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Report Summary

# COMMAND ----------

all_results = customer_results + product_results + order_results + [ri_customer, ri_product]
report = validator.generate_report(all_results)

print(f"{'='*60}")
print(f"DATA QUALITY REPORT")
print(f"{'='*60}")
print(f"Total checks:      {report['total_checks']}")
print(f"Passed:            {report['passed']}")
print(f"Failed:            {report['failed']}")
print(f"Critical failures: {report['critical_failures']}")
print(f"Warnings:          {report['warnings']}")
print(f"{'='*60}")

# Display detailed results
for detail in report["details"]:
    status_icon = "✓" if detail["status"] == "PASS" else "✗"
    print(f"  {status_icon} [{detail['severity']:8s}] {detail['rule']:35s} {detail['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Gate — Block Pipeline on Critical Failures

# COMMAND ----------

try:
    validator.assert_critical_passed(all_results)
    print("\n✓ QUALITY GATE PASSED — Pipeline may proceed to Gold layer")
    dbutils.notebook.exit("QUALITY_GATE_PASSED")
except Exception as e:
    print(f"\n✗ QUALITY GATE FAILED — Pipeline blocked")
    print(str(e))
    dbutils.notebook.exit("QUALITY_GATE_FAILED")
