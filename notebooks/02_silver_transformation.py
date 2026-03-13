# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer Transformation
# MAGIC
# MAGIC Cleans, validates, and deduplicates Bronze data into Silver.
# MAGIC Bad records are quarantined for auditing — never silently dropped.
# MAGIC
# MAGIC **Transformations:** Type casting, null handling, deduplication,
# MAGIC standardization, derived columns, quarantine routing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")

STORAGE_ACCOUNT = spark.conf.get("spark.lakehouse.storage_account", "lakehousestorage")
CONTAINER = "lakehouse"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
QUARANTINE_PATH = f"{BASE_PATH}/quarantine"

print(f"Bronze → Silver transformation (env={environment})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Modules

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/lakehouse-project")

from src.transformations.silver import (
    transform_customers,
    transform_products,
    transform_orders,
    write_silver_table,
    write_quarantine,
)
from src.utils.helpers import get_logger

logger = get_logger("notebook.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Customers

# COMMAND ----------

bronze_customers = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")
print(f"Bronze customers: {bronze_customers.count()} rows")

clean_customers, quarantine_customers = transform_customers(bronze_customers)

print(f"Clean: {clean_customers.count()} | Quarantined: {quarantine_customers.count()}")
display(clean_customers.limit(10))

# COMMAND ----------

write_silver_table(clean_customers, SILVER_PATH, "customers")
write_quarantine(quarantine_customers, QUARANTINE_PATH, "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Products

# COMMAND ----------

bronze_products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
print(f"Bronze products: {bronze_products.count()} rows")

clean_products, quarantine_products = transform_products(bronze_products)

print(f"Clean: {clean_products.count()} | Quarantined: {quarantine_products.count()}")
display(clean_products.limit(10))

# COMMAND ----------

write_silver_table(clean_products, SILVER_PATH, "products")
write_quarantine(quarantine_products, QUARANTINE_PATH, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Orders

# COMMAND ----------

bronze_orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
print(f"Bronze orders: {bronze_orders.count()} rows")

clean_orders, quarantine_orders = transform_orders(bronze_orders)

print(f"Clean: {clean_orders.count()} | Quarantined: {quarantine_orders.count()}")

# COMMAND ----------

# Show quarantined records for inspection
if quarantine_orders.count() > 0:
    print("⚠ Quarantined order records:")
    display(quarantine_orders)

# COMMAND ----------

write_silver_table(clean_orders, SILVER_PATH, "orders")
write_quarantine(quarantine_orders, QUARANTINE_PATH, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary

# COMMAND ----------

summary = {}
for table in ["customers", "products", "orders"]:
    df = spark.read.format("delta").load(f"{SILVER_PATH}/{table}")
    summary[table] = df.count()
    print(f"silver.{table}: {df.count()} rows")

print(f"\nTotal quarantined records: "
      f"{quarantine_customers.count() + quarantine_products.count() + quarantine_orders.count()}")

# COMMAND ----------

dbutils.notebook.exit("SILVER_TRANSFORMATION_SUCCESS")
