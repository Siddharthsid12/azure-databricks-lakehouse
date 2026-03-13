# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer Ingestion
# MAGIC
# MAGIC Ingests raw data from source files into Bronze Delta tables.
# MAGIC No business logic is applied — data is stored as-is with metadata columns.
# MAGIC
# MAGIC **Sources:** CSV (customers, orders), JSON (products)
# MAGIC **Target:** Bronze Delta tables in ADLS Gen2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Parameters (set via Databricks widgets or job parameters)
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("source_path", "", "Source Data Path")

environment = dbutils.widgets.get("environment")
source_path = dbutils.widgets.get("source_path")

# Storage paths
STORAGE_ACCOUNT = spark.conf.get("spark.lakehouse.storage_account", "lakehousestorage")
CONTAINER = "lakehouse"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE_PATH = f"{BASE_PATH}/bronze"
SOURCE_DATA_PATH = source_path or f"{BASE_PATH}/landing"

print(f"Environment: {environment}")
print(f"Source path: {SOURCE_DATA_PATH}")
print(f"Bronze path: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Pipeline Modules

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/lakehouse-project")

from src.transformations.bronze import (
    ingest_csv,
    ingest_json,
    write_bronze_table,
    SCHEMAS,
)
from src.utils.helpers import get_logger

logger = get_logger("notebook.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customers (CSV)

# COMMAND ----------

customers_df = ingest_csv(
    spark,
    f"{SOURCE_DATA_PATH}/customers.csv",
    source_name="customers",
    schema=SCHEMAS["customers"],
)

display(customers_df.limit(10))
print(f"Customers row count: {customers_df.count()}")

# COMMAND ----------

write_bronze_table(customers_df, BRONZE_PATH, "customers", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Products (JSON)

# COMMAND ----------

products_df = ingest_json(
    spark,
    f"{SOURCE_DATA_PATH}/products.json",
    source_name="products",
    schema=SCHEMAS["products"],
)

display(products_df.limit(10))
print(f"Products row count: {products_df.count()}")

# COMMAND ----------

write_bronze_table(products_df, BRONZE_PATH, "products", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Orders (CSV)

# COMMAND ----------

orders_df = ingest_csv(
    spark,
    f"{SOURCE_DATA_PATH}/orders.csv",
    source_name="orders",
    schema=SCHEMAS["orders"],
)

display(orders_df.limit(10))
print(f"Orders row count: {orders_df.count()}")

# COMMAND ----------

write_bronze_table(orders_df, BRONZE_PATH, "orders", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

for table in ["customers", "products", "orders"]:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")
    print(f"bronze.{table}: {df.count()} rows, {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit with status

# COMMAND ----------

dbutils.notebook.exit("BRONZE_INGESTION_SUCCESS")
