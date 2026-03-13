# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Star Schema with SCD Type 2
# MAGIC
# MAGIC Builds the dimensional model for analytics:
# MAGIC - **dim_date**: Calendar dimension (generated)
# MAGIC - **dim_customer**: Customer dimension with SCD Type 2 historical tracking
# MAGIC - **dim_product**: Product dimension with SCD Type 2 historical tracking
# MAGIC - **fact_orders**: Order fact table with surrogate key references
# MAGIC
# MAGIC SCD Type 2 preserves the full history of dimension changes, enabling
# MAGIC accurate point-in-time reporting (e.g., "what segment was this customer
# MAGIC in when they placed this order?").

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")

STORAGE_ACCOUNT = spark.conf.get("spark.lakehouse.storage_account", "lakehousestorage")
CONTAINER = "lakehouse"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

print(f"Silver → Gold star schema build (env={environment})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Modules

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/lakehouse-project")

from src.transformations.gold import run_gold_layer
from src.utils.helpers import get_logger

logger = get_logger("notebook.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Star Schema

# COMMAND ----------

results = run_gold_layer(spark, SILVER_PATH, GOLD_PATH)

for table, count in results.items():
    print(f"gold.{table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer — SCD Type 2

# COMMAND ----------

dim_customer = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")

# Show current records
print("Current customer records:")
display(dim_customer.filter("is_current = true").orderBy("customer_id"))

# Show historical records (expired versions)
historical = dim_customer.filter("is_current = false")
if historical.count() > 0:
    print(f"\nHistorical (expired) records: {historical.count()}")
    display(historical)
else:
    print("\nNo historical records yet (first load)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_product — SCD Type 2

# COMMAND ----------

dim_product = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")
display(dim_product.filter("is_current = true").orderBy("product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date — Calendar Dimension

# COMMAND ----------

dim_date = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")
display(dim_date.limit(20))
print(f"Date range: {dim_date.count()} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_orders — Fact Table

# COMMAND ----------

fact_orders = spark.read.format("delta").load(f"{GOLD_PATH}/fact_orders")
display(fact_orders.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star Schema Validation Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Customer Segment

# COMMAND ----------

fact_orders.createOrReplaceTempView("fact_orders")
dim_customer.createOrReplaceTempView("dim_customer")
dim_product.createOrReplaceTempView("dim_product")
dim_date.createOrReplaceTempView("dim_date")

revenue_by_segment = spark.sql("""
    SELECT
        c.customer_segment,
        COUNT(DISTINCT f.order_id) AS total_orders,
        ROUND(SUM(f.net_revenue), 2) AS total_revenue,
        ROUND(AVG(f.net_revenue), 2) AS avg_order_value
    FROM fact_orders f
    JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
    GROUP BY c.customer_segment
    ORDER BY total_revenue DESC
""")
display(revenue_by_segment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue Trend

# COMMAND ----------

monthly_trend = spark.sql("""
    SELECT
        d.year_month,
        COUNT(DISTINCT f.order_id) AS orders,
        ROUND(SUM(f.net_revenue), 2) AS revenue
    FROM fact_orders f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year_month
    ORDER BY d.year_month
""")
display(monthly_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Products by Revenue

# COMMAND ----------

top_products = spark.sql("""
    SELECT
        p.product_name,
        p.product_category,
        p.brand,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.quantity) AS units_sold,
        ROUND(SUM(f.net_revenue), 2) AS total_revenue
    FROM fact_orders f
    JOIN dim_product p ON f.product_key = p.product_key AND p.is_current = true
    GROUP BY p.product_name, p.product_category, p.brand
    ORDER BY total_revenue DESC
""")
display(top_products)

# COMMAND ----------

dbutils.notebook.exit("GOLD_STAR_SCHEMA_SUCCESS")
