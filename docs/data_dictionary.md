# Data Dictionary

## Overview

This document describes all tables in the Lakehouse across Bronze, Silver, and Gold layers.

---

## Bronze Layer (Raw)

Raw data ingested as-is from source systems. Metadata columns added for lineage tracking.

### Metadata Columns (all Bronze tables)

| Column | Type | Description |
|--------|------|-------------|
| `_ingestion_timestamp` | timestamp | UTC time when the record was ingested |
| `_source_file` | string | Source identifier (e.g., "customers", "orders") |
| `_batch_id` | string | Unique batch identifier (format: YYYYMMDD_HHMMSS) |

### bronze.customers

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| customer_id | string | CSV | Unique customer identifier (business key) |
| first_name | string | CSV | Customer first name |
| last_name | string | CSV | Customer last name |
| email | string | CSV | Email address |
| phone | string | CSV | Phone number (international format) |
| segment | string | CSV | Customer tier: Standard, Premium, Enterprise |
| region | string | CSV | Geographic region |
| city | string | CSV | City of residence |
| country | string | CSV | Country of residence |
| registration_date | string | CSV | Account creation date (YYYY-MM-DD) |

### bronze.products

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| product_id | string | JSON | Unique product identifier (business key) |
| name | string | JSON | Product name |
| category | string | JSON | Top-level category |
| subcategory | string | JSON | Sub-category |
| brand | string | JSON | Brand name |
| unit_price | double | JSON | Retail price (EUR) |
| cost_price | double | JSON | Cost to company (EUR) |
| launch_date | string | JSON | Product launch date |
| is_active | boolean | JSON | Whether product is currently available |

### bronze.orders

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| order_id | string | CSV | Unique order identifier (business key) |
| customer_id | string | CSV | FK to customers |
| product_id | string | CSV | FK to products |
| order_date | string | CSV | Date order was placed |
| quantity | integer | CSV | Number of units ordered |
| unit_price | double | CSV | Price per unit at time of order |
| discount_pct | double | CSV | Discount applied (0.0 to 1.0) |
| payment_method | string | CSV | Payment type: credit_card, bank_transfer, paypal |
| order_status | string | CSV | Status: pending, shipped, completed, cancelled, returned |
| shipping_country | string | CSV | Destination country |

---

## Silver Layer (Cleaned & Validated)

Cleaned, deduplicated, type-enforced data. Bad records routed to quarantine.

### Derived Columns Added in Silver

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| customers | full_name | string | Concatenation of first_name + last_name |
| products | profit_margin | double | (unit_price - cost_price) / unit_price |
| orders | gross_revenue | double | quantity × unit_price |
| orders | net_revenue | double | quantity × unit_price × (1 - discount_pct) |

### Quality Rules Applied

| Table | Rule | Action |
|-------|------|--------|
| customers | Null customer_id | → quarantine |
| customers | Null email | → quarantine |
| customers | Duplicate customer_id | Keep latest by registration_date |
| products | Null product_id | → quarantine |
| products | unit_price ≤ 0 | → quarantine |
| products | Duplicate product_id | Keep latest by launch_date |
| orders | Null order_id / customer_id / product_id | → quarantine |
| orders | Invalid order_date | → quarantine |
| orders | quantity ≤ 0 | → quarantine |
| orders | discount_pct outside [0, 1] | → quarantine |

---

## Gold Layer (Star Schema)

Dimensional model optimized for analytics and reporting.

### dim_date (Calendar Dimension)

| Column | Type | Description |
|--------|------|-------------|
| date_key | integer | Surrogate key (YYYYMMDD format) |
| full_date | date | Actual date value |
| year | integer | Calendar year |
| quarter | integer | Calendar quarter (1–4) |
| month | integer | Month number (1–12) |
| month_name | string | Full month name |
| week_of_year | integer | ISO week number |
| day_of_month | integer | Day of month (1–31) |
| day_of_week | integer | Day of week (1=Sunday, 7=Saturday) |
| day_name | string | Full day name |
| is_weekend | boolean | True if Saturday or Sunday |
| fiscal_year | integer | Fiscal year (July start) |
| fiscal_quarter | integer | Fiscal quarter |
| year_month | string | YYYY-MM format for grouping |

### dim_customer (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| customer_key | long | Surrogate key (auto-generated) |
| customer_id | string | Business key (natural key from source) |
| customer_name | string | Full name |
| first_name | string | First name |
| last_name | string | Last name |
| email | string | Email address |
| phone | string | Phone number |
| customer_segment | string | Tier: Standard, Premium, Enterprise |
| region | string | Geographic region |
| city | string | City |
| country | string | Country |
| registration_date | date | Account creation date |
| scd_start_date | date | Effective start date of this record version |
| scd_end_date | date | Effective end date (9999-12-31 if current) |
| is_current | boolean | True if this is the active version |
| _scd_version | integer | Version number for this business key |

### dim_product (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| product_key | long | Surrogate key (auto-generated) |
| product_id | string | Business key (natural key from source) |
| product_name | string | Product display name |
| product_category | string | Top-level category |
| product_subcategory | string | Sub-category |
| brand | string | Brand name |
| list_price | double | Current retail price |
| cost_price | double | Cost to company |
| profit_margin | double | Calculated margin |
| launch_date | date | Product launch date |
| is_active | boolean | Product availability flag |
| scd_start_date | date | Effective start date |
| scd_end_date | date | Effective end date (9999-12-31 if current) |
| is_current | boolean | True if active version |
| _scd_version | integer | Version number |

### fact_orders (Transactional Fact)

| Column | Type | Description |
|--------|------|-------------|
| order_key | long | Surrogate key |
| order_id | string | Business key (degenerate dimension) |
| date_key | integer | FK → dim_date.date_key |
| customer_key | long | FK → dim_customer.customer_key |
| product_key | long | FK → dim_product.product_key |
| order_date | date | Order placement date |
| quantity | integer | Units ordered |
| unit_price | double | Price at time of order |
| discount_pct | double | Discount applied |
| gross_revenue | double | quantity × unit_price |
| net_revenue | double | Revenue after discount |
| payment_method | string | Payment type |
| order_status | string | Current order status |
| shipping_country | string | Destination country |
