# Databricks notebook source
# MAGIC %md
# MAGIC # Loading the Fact tables in the Gold layer 
# MAGIC ## Connecting to the Gold layer (Target)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG jeromeattinger_lakehouse;
# MAGIC USE SCHEMA gold;
# MAGIC
# MAGIC DECLARE OR REPLACE load_date = current_timestamp();
# MAGIC VALUES load_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW _tmp_fact_sales AS
# MAGIC SELECT
# MAGIC     CAST(soh.sales_order_id AS INT) AS sales_order_id,
# MAGIC     CAST(sod.sales_order_detail_id AS INT) AS sales_order_detail_id,
# MAGIC
# MAGIC     --
# MAGIC     10000 * YEAR(soh.order_date) + 100 * MONTH(soh.order_date) + DAY(soh.order_date) AS _tf_dim_calendar_id,
# MAGIC     COALESCE(cust._tf_dim_customer_id, -9) AS _tf_dim_customer_id,
# MAGIC     COALESCE(geo._tf_dim_geography_id, -9) AS _tf_dim_geography_id,
# MAGIC
# MAGIC     --
# MAGIC     COALESCE(TRY_CAST(sod.order_qty AS SMALLINT), 0) AS sales_order_qty,
# MAGIC     COALESCE(TRY_CAST(sod.unit_price AS DECIMAL(19,4)), 0) AS sales_unit_price,
# MAGIC     COALESCE(TRY_CAST(sod.unit_price_discount AS DECIMAL(19,4)), 0) AS sales_unit_price_discount,
# MAGIC     COALESCE(TRY_CAST(sod.line_total AS DECIMAL(38, 6)), 0) AS sales_line_total
# MAGIC
# MAGIC   FROM silver.sales_order_detail sod
# MAGIC     LEFT OUTER JOIN silver.sales_order_header soh 
# MAGIC       ON sod.sales_order_id = soh.sales_order_id AND soh._tf_valid_to IS NULL
# MAGIC       LEFT OUTER JOIN silver.customer c 
# MAGIC         ON soh.customer_id = c.customer_id AND c._tf_valid_to IS NULL
# MAGIC         LEFT OUTER JOIN gold.dim_customer cust
# MAGIC           ON c.customer_id = cust.cust_customer_id
# MAGIC       LEFT OUTER JOIN silver.address a 
# MAGIC         ON soh.bill_to_address_id = a.address_id AND a._tf_valid_to IS NULL
# MAGIC         LEFT OUTER JOIN gold.dim_geography geo 
# MAGIC           ON a.address_id = geo.geo_address_id
# MAGIC   WHERE sod._tf_valid_to IS NULL;
# MAGIC
# MAGIC SELECT * FROM _tmp_fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.fact_sales AS tgt
# MAGIC USING _tmp_fact_sales AS src
# MAGIC ON tgt.sales_order_detail_id = src.sales_order_detail_id 
# MAGIC   AND tgt.sales_order_id = src.sales_order_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt._tf_dim_calendar_id != src._tf_dim_calendar_id OR
# MAGIC     tgt._tf_dim_customer_id != src._tf_dim_customer_id OR
# MAGIC     tgt._tf_dim_geography_id != src._tf_dim_geography_id OR
# MAGIC     tgt.sales_order_qty != src.sales_order_qty OR
# MAGIC     tgt.sales_unit_price != src.sales_unit_price OR
# MAGIC     tgt.sales_unit_price_discount != src.sales_unit_price_discount OR
# MAGIC     tgt.sales_line_total != src.sales_line_total
# MAGIC ) THEN 
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     _tf_dim_calendar_id = src._tf_dim_calendar_id,
# MAGIC     _tf_dim_customer_id = src._tf_dim_customer_id,
# MAGIC     _tf_dim_geography_id = src._tf_dim_geography_id,
# MAGIC     tgt.sales_order_qty = src.sales_order_qty,
# MAGIC     tgt.sales_unit_price = src.sales_unit_price,
# MAGIC     tgt.sales_unit_price_discount = src.sales_unit_price_discount,
# MAGIC     tgt.sales_line_total = src.sales_line_total,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     tgt.sales_order_id,
# MAGIC     tgt.sales_order_detail_id,
# MAGIC     tgt._tf_dim_calendar_id,
# MAGIC     tgt._tf_dim_customer_id,
# MAGIC     tgt._tf_dim_geography_id,
# MAGIC     tgt.sales_order_qty,
# MAGIC     tgt.sales_unit_price,
# MAGIC     tgt.sales_unit_price_discount,
# MAGIC     tgt.sales_line_total,
# MAGIC     tgt._tf_create_date,
# MAGIC     tgt._tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.sales_order_id,
# MAGIC     src.sales_order_detail_id,
# MAGIC     src._tf_dim_calendar_id,
# MAGIC     src._tf_dim_customer_id,
# MAGIC     src._tf_dim_geography_id,
# MAGIC     src.sales_order_qty,
# MAGIC     src.sales_unit_price,
# MAGIC     src.sales_unit_price_discount,
# MAGIC     src.sales_line_total,
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   );
