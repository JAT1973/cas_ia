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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW _tmp_fact_product AS
# MAGIC SELECT
# MAGIC     -- Product ID
# MAGIC     CAST(p.product_id AS INT) AS prod_product_id,
# MAGIC
# MAGIC     -- Calendar ID
# MAGIC     10000 * YEAR(p.modified_date) + 100 * MONTH(p.modified_date) + DAY(p.modified_date) AS _tf_dim_calendar_id,
# MAGIC
# MAGIC     -- Measures
# MAGIC     COALESCE(TRY_CAST(p.standard_cost AS DECIMAL(19, 4)), 0.00) AS product_standard_cost,
# MAGIC     COALESCE(TRY_CAST(p.list_price AS DECIMAL(19, 4)), 0.00) AS product_list_price,
# MAGIC     COALESCE(TRY_CAST(p.weight AS DECIMAL(19, 4)), 0.00) AS product_weight,
# MAGIC     NULL AS product_inventory_quantity,
# MAGIC     CASE WHEN p.discontinued_date IS NOT NULL THEN TRUE ELSE FALSE END AS product_discontinued
# MAGIC
# MAGIC   FROM silver.product p
# MAGIC   WHERE p._tf_valid_to IS NULL;
# MAGIC
# MAGIC -- Vérification de la vue temporaire
# MAGIC SELECT * FROM _tmp_fact_product;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.fact_product AS tgt
# MAGIC USING _tmp_fact_product AS src
# MAGIC ON tgt.product_id = src.prod_product_id 
# MAGIC    AND tgt._tf_dim_calendar_id = src._tf_dim_calendar_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.product_standard_cost != src.product_standard_cost OR
# MAGIC     tgt.product_list_price != src.product_list_price OR
# MAGIC     tgt.product_weight != src.product_weight OR
# MAGIC     tgt.product_inventory_quantity != src.product_inventory_quantity OR
# MAGIC     tgt.product_discontinued != src.product_discontinued
# MAGIC ) THEN 
# MAGIC
# MAGIC   UPDATE SET 
# MAGIC     tgt.product_standard_cost = src.product_standard_cost,
# MAGIC     tgt.product_list_price = src.product_list_price,
# MAGIC     tgt.product_weight = src.product_weight,
# MAGIC     tgt.product_inventory_quantity = src.product_inventory_quantity,
# MAGIC     tgt.product_discontinued = src.product_discontinued,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     _tf_dim_calendar_id,
# MAGIC     product_standard_cost,
# MAGIC     product_list_price,
# MAGIC     product_weight,
# MAGIC     product_inventory_quantity,
# MAGIC     product_discontinued,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.prod_product_id,
# MAGIC     src._tf_dim_calendar_id,
# MAGIC     src.product_standard_cost,
# MAGIC     src.product_list_price,
# MAGIC     src.product_weight,
# MAGIC     src.product_inventory_quantity,
# MAGIC     src.product_discontinued,
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW _tmp_fact_sales_detail AS
# MAGIC SELECT
# MAGIC     -- Identifiants de commande
# MAGIC     CAST(soh.sales_order_id AS INT) AS sales_order_id,
# MAGIC     CAST(sod.sales_order_detail_id AS INT) AS sales_order_detail_id,
# MAGIC
# MAGIC     -- Clés étrangères pour les dimensions
# MAGIC     10000 * YEAR(soh.order_date) + 100 * MONTH(soh.order_date) + DAY(soh.order_date) AS _tf_dim_calendar_id,
# MAGIC     COALESCE(cust._tf_dim_customer_id, -9) AS _tf_dim_customer_id,
# MAGIC     COALESCE(prod._tf_dim_product_id, -9) AS _tf_dim_product_id,
# MAGIC     COALESCE(geo._tf_dim_geography_id, -9) AS _tf_dim_geography_id,
# MAGIC
# MAGIC     -- Mesures
# MAGIC     COALESCE(TRY_CAST(sod.order_qty AS SMALLINT), 0) AS sales_order_qty,
# MAGIC     COALESCE(TRY_CAST(sod.unit_price AS DECIMAL(19, 4)), 0.00) AS sales_unit_price,
# MAGIC     COALESCE(TRY_CAST(sod.unit_price_discount AS DECIMAL(19, 4)), 0.00) AS sales_unit_price_discount,
# MAGIC     COALESCE(TRY_CAST(sod.line_total AS DECIMAL(38, 6)), 0.00) AS sales_line_total
# MAGIC
# MAGIC FROM silver.sales_order_detail sod
# MAGIC     LEFT OUTER JOIN silver.sales_order_header soh 
# MAGIC         ON sod.sales_order_id = soh.sales_order_id AND soh._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN silver.customer c 
# MAGIC         ON soh.customer_id = c.customer_id AND c._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN gold.dim_customer cust
# MAGIC         ON c.customer_id = cust.cust_customer_id
# MAGIC     LEFT OUTER JOIN silver.product p
# MAGIC         ON sod.product_id = p.product_id AND p._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN gold.dim_product prod
# MAGIC         ON p.product_id = prod.prod_product_id
# MAGIC     LEFT OUTER JOIN silver.address a 
# MAGIC         ON soh.bill_to_address_id = a.address_id AND a._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN gold.dim_geography geo
# MAGIC         ON a.address_id = geo.geo_address_id
# MAGIC WHERE sod._tf_valid_to IS NULL;
# MAGIC
# MAGIC -- Vérification des données
# MAGIC SELECT * FROM _tmp_fact_sales_detail;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.fact_sales_detail AS tgt
# MAGIC USING _tmp_fact_sales_detail AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id 
# MAGIC    AND tgt.sales_order_detail_id = src.sales_order_detail_id
# MAGIC
# MAGIC -- 1) Mise à jour des enregistrements existants si des différences sont détectées
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt._tf_dim_calendar_id != src._tf_dim_calendar_id OR
# MAGIC     tgt._tf_dim_customer_id != src._tf_dim_customer_id OR
# MAGIC     tgt._tf_dim_product_id != src._tf_dim_product_id OR
# MAGIC     tgt._tf_dim_geography_id != src._tf_dim_geography_id OR
# MAGIC     tgt.sales_order_qty != src.sales_order_qty OR
# MAGIC     tgt.sales_unit_price != src.sales_unit_price OR
# MAGIC     tgt.sales_unit_price_discount != src.sales_unit_price_discount OR
# MAGIC     tgt.sales_line_total != src.sales_line_total
# MAGIC ) THEN 
# MAGIC
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_dim_calendar_id = src._tf_dim_calendar_id,
# MAGIC     tgt._tf_dim_customer_id = src._tf_dim_customer_id,
# MAGIC     tgt._tf_dim_product_id = src._tf_dim_product_id,
# MAGIC     tgt._tf_dim_geography_id = src._tf_dim_geography_id,
# MAGIC     tgt.sales_order_qty = src.sales_order_qty,
# MAGIC     tgt.sales_unit_price = src.sales_unit_price,
# MAGIC     tgt.sales_unit_price_discount = src.sales_unit_price_discount,
# MAGIC     tgt.sales_line_total = src.sales_line_total,
# MAGIC     tgt._tf_update_date = current_timestamp()
# MAGIC
# MAGIC -- 2) Insertion des nouveaux enregistrements
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC   INSERT (
# MAGIC     sales_order_id,
# MAGIC     sales_order_detail_id,
# MAGIC     _tf_dim_calendar_id,
# MAGIC     _tf_dim_customer_id,
# MAGIC     _tf_dim_product_id,
# MAGIC     _tf_dim_geography_id,
# MAGIC     sales_order_qty,
# MAGIC     sales_unit_price,
# MAGIC     sales_unit_price_discount,
# MAGIC     sales_line_total,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.sales_order_id,
# MAGIC     src.sales_order_detail_id,
# MAGIC     src._tf_dim_calendar_id,
# MAGIC     src._tf_dim_customer_id,
# MAGIC     src._tf_dim_product_id,
# MAGIC     src._tf_dim_geography_id,
# MAGIC     src.sales_order_qty,
# MAGIC     src.sales_unit_price,
# MAGIC     src.sales_unit_price_discount,
# MAGIC     src.sales_line_total,
# MAGIC     current_timestamp(),  -- _tf_create_date
# MAGIC     current_timestamp()   -- _tf_update_date
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW _tmp_fact_shipping AS
# MAGIC SELECT
# MAGIC     -- Identifiants
# MAGIC     CAST(soh.sales_order_id AS INT) AS sales_order_id,
# MAGIC
# MAGIC     -- Clés étrangères pour les dimensions
# MAGIC     10000 * YEAR(soh.ship_date) + 100 * MONTH(soh.ship_date) + DAY(soh.ship_date) AS _tf_dim_calendar_id,
# MAGIC     COALESCE(addr._tf_dim_address_id, -9) AS _tf_dim_address_id,
# MAGIC     COALESCE(cust._tf_dim_customer_id, -9) AS _tf_dim_customer_id,
# MAGIC
# MAGIC     -- Mesures
# MAGIC     COALESCE(TRY_CAST(soh.freight AS DECIMAL(19, 4)), 0.00) AS shipping_freight,
# MAGIC     COALESCE(TRY_CAST(soh.tax_amt AS DECIMAL(19, 4)), 0.00) AS shipping_tax_amount,
# MAGIC     COALESCE(TRY_CAST(soh.freight + soh.tax_amt AS DECIMAL(19, 4)), 0.00) AS shipping_total,
# MAGIC     DATEDIFF(soh.ship_date, soh.order_date) AS shipping_duration_days
# MAGIC
# MAGIC FROM silver.sales_order_header soh
# MAGIC     LEFT OUTER JOIN silver.address a 
# MAGIC         ON soh.ship_to_address_id = a.address_id AND a._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN gold.dim_address addr
# MAGIC         ON a.address_id = addr.addr_address_id
# MAGIC     LEFT OUTER JOIN silver.customer c
# MAGIC         ON soh.customer_id = c.customer_id AND c._tf_valid_to IS NULL
# MAGIC     LEFT OUTER JOIN gold.dim_customer cust
# MAGIC         ON c.customer_id = cust.cust_customer_id
# MAGIC WHERE soh._tf_valid_to IS NULL;
# MAGIC
# MAGIC -- Vérification des données
# MAGIC SELECT * FROM _tmp_fact_shipping;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.fact_shipping AS tgt
# MAGIC USING _tmp_fact_shipping AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id
# MAGIC
# MAGIC -- 1) Mise à jour des enregistrements existants si des différences sont détectées
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt._tf_dim_calendar_id != src._tf_dim_calendar_id OR
# MAGIC     tgt._tf_dim_address_id != src._tf_dim_address_id OR
# MAGIC     tgt.shipping_freight != src.shipping_freight OR
# MAGIC     tgt.shipping_tax_amount != src.shipping_tax_amount OR
# MAGIC     tgt.shipping_total != src.shipping_total OR
# MAGIC     tgt.shipping_duration_days != src.shipping_duration_days
# MAGIC ) THEN 
# MAGIC
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_dim_calendar_id = src._tf_dim_calendar_id,
# MAGIC     tgt._tf_dim_address_id = src._tf_dim_address_id,
# MAGIC     tgt.shipping_freight = src.shipping_freight,
# MAGIC     tgt.shipping_tax_amount = src.shipping_tax_amount,
# MAGIC     tgt.shipping_total = src.shipping_total,
# MAGIC     tgt.shipping_duration_days = src.shipping_duration_days,
# MAGIC     tgt._tf_update_date = current_timestamp()
# MAGIC
# MAGIC -- 2) Insertion des nouveaux enregistrements
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC   INSERT (
# MAGIC     sales_order_id,
# MAGIC     _tf_dim_calendar_id,
# MAGIC     _tf_dim_address_id,
# MAGIC     shipping_freight,
# MAGIC     shipping_tax_amount,
# MAGIC     shipping_total,
# MAGIC     shipping_duration_days,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.sales_order_id,
# MAGIC     src._tf_dim_calendar_id,
# MAGIC     src._tf_dim_address_id,
# MAGIC     src.shipping_freight,
# MAGIC     src.shipping_tax_amount,
# MAGIC     src.shipping_total,
# MAGIC     src.shipping_duration_days,
# MAGIC     current_timestamp(),  -- _tf_create_date
# MAGIC     current_timestamp()   -- _tf_update_date
# MAGIC   );
# MAGIC
