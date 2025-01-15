# Databricks notebook source
# MAGIC %md
# MAGIC # Loading the Dim tables in the Gold layer 
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
# MAGIC MERGE INTO gold.dim_geography AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CAST(address_id AS INT) AS geo_address_id,
# MAGIC         COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS geo_address_line_1,
# MAGIC         COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS geo_address_line_2,
# MAGIC         COALESCE(TRY_CAST(city AS STRING), 'N/A') AS geo_city,
# MAGIC         COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS geo_state_province,
# MAGIC         COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS geo_country_region,
# MAGIC         COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS geo_postal_code
# MAGIC     FROM silver.address
# MAGIC     WHERE _tf_valid_to IS NULL
# MAGIC ) AS src
# MAGIC ON tgt.geo_address_id = src.geo_address_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.geo_address_line_1 != src.geo_address_line_1 OR 
# MAGIC     tgt.geo_address_line_2 != src.geo_address_line_2 OR 
# MAGIC     tgt.geo_city != src.geo_city OR
# MAGIC     tgt.geo_state_province != src.geo_state_province OR
# MAGIC     tgt.geo_country_region != src.geo_country_region OR
# MAGIC     tgt.geo_postal_code != src.geo_postal_code
# MAGIC ) THEN 
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     tgt.geo_address_line_1 = src.geo_address_line_1,
# MAGIC     tgt.geo_address_line_2 = src.geo_address_line_2,
# MAGIC     tgt.geo_city = src.geo_city,
# MAGIC     tgt.geo_state_province = src.geo_state_province,
# MAGIC     tgt.geo_country_region = src.geo_country_region,
# MAGIC     tgt.geo_postal_code = src.geo_postal_code,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     geo_address_id,
# MAGIC     geo_address_line_1,
# MAGIC     geo_address_line_2,
# MAGIC     geo_city,
# MAGIC     geo_state_province,
# MAGIC     geo_country_region,
# MAGIC     geo_postal_code,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.geo_address_id,
# MAGIC     src.geo_address_line_1,
# MAGIC     src.geo_address_line_2,
# MAGIC     src.geo_city,
# MAGIC     src.geo_state_province,
# MAGIC     src.geo_country_region,
# MAGIC     src.geo_postal_code,
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_customer AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CAST(customer_id AS INT) AS cust_customer_id,
# MAGIC         COALESCE(TRY_CAST(title AS STRING), 'N/A') AS cust_title,
# MAGIC         COALESCE(TRY_CAST(first_name AS STRING), 'N/A') AS cust_first_name,
# MAGIC         COALESCE(TRY_CAST(middle_name AS STRING), 'N/A') AS cust_middle_name,
# MAGIC         COALESCE(TRY_CAST(last_name AS STRING), 'N/A') AS cust_last_name,
# MAGIC         COALESCE(TRY_CAST(suffix AS STRING), 'N/A') AS cust_suffix,
# MAGIC         COALESCE(TRY_CAST(company_name AS STRING), 'N/A') AS cust_company_name,
# MAGIC         COALESCE(TRY_CAST(sales_person AS STRING), 'N/A') AS cust_sales_person,
# MAGIC         COALESCE(TRY_CAST(email_address AS STRING), 'N/A') AS cust_email_address,
# MAGIC         COALESCE(TRY_CAST(phone AS STRING), 'N/A') AS cust_phone
# MAGIC     FROM silver.customer
# MAGIC     WHERE _tf_valid_to IS NULL
# MAGIC ) AS src
# MAGIC ON tgt.cust_customer_id = src.cust_customer_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.cust_title != src.cust_title OR
# MAGIC     tgt.cust_first_name != src.cust_first_name OR
# MAGIC     tgt.cust_middle_name != src.cust_middle_name OR
# MAGIC     tgt.cust_last_name != src.cust_last_name OR
# MAGIC     tgt.cust_suffix != src.cust_suffix OR
# MAGIC     tgt.cust_company_name != src.cust_company_name OR
# MAGIC     tgt.cust_sales_person != src.cust_sales_person OR
# MAGIC     tgt.cust_email_address != src.cust_email_address OR
# MAGIC     tgt.cust_phone != src.cust_phone
# MAGIC ) THEN 
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     tgt.cust_title = src.cust_title,
# MAGIC     tgt.cust_first_name = src.cust_first_name,
# MAGIC     tgt.cust_middle_name = src.cust_middle_name,
# MAGIC     tgt.cust_last_name = src.cust_last_name,
# MAGIC     tgt.cust_suffix = src.cust_suffix,
# MAGIC     tgt.cust_company_name = src.cust_company_name,
# MAGIC     tgt.cust_sales_person = src.cust_sales_person,
# MAGIC     tgt.cust_email_address = src.cust_email_address,
# MAGIC     tgt.cust_phone = src.cust_phone,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     cust_customer_id,
# MAGIC     cust_title,
# MAGIC     cust_first_name,
# MAGIC     cust_middle_name,
# MAGIC     cust_last_name,
# MAGIC     cust_suffix,
# MAGIC     cust_company_name,
# MAGIC     cust_sales_person,
# MAGIC     cust_email_address,
# MAGIC     cust_phone,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.cust_customer_id,
# MAGIC     src.cust_title,
# MAGIC     src.cust_first_name,
# MAGIC     src.cust_middle_name,
# MAGIC     src.cust_last_name,
# MAGIC     src.cust_suffix,
# MAGIC     src.cust_company_name,
# MAGIC     src.cust_sales_person,
# MAGIC     src.cust_email_address,
# MAGIC     src.cust_phone,
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_address AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CAST(address_id AS INT) AS addr_address_id,
# MAGIC         COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS addr_line1,
# MAGIC         COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS addr_line2,
# MAGIC         COALESCE(TRY_CAST(city AS STRING), 'N/A') AS addr_city,
# MAGIC         COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS addr_state_province,
# MAGIC         COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS addr_country_region,
# MAGIC         COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS addr_postal_code
# MAGIC     FROM silver.address
# MAGIC     WHERE _tf_valid_to IS NULL
# MAGIC ) AS src
# MAGIC ON tgt.addr_address_id = src.addr_address_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.addr_line1 != src.addr_line1 OR 
# MAGIC     tgt.addr_line2 != src.addr_line2 OR 
# MAGIC     tgt.addr_city != src.addr_city OR
# MAGIC     tgt.addr_state_province != src.addr_state_province OR
# MAGIC     tgt.addr_country_region != src.addr_country_region OR
# MAGIC     tgt.addr_postal_code != src.addr_postal_code
# MAGIC ) THEN 
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     tgt.addr_line1 = src.addr_line1,
# MAGIC     tgt.addr_line2 = src.addr_line2,
# MAGIC     tgt.addr_city = src.addr_city,
# MAGIC     tgt.addr_state_province = src.addr_state_province,
# MAGIC     tgt.addr_country_region = src.addr_country_region,
# MAGIC     tgt.addr_postal_code = src.addr_postal_code,
# MAGIC     tgt._tf_update_date = current_timestamp()
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     addr_address_id,
# MAGIC     addr_line1,
# MAGIC     addr_line2,
# MAGIC     addr_city,
# MAGIC     addr_state_province,
# MAGIC     addr_country_region,
# MAGIC     addr_postal_code,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.addr_address_id,
# MAGIC     src.addr_line1,
# MAGIC     src.addr_line2,
# MAGIC     src.addr_city,
# MAGIC     src.addr_state_province,
# MAGIC     src.addr_country_region,
# MAGIC     src.addr_postal_code,
# MAGIC     current_timestamp(),  -- _tf_create_date
# MAGIC     current_timestamp()   -- _tf_update_date
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_order AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CAST(sales_order_id AS INT) AS order_order_id,
# MAGIC         TRY_CAST(order_date AS DATE) AS order_date,
# MAGIC         TRY_CAST(due_date AS DATE) AS order_due_date,
# MAGIC         TRY_CAST(ship_date AS DATE) AS order_ship_date,
# MAGIC         COALESCE(TRY_CAST(status AS STRING), 'N/A') AS order_status,
# MAGIC         COALESCE(TRY_CAST(online_order_flag AS BOOLEAN), FALSE) AS order_online_flag,
# MAGIC         COALESCE(TRY_CAST(ship_method AS STRING), 'N/A') AS order_ship_method,
# MAGIC         COALESCE(TRY_CAST(tax_amt AS DECIMAL(19, 4)), 0.00) AS order_tax_amount,
# MAGIC         COALESCE(TRY_CAST(freight AS DECIMAL(19, 4)), 0.00) AS order_freight,
# MAGIC         COALESCE(TRY_CAST(sub_total AS DECIMAL(19, 4)), 0.00) AS order_subtotal,
# MAGIC         COALESCE(TRY_CAST(total_due AS DECIMAL(19, 4)), 0.00) AS order_total_due
# MAGIC     FROM silver.sales_order_header
# MAGIC     WHERE _tf_valid_to IS NULL
# MAGIC ) AS src
# MAGIC ON tgt.order_order_id = src.order_order_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.order_date != src.order_date OR
# MAGIC     tgt.order_due_date != src.order_due_date OR
# MAGIC     tgt.order_ship_date != src.order_ship_date OR
# MAGIC     tgt.order_status != src.order_status OR
# MAGIC     tgt.order_online_flag != src.order_online_flag OR
# MAGIC     tgt.order_ship_method != src.order_ship_method OR
# MAGIC     tgt.order_tax_amount != src.order_tax_amount OR
# MAGIC     tgt.order_freight != src.order_freight OR
# MAGIC     tgt.order_subtotal != src.order_subtotal OR
# MAGIC     tgt.order_total_due != src.order_total_due
# MAGIC ) THEN 
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     tgt.order_date = src.order_date,
# MAGIC     tgt.order_due_date = src.order_due_date,
# MAGIC     tgt.order_ship_date = src.order_ship_date,
# MAGIC     tgt.order_status = src.order_status,
# MAGIC     tgt.order_online_flag = src.order_online_flag,
# MAGIC     tgt.order_ship_method = src.order_ship_method,
# MAGIC     tgt.order_tax_amount = src.order_tax_amount,
# MAGIC     tgt.order_freight = src.order_freight,
# MAGIC     tgt.order_subtotal = src.order_subtotal,
# MAGIC     tgt.order_total_due = src.order_total_due,
# MAGIC     tgt._tf_update_date = current_timestamp()
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     order_order_id,
# MAGIC     order_date,
# MAGIC     order_due_date,
# MAGIC     order_ship_date,
# MAGIC     order_status,
# MAGIC     order_online_flag,
# MAGIC     order_ship_method,
# MAGIC     order_tax_amount,
# MAGIC     order_freight,
# MAGIC     order_subtotal,
# MAGIC     order_total_due,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.order_order_id,
# MAGIC     src.order_date,
# MAGIC     src.order_due_date,
# MAGIC     src.order_ship_date,
# MAGIC     src.order_status,
# MAGIC     src.order_online_flag,
# MAGIC     src.order_ship_method,
# MAGIC     src.order_tax_amount,
# MAGIC     src.order_freight,
# MAGIC     src.order_subtotal,
# MAGIC     src.order_total_due,
# MAGIC     current_timestamp(),  -- _tf_create_date
# MAGIC     current_timestamp()   -- _tf_update_date
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_product AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CAST(p.product_id AS INT) AS prod_product_id,
# MAGIC         COALESCE(TRY_CAST(p.name AS STRING), 'N/A') AS prod_name,
# MAGIC         COALESCE(TRY_CAST(p.product_number AS STRING), 'N/A') AS prod_product_number,
# MAGIC         COALESCE(TRY_CAST(p.color AS STRING), 'N/A') AS prod_color,
# MAGIC         COALESCE(TRY_CAST(p.size AS STRING), 'N/A') AS prod_size,
# MAGIC         COALESCE(TRY_CAST(p.standard_cost AS DECIMAL(19, 4)), 0.00) AS prod_standard_cost,
# MAGIC         COALESCE(TRY_CAST(p.list_price AS DECIMAL(19, 4)), 0.00) AS prod_list_price,
# MAGIC         COALESCE(TRY_CAST(p.weight AS DECIMAL(19, 4)), 0.00) AS prod_weight,
# MAGIC         COALESCE(TRY_CAST(pm.name AS STRING), 'N/A') AS prod_model_name, -- Nom du modèle depuis ProductModel
# MAGIC         COALESCE(TRY_CAST(pc.name AS STRING), 'N/A') AS prod_category_name, -- Nom de la catégorie depuis ProductCategory
# MAGIC         COALESCE(TRY_CAST(p.thumbnail_photo AS STRING), 'N/A') AS prod_thumbnail_photo,
# MAGIC         COALESCE(TRY_CAST(pd.description AS STRING), 'N/A') AS prod_description -- Description depuis ProductDescription
# MAGIC     FROM silver.product AS p
# MAGIC     LEFT JOIN silver.productmodel AS pm ON p.product_model_id = pm.product_model_id
# MAGIC     LEFT JOIN silver.productcategory AS pc ON p.product_category_id = pc.product_category_id
# MAGIC     LEFT JOIN silver.productmodelproductdescription AS pmpd ON p.product_model_id = pmpd.product_model_id
# MAGIC     LEFT JOIN silver.productdescription AS pd ON pmpd.product_description_id = pd.product_description_id
# MAGIC     WHERE pmpd.culture = 'en' -- Filtrer pour les descriptions en anglais
# MAGIC       AND p._tf_valid_to IS NULL
# MAGIC ) AS src
# MAGIC ON tgt.prod_product_id = src.prod_product_id
# MAGIC
# MAGIC -- 1) Update existing records when a difference is detected
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.prod_name != src.prod_name OR
# MAGIC     tgt.prod_product_number != src.prod_product_number OR
# MAGIC     tgt.prod_color != src.prod_color OR
# MAGIC     tgt.prod_size != src.prod_size OR
# MAGIC     tgt.prod_standard_cost != src.prod_standard_cost OR
# MAGIC     tgt.prod_list_price != src.prod_list_price OR
# MAGIC     tgt.prod_weight != src.prod_weight OR
# MAGIC     tgt.prod_model_name != src.prod_model_name OR
# MAGIC     tgt.prod_category_name != src.prod_category_name OR
# MAGIC     tgt.prod_thumbnail_photo != src.prod_thumbnail_photo OR
# MAGIC     tgt.prod_description != src.prod_description
# MAGIC ) THEN
# MAGIC   
# MAGIC   UPDATE SET 
# MAGIC     tgt.prod_name = src.prod_name,
# MAGIC     tgt.prod_product_number = src.prod_product_number,
# MAGIC     tgt.prod_color = src.prod_color,
# MAGIC     tgt.prod_size = src.prod_size,
# MAGIC     tgt.prod_standard_cost = src.prod_standard_cost,
# MAGIC     tgt.prod_list_price = src.prod_list_price,
# MAGIC     tgt.prod_weight = src.prod_weight,
# MAGIC     tgt.prod_model_name = src.prod_model_name,
# MAGIC     tgt.prod_category_name = src.prod_category_name,
# MAGIC     tgt.prod_thumbnail_photo = src.prod_thumbnail_photo,
# MAGIC     tgt.prod_description = src.prod_description,
# MAGIC     tgt._tf_update_date = current_timestamp()
# MAGIC
# MAGIC -- 2) Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   
# MAGIC   INSERT (
# MAGIC     prod_product_id,
# MAGIC     prod_name,
# MAGIC     prod_product_number,
# MAGIC     prod_color,
# MAGIC     prod_size,
# MAGIC     prod_standard_cost,
# MAGIC     prod_list_price,
# MAGIC     prod_weight,
# MAGIC     prod_model_name,
# MAGIC     prod_category_name,
# MAGIC     prod_thumbnail_photo,
# MAGIC     prod_description,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.prod_product_id,
# MAGIC     src.prod_name,
# MAGIC     src.prod_product_number,
# MAGIC     src.prod_color,
# MAGIC     src.prod_size,
# MAGIC     src.prod_standard_cost,
# MAGIC     src.prod_list_price,
# MAGIC     src.prod_weight,
# MAGIC     src.prod_model_name,
# MAGIC     src.prod_category_name,
# MAGIC     src.prod_thumbnail_photo,
# MAGIC     src.prod_description,
# MAGIC     current_timestamp(),  -- _tf_create_date
# MAGIC     current_timestamp()   -- _tf_update_date
# MAGIC   );
# MAGIC
