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
