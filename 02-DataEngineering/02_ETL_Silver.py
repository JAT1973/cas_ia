# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion in the Silver layer
# MAGIC
# MAGIC ## Connecting to the Silver layer (Target)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG jeromeattinger_lakehouse;
# MAGIC USE DATABASE silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE OR REPLACE load_date = current_timestamp();
# MAGIC VALUES load_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental load of address

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.address AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         AddressID       AS address_id,
# MAGIC         AddressLine1    AS address_line1,
# MAGIC         AddressLine2    AS address_line2,
# MAGIC         City            AS city,
# MAGIC         StateProvince   AS state_province,
# MAGIC         CountryRegion   AS country_region,
# MAGIC         PostalCode      AS postal_code,
# MAGIC         rowguid         AS rowguid,
# MAGIC         ModifiedDate    AS modified_date
# MAGIC     FROM bronze.address
# MAGIC ) AS src
# MAGIC ON tgt.address_id = src.address_id
# MAGIC   AND tgt._tf_valid_to IS NULL   -- Only match against 'active' records in silver
# MAGIC   
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.address_line1    != src.address_line1
# MAGIC     OR tgt.address_line2    != src.address_line2
# MAGIC     OR tgt.city             != src.city
# MAGIC     OR tgt.state_province   != src.state_province
# MAGIC     OR tgt.country_region   != src.country_region
# MAGIC     OR tgt.postal_code      != src.postal_code
# MAGIC     OR tgt.rowguid          != src.rowguid
# MAGIC     OR tgt.modified_date    != src.modified_date
# MAGIC     -- etc. for any columns you want to track changes on
# MAGIC ) AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 1) Close the old record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC   
# MAGIC WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 2) Close the deleted record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.address AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         AddressID       AS address_id,
# MAGIC         AddressLine1    AS address_line1,
# MAGIC         AddressLine2    AS address_line2,
# MAGIC         City            AS city,
# MAGIC         StateProvince   AS state_province,
# MAGIC         CountryRegion   AS country_region,
# MAGIC         PostalCode      AS postal_code,
# MAGIC         rowguid         AS rowguid,
# MAGIC         ModifiedDate    AS modified_date
# MAGIC     FROM bronze.address
# MAGIC ) AS src
# MAGIC ON tgt.address_id = src.address_id
# MAGIC   AND tgt._tf_valid_to IS NULL   -- Only match against 'active' records in silver
# MAGIC   
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- 3) Insert NEW records (either truly new address_id or a new version if the old one was just closed)
# MAGIC   INSERT (
# MAGIC     address_id,
# MAGIC     address_line1,
# MAGIC     address_line2,
# MAGIC     city,
# MAGIC     state_province,
# MAGIC     country_region,
# MAGIC     postal_code,
# MAGIC     rowguid,
# MAGIC     modified_date,
# MAGIC     _tf_valid_from,
# MAGIC     _tf_valid_to,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.address_id,
# MAGIC     src.address_line1,
# MAGIC     src.address_line2,
# MAGIC     src.city,
# MAGIC     src.state_province,
# MAGIC     src.country_region,
# MAGIC     src.postal_code,
# MAGIC     src.rowguid,
# MAGIC     src.modified_date,
# MAGIC     load_date,        -- _tf_valid_from
# MAGIC     NULL,             -- _tf_valid_to
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental load of customer 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.customer AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CustomerID       AS customer_id,
# MAGIC         NameStyle        AS name_style,
# MAGIC         Title            AS title,
# MAGIC         FirstName        AS first_name,
# MAGIC         MiddleName       AS middle_name,
# MAGIC         LastName         AS last_name,
# MAGIC         Suffix           AS suffix,
# MAGIC         CompanyName      AS company_name,
# MAGIC         SalesPerson      AS sales_person,
# MAGIC         EmailAddress     AS email_address,
# MAGIC         Phone            AS phone,
# MAGIC         PasswordHash     AS password_hash,
# MAGIC         PasswordSalt     AS password_salt,
# MAGIC         rowguid          AS rowguid,
# MAGIC         ModifiedDate     AS modified_date
# MAGIC     FROM bronze.customer
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.name_style        != src.name_style
# MAGIC     OR tgt.title             != src.title
# MAGIC     OR tgt.first_name        != src.first_name
# MAGIC     OR tgt.middle_name       != src.middle_name
# MAGIC     OR tgt.last_name         != src.last_name
# MAGIC     OR tgt.suffix            != src.suffix
# MAGIC     OR tgt.company_name      != src.company_name
# MAGIC     OR tgt.sales_person      != src.sales_person
# MAGIC     OR tgt.email_address     != src.email_address
# MAGIC     OR tgt.phone             != src.phone
# MAGIC     OR tgt.password_hash     != src.password_hash
# MAGIC     OR tgt.password_salt     != src.password_salt
# MAGIC     OR tgt.rowguid           != src.rowguid
# MAGIC     OR tgt.modified_date     != src.modified_date
# MAGIC ) AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 1) Close the old record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 2) Close the deleted record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.customer AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CustomerID       AS customer_id,
# MAGIC         NameStyle        AS name_style,
# MAGIC         Title            AS title,
# MAGIC         FirstName        AS first_name,
# MAGIC         MiddleName       AS middle_name,
# MAGIC         LastName         AS last_name,
# MAGIC         Suffix           AS suffix,
# MAGIC         CompanyName      AS company_name,
# MAGIC         SalesPerson      AS sales_person,
# MAGIC         EmailAddress     AS email_address,
# MAGIC         Phone            AS phone,
# MAGIC         PasswordHash     AS password_hash,
# MAGIC         PasswordSalt     AS password_salt,
# MAGIC         rowguid          AS rowguid,
# MAGIC         ModifiedDate     AS modified_date
# MAGIC     FROM bronze.customer
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- 3) Insert NEW records (new customer_id or new version of existing record)
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     name_style,
# MAGIC     title,
# MAGIC     first_name,
# MAGIC     middle_name,
# MAGIC     last_name,
# MAGIC     suffix,
# MAGIC     company_name,
# MAGIC     sales_person,
# MAGIC     email_address,
# MAGIC     phone,
# MAGIC     password_hash,
# MAGIC     password_salt,
# MAGIC     rowguid,
# MAGIC     modified_date,
# MAGIC     _tf_valid_from,
# MAGIC     _tf_valid_to,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.customer_id,
# MAGIC     src.name_style,
# MAGIC     src.title,
# MAGIC     src.first_name,
# MAGIC     src.middle_name,
# MAGIC     src.last_name,
# MAGIC     src.suffix,
# MAGIC     src.company_name,
# MAGIC     src.sales_person,
# MAGIC     src.email_address,
# MAGIC     src.phone,
# MAGIC     src.password_hash,
# MAGIC     src.password_salt,
# MAGIC     src.rowguid,
# MAGIC     src.modified_date,
# MAGIC     load_date,        -- _tf_valid_from
# MAGIC     NULL,             -- _tf_valid_to
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental load of sales_order_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.sales_order_detail AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         SalesOrderID          AS sales_order_id,
# MAGIC         SalesOrderDetailID    AS sales_order_detail_id,
# MAGIC         OrderQty              AS order_qty,
# MAGIC         ProductID             AS product_id,
# MAGIC         UnitPrice             AS unit_price,
# MAGIC         UnitPriceDiscount     AS unit_price_discount,
# MAGIC         LineTotal             AS line_total,
# MAGIC         rowguid               AS rowguid,
# MAGIC         ModifiedDate          AS modified_date
# MAGIC     FROM bronze.salesorderdetail
# MAGIC ) AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id
# MAGIC    AND tgt.sales_order_detail_id = src.sales_order_detail_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.order_qty           != src.order_qty
# MAGIC     OR tgt.product_id          != src.product_id
# MAGIC     OR tgt.unit_price          != src.unit_price
# MAGIC     OR tgt.unit_price_discount != src.unit_price_discount
# MAGIC     OR tgt.line_total          != src.line_total
# MAGIC     OR tgt.rowguid             != src.rowguid
# MAGIC     OR tgt.modified_date       != src.modified_date
# MAGIC     -- etc. for any additional columns to track changes
# MAGIC ) AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 1) Close the old record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 2) Close the deleted record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.sales_order_detail AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         SalesOrderID          AS sales_order_id,
# MAGIC         SalesOrderDetailID    AS sales_order_detail_id,
# MAGIC         OrderQty              AS order_qty,
# MAGIC         ProductID             AS product_id,
# MAGIC         UnitPrice             AS unit_price,
# MAGIC         UnitPriceDiscount     AS unit_price_discount,
# MAGIC         LineTotal             AS line_total,
# MAGIC         rowguid               AS rowguid,
# MAGIC         ModifiedDate          AS modified_date
# MAGIC     FROM bronze.salesorderdetail
# MAGIC ) AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id
# MAGIC    AND tgt.sales_order_detail_id = src.sales_order_detail_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- 3) Insert NEW records (new sales_order_id or new version of existing record)
# MAGIC   INSERT (
# MAGIC     sales_order_id,
# MAGIC     sales_order_detail_id,
# MAGIC     order_qty,
# MAGIC     product_id,
# MAGIC     unit_price,
# MAGIC     unit_price_discount,
# MAGIC     line_total,
# MAGIC     rowguid,
# MAGIC     modified_date,
# MAGIC     _tf_valid_from,
# MAGIC     _tf_valid_to,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.sales_order_id,
# MAGIC     src.sales_order_detail_id,
# MAGIC     src.order_qty,
# MAGIC     src.product_id,
# MAGIC     src.unit_price,
# MAGIC     src.unit_price_discount,
# MAGIC     src.line_total,
# MAGIC     src.rowguid,
# MAGIC     src.modified_date,
# MAGIC     load_date,        -- _tf_valid_from
# MAGIC     NULL,             -- _tf_valid_to
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental load of sales_order_header

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.sales_order_header AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         SalesOrderID          AS sales_order_id,
# MAGIC         RevisionNumber        AS revision_number,
# MAGIC         OrderDate             AS order_date,
# MAGIC         DueDate               AS due_date,
# MAGIC         ShipDate              AS ship_date,
# MAGIC         Status                AS status,
# MAGIC         OnlineOrderFlag       AS online_order_flag,
# MAGIC         SalesOrderNumber      AS sales_order_number,
# MAGIC         PurchaseOrderNumber   AS purchase_order_number,
# MAGIC         AccountNumber         AS account_number,
# MAGIC         CustomerID            AS customer_id,
# MAGIC         ShipToAddressID       AS ship_to_address_id,
# MAGIC         BillToAddressID       AS bill_to_address_id,
# MAGIC         ShipMethod            AS ship_method,
# MAGIC         CreditCardApprovalCode AS credit_card_approval_code,
# MAGIC         SubTotal              AS sub_total,
# MAGIC         TaxAmt                AS tax_amt,
# MAGIC         Freight               AS freight,
# MAGIC         TotalDue              AS total_due,
# MAGIC         Comment               AS comment,
# MAGIC         rowguid               AS rowguid,
# MAGIC         ModifiedDate          AS modified_date
# MAGIC     FROM bronze.salesorderheader
# MAGIC ) AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.revision_number        != src.revision_number
# MAGIC     OR tgt.order_date             != src.order_date
# MAGIC     OR tgt.due_date               != src.due_date
# MAGIC     OR tgt.ship_date              != src.ship_date
# MAGIC     OR tgt.status                 != src.status
# MAGIC     OR tgt.online_order_flag      != src.online_order_flag
# MAGIC     OR tgt.sales_order_number     != src.sales_order_number
# MAGIC     OR tgt.purchase_order_number  != src.purchase_order_number
# MAGIC     OR tgt.account_number         != src.account_number
# MAGIC     OR tgt.customer_id            != src.customer_id
# MAGIC     OR tgt.ship_to_address_id     != src.ship_to_address_id
# MAGIC     OR tgt.bill_to_address_id     != src.bill_to_address_id
# MAGIC     OR tgt.ship_method            != src.ship_method
# MAGIC     OR tgt.credit_card_approval_code != src.credit_card_approval_code
# MAGIC     OR tgt.sub_total              != src.sub_total
# MAGIC     OR tgt.tax_amt                != src.tax_amt
# MAGIC     OR tgt.freight                != src.freight
# MAGIC     OR tgt.total_due              != src.total_due
# MAGIC     OR tgt.comment                != src.comment
# MAGIC     OR tgt.rowguid                != src.rowguid
# MAGIC     OR tgt.modified_date          != src.modified_date
# MAGIC ) AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 1) Close the old record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE AND tgt._tf_valid_to IS NULL THEN
# MAGIC   -- 2) Close the deleted record by setting _tf_valid_to
# MAGIC   UPDATE SET 
# MAGIC     tgt._tf_valid_to    = load_date,
# MAGIC     tgt._tf_update_date = load_date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.sales_order_header AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         SalesOrderID          AS sales_order_id,
# MAGIC         RevisionNumber        AS revision_number,
# MAGIC         OrderDate             AS order_date,
# MAGIC         DueDate               AS due_date,
# MAGIC         ShipDate              AS ship_date,
# MAGIC         Status                AS status,
# MAGIC         OnlineOrderFlag       AS online_order_flag,
# MAGIC         SalesOrderNumber      AS sales_order_number,
# MAGIC         PurchaseOrderNumber   AS purchase_order_number,
# MAGIC         AccountNumber         AS account_number,
# MAGIC         CustomerID            AS customer_id,
# MAGIC         ShipToAddressID       AS ship_to_address_id,
# MAGIC         BillToAddressID       AS bill_to_address_id,
# MAGIC         ShipMethod            AS ship_method,
# MAGIC         CreditCardApprovalCode AS credit_card_approval_code,
# MAGIC         SubTotal              AS sub_total,
# MAGIC         TaxAmt                AS tax_amt,
# MAGIC         Freight               AS freight,
# MAGIC         TotalDue              AS total_due,
# MAGIC         Comment               AS comment,
# MAGIC         rowguid               AS rowguid,
# MAGIC         ModifiedDate          AS modified_date
# MAGIC     FROM bronze.salesorderheader
# MAGIC ) AS src
# MAGIC ON tgt.sales_order_id = src.sales_order_id
# MAGIC    AND tgt._tf_valid_to IS NULL  -- Only match against 'active' records in silver
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- 3) Insert NEW records (new sales_order_id or new version of existing record)
# MAGIC   INSERT (
# MAGIC     sales_order_id,
# MAGIC     revision_number,
# MAGIC     order_date,
# MAGIC     due_date,
# MAGIC     ship_date,
# MAGIC     status,
# MAGIC     online_order_flag,
# MAGIC     sales_order_number,
# MAGIC     purchase_order_number,
# MAGIC     account_number,
# MAGIC     customer_id,
# MAGIC     ship_to_address_id,
# MAGIC     bill_to_address_id,
# MAGIC     ship_method,
# MAGIC     credit_card_approval_code,
# MAGIC     sub_total,
# MAGIC     tax_amt,
# MAGIC     freight,
# MAGIC     total_due,
# MAGIC     comment,
# MAGIC     rowguid,
# MAGIC     modified_date,
# MAGIC     _tf_valid_from,
# MAGIC     _tf_valid_to,
# MAGIC     _tf_create_date,
# MAGIC     _tf_update_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.sales_order_id,
# MAGIC     src.revision_number,
# MAGIC     src.order_date,
# MAGIC     src.due_date,
# MAGIC     src.ship_date,
# MAGIC     src.status,
# MAGIC     src.online_order_flag,
# MAGIC     src.sales_order_number,
# MAGIC     src.purchase_order_number,
# MAGIC     src.account_number,
# MAGIC     src.customer_id,
# MAGIC     src.ship_to_address_id,
# MAGIC     src.bill_to_address_id,
# MAGIC     src.ship_method,
# MAGIC     src.credit_card_approval_code,
# MAGIC     src.sub_total,
# MAGIC     src.tax_amt,
# MAGIC     src.freight,
# MAGIC     src.total_due,
# MAGIC     src.comment,
# MAGIC     src.rowguid,
# MAGIC     src.modified_date,
# MAGIC     load_date,        -- _tf_valid_from
# MAGIC     NULL,             -- _tf_valid_to
# MAGIC     load_date,        -- _tf_create_date
# MAGIC     load_date         -- _tf_update_date
# MAGIC   )
