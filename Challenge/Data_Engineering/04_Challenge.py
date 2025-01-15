# Databricks notebook source
# MAGIC %md
# MAGIC # Code to test the SCD2 in Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG jeromeattinger_lakehouse;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate an UPDATE in source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM salesorderdetail WHERE SalesOrderDetailID IN ('110563','110640');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE salesorderdetail SET OrderQty = 10, LineTotal = 10 * UnitPrice WHERE SalesOrderDetailID IN ('110563', '110640'); 

# COMMAND ----------

# MAGIC %md
# MAGIC ## !!! Run le Notebook 02_Challenge qui va reload la base de données silver

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA silver;
# MAGIC SELECT * FROM  silver.sales_order_detail WHERE sales_order_detail_id IN ('110563','110640');
