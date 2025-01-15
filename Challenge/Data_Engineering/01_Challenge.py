# Databricks notebook source
# MAGIC %md
# MAGIC # Compléter la couche Bronze:
# MAGIC
# MAGIC ## Connecting to the bronze layer (Target)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG jeromeattinger_lakehouse;
# MAGIC --DROP DATABASE IF EXISTS jeromeattinger_lakehouse.bronze CASCADE;
# MAGIC --CREATE DATABASE IF NOT EXISTS jeromeattinger_lakehouse.bronze;
# MAGIC USE DATABASE bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the JDBC connection to the Azure SQL Database (Source)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("loading bronze layer").getOrCreate()
jdbcHostname = "sql-datasource-dev-001.database.windows.net"
jdbcDatabase = "sqldb-adventureworks-dev-001"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
    "user" : "levkiwi-admin",
    "password" : "cas-ia2024",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of All Tables

# COMMAND ----------

# Étape 1 : Récupérer la liste des tables avec leurs schémas
metadata_query = """
(
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT IN ('ErrorLog', 'BuildVersion')
) AS metadata
"""
tables_df = spark.read.jdbc(url=jdbcUrl, table=metadata_query, properties=connectionProperties)
tables = [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in tables_df.collect()]

print("Tables détectées :", tables)

# Étape 2 : Charger chaque table et sauvegarder dans le schéma 'bronze'
dataframes = {}
for schema, table in tables:
    print(f"Chargement de la table : {schema}.{table}")
    
    # Charger la table
    df = spark.read.jdbc(
        url=jdbcUrl,
        table=f"{schema}.{table}",
        properties=connectionProperties
    )
    dataframes[table] = df

    # Nom de la table dans le schéma bronze
    bronze_table_name = f"bronze.{table}"
    df.write.mode("overwrite").saveAsTable(bronze_table_name)
    print(f"Table {bronze_table_name} chargée avec succès !")

