# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("reading sql database").getOrCreate()
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

df = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Customer", properties=connectionProperties)
display(df)
