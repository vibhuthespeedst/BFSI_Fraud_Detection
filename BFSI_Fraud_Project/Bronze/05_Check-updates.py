# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")  

# COMMAND ----------

spark.sql("SHOW TABLES IN `bfsi-project-team-3`.`bronze-tables`").show(truncate=False)

# COMMAND ----------

display(spark.table("`bfsi-project-team-3`.`bronze-tables`.`card_txn_bronze`").limit(5))
display(spark.table("`bfsi-project-team-3`.`bronze-tables`.`upi_txn_bronze`").limit(5))
display(spark.table("`bfsi-project-team-3`.`bronze-tables`.`customers_corebanking_bronze`").limit(5))
display(spark.table("`bfsi-project-team-3`.`bronze-tables`.`merchant_master_bronze`").limit(5))