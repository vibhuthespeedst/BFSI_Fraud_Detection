# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")  

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

customers_df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`customers_corebanking`")

customers_bronze_df = (
    customers_df
    .withColumn("_source_system", lit("CORE_BANKING"))
    .withColumn("_load_ts", current_timestamp())
    .withColumn("_file_name", lit("customers_corebanking.csv"))
    .withColumn("_batch_id", lit("batch_001"))
)

customers_bronze_df.write.format("delta").mode("overwrite").saveAsTable("`bfsi-project-team-3`.`bronze-tables`.`customers_corebanking_bronze`")

# COMMAND ----------

merchant_df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`merchant_master`")

merchant_bronze_df = (
    merchant_df
    .withColumn("_source_system", lit("MERCHANT"))
    .withColumn("_load_ts", current_timestamp())
    .withColumn("_file_name", lit("merchant_master.csv"))
    .withColumn("_batch_id", lit("batch_001"))
)

merchant_bronze_df.write.format("delta").mode("overwrite").saveAsTable("`bfsi-project-team-3`.`bronze-tables`.`merchant_master_bronze`")