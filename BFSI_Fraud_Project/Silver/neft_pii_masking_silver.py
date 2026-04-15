# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")

spark.sql("USE SCHEMA `bronze-tables`")

# COMMAND ----------

from pyspark.sql.functions import col, lit


# COMMAND ----------

df = spark.table("neft_rtgs_txn_bronze")

# COMMAND ----------

display(df)

# COMMAND ----------

print("Columns:", df.columns)

# COMMAND ----------

if "dbtr_nm" in df.columns:
    df = df.withColumn("dbtr_nm_masked", lit("REDACTED")).drop("dbtr_nm")

if "cdtr_nm" in df.columns:
    df = df.withColumn("cdtr_nm_masked", lit("REDACTED")).drop("cdtr_nm")


# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.`neft_rtgs_txn_silver`")

# COMMAND ----------

display(df)