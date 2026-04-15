# Databricks notebook source

from pyspark.sql.functions import col, trim, upper

df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`merchant_master_bronze`")


# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

if "merchant_name" in df.columns:
    df = df.withColumn("merchant_name", trim(col("merchant_name")))

if "merchant_category" in df.columns:
    df = df.withColumn("merchant_category", upper(trim(col("merchant_category"))))


if "merchant_id" in df.columns:
    df = df.filter(col("merchant_id").isNotNull())


# COMMAND ----------


df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.`merchant_master_silver`")

# COMMAND ----------

display(df)