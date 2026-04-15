# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")  

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")

# COMMAND ----------

upi_df = spark.table("upi_transactions")
display(upi_df)

# COMMAND ----------

upi_split_df = upi_df.select(
    split(col(upi_df.columns[0]), "\\|").alias("data")
)

# COMMAND ----------

upi_df = upi_split_df.select(
    col("data")[0].alias("txn_ts"),
    col("data")[1].alias("upi_txn_id"),
    col("data")[2].alias("payer_vpa"),
    col("data")[3].alias("payee_vpa"),
    col("data")[4].alias("payer_bank"),
    col("data")[5].alias("payee_bank"),
    col("data")[6].cast("double").alias("amount"),
    col("data")[7].alias("status"),
    col("data")[8].alias("remarks"),
    col("data")[9].alias("device_fingerprint"),
    col("data")[10].alias("ip_address"),
    col("data")[11].cast("double").alias("latitude"),
    col("data")[12].cast("double").alias("longitude")
)

# COMMAND ----------

upi_df.printSchema()
display(upi_df)

# COMMAND ----------

upi_valid_df = (
    upi_df
    .withColumn("is_valid_txn_id", col("upi_txn_id").isNotNull())
    .withColumn("is_valid_amount", col("amount") > 0)
)

# COMMAND ----------

upi_valid_df = upi_valid_df.withColumn(
    "is_valid_record",
    col("is_valid_txn_id") & col("is_valid_amount") 
)

# COMMAND ----------

total_records = upi_df.count()

print("Total records:", total_records)


# COMMAND ----------

upi_valid_df = upi_valid_df.withColumn(
    "file_validation_status",
    when(col("is_valid_record"), lit("PASS")).otherwise(lit("FAIL"))
)

# COMMAND ----------

from pyspark.sql.functions import *

upi_bronze_df = (
    upi_valid_df
    .withColumn("_source_system", lit("UPI"))
    .withColumn("_load_ts", current_timestamp())
    .withColumn("_file_name", lit("upi_transactions.csv"))
    .withColumn("_batch_id", lit("batch_001"))
)

# COMMAND ----------

upi_bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("upi_txn_bronze")

# COMMAND ----------

display(spark.table("upi_txn_bronze"))