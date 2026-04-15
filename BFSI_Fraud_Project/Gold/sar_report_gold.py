# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `gold-tables`")

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Load Data
df = spark.table("`bfsi-project-team-3`.`silver-tables`.`silver_fraud_alerts`")

# COMMAND ----------

# Add txn_date
df = df.withColumn("txn_date", to_date("txn_timestamp"))

# COMMAND ----------

display(df)

# COMMAND ----------

# Window: daily total per customer

w = Window.partitionBy("customer_id", "txn_date")

df = df.withColumn(
    "daily_total",
    sum("amount").over(w)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# Filter conditions
high_value_txn = df.filter(col("daily_total") > 1000000)
fraud_txn = df.filter(col("fraud_flag") == True)

# COMMAND ----------

# Combine
sar = high_value_txn.unionByName(fraud_txn)

# COMMAND ----------

# Masking
sar = sar.withColumn(
    "masked_txn_id", sha2(col("txn_id"), 256)
).withColumn(
    "customer_token", sha2(col("customer_id"), 256)
)


# COMMAND ----------

# Rules flatten
sar = sar.withColumn(
    "fraud_rule_triggered",
    concat_ws(",", coalesce(col("rules_triggered"), array()))
)

# COMMAND ----------

# Risk rating

sar = sar.withColumn(
    "risk_rating",
    when(col("fraud_flag") == True, "HIGH")
    .when(col("amount_zscore") > 2, "MEDIUM")
    .otherwise("LOW")
)


# COMMAND ----------

# Final select (REMOVE daily_total)
sar_final = sar.select(
    col("txn_date").alias("report_date"),
    "masked_txn_id",
    "customer_token",
    col("amount").alias("txn_amount"),
    col("channel").alias("txn_channel"),
    "fraud_rule_triggered",
    "risk_rating",
    col("merchant_id").alias("reporting_branch")
).dropDuplicates()

# COMMAND ----------

# Write
sar_final.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("report_date") \
    .saveAsTable("sar_report")

# COMMAND ----------

display(sar_final)