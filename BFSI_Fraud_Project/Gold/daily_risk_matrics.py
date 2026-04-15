# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `gold-tables`")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

#Load Data
df = spark.table("`bfsi-project-team-3`.`silver-tables`.`silver_fraud_alerts`")

# Add txn_date
df = df.withColumn("txn_date", to_date("txn_timestamp"))


# COMMAND ----------

#Core Metrics

agg = df.groupBy("txn_date").agg(
    count("*").alias("total_txn_count"),
    sum("amount").alias("total_txn_value_inr"),
    sum(when(col("fraud_flag") == True, 1).otherwise(0)).alias("fraud_flagged_count"),
    sum(when(col("fraud_flag") == True, col("amount")).otherwise(0)).alias("fraud_flagged_value")
)


# COMMAND ----------

display(df)

# COMMAND ----------

# Fraud %
agg = agg.withColumn(
    "fraud_rate_pct",
    (col("fraud_flagged_count") / col("total_txn_count")) * 100
)

# COMMAND ----------

display(agg)

# COMMAND ----------

#Channel Breakdown

channel = df.groupBy("txn_date", "channel").count()

pivot = channel.groupBy("txn_date") \
    .pivot("channel") \
    .sum("count") \
    .fillna(0)

# COMMAND ----------

# Final Dashboard Table

final_df = agg.join(pivot, "txn_date")

# COMMAND ----------

# Top Risky Merchants

merchant_risk = df.groupBy("txn_date", "merchant_id").agg(
    (sum(when(col("fraud_flag") == True, 1).otherwise(0)) / count("*") * 100)
    .alias("fraud_rate_pct")
)

top_merchants = merchant_risk.withColumn(
    "rank",
    dense_rank().over(
        Window.partitionBy("txn_date").orderBy(col("fraud_rate_pct").desc())
    )
).filter(col("rank") <= 5)

# COMMAND ----------

# Main dashboard table
final_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_date") \
    .saveAsTable("daily_risk_metrics")

# Top risky merchants
top_merchants.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_date") \
    .saveAsTable("top_risky_merchants")

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(merchant_risk)

# COMMAND ----------

display(top_merchants)