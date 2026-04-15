# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `gold-tables`")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#Load Data

df = spark.table("`bfsi-project-team-3`.`silver-tables`.`silver_fraud_alerts`")

# COMMAND ----------

# EAD (Exposure)
df = df.withColumn("ead", col("amount"))


# COMMAND ----------

display(df.select("txn_id", "ead"))

# COMMAND ----------

# LGD (Loss Given Default)
df = df.withColumn(
    "lgd",
    when(col("is_international") == 1, 0.45)  # higher risk
    .otherwise(0.25)
)


# COMMAND ----------

display(df.select("txn_id", "lgd"))

# COMMAND ----------

# PD (Probability of Default)
df = df.withColumn(
    "pd",
    when(col("fraud_flag") == True, 0.12)
    .when(col("amount_zscore") > 2, 0.03)
    .otherwise(0.005)
)

# COMMAND ----------

display(df.select("txn_id", "pd"))

# COMMAND ----------

#RWA Calculation
df = df.withColumn(
    "rwa",
    col("ead") * col("lgd") * col("pd") * 12.5
)

# COMMAND ----------

display(df)

# COMMAND ----------

# Aggregate Total RWA

total_rwa_df = df.agg(
    sum("rwa").alias("total_rwa")
)

# COMMAND ----------

display(total_rwa_df)

# COMMAND ----------


capital_df = spark.createDataFrame(
    [(500000000, 200000000)],  # Tier1, Tier2
    ["tier1_capital", "tier2_capital"]
)

# COMMAND ----------

# Compute CAR
result = capital_df.crossJoin(total_rwa_df) \
    .withColumn(
        "car",
        ((col("tier1_capital") + col("tier2_capital")) / col("total_rwa")) * 100
    )

# Basel threshold check
result = result.withColumn(
    "is_below_threshold",
    col("car") < 11.5
)

# COMMAND ----------

# Final Select
final_df = result.select(
    "tier1_capital",
    "tier2_capital",
    "total_rwa",
    "car",
    "is_below_threshold"
)

# COMMAND ----------

final_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capital_adequacy_summary")

# COMMAND ----------

display(final_df)

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()