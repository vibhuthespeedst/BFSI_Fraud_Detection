# Databricks notebook source
# DBTITLE 1,load feature table
df = spark.table("`bfsi-project-team-3`.`silver-tables`.silver_features")

# COMMAND ----------

# DBTITLE 1,verify
df.printSchema()
display(df.limit(5))
print("Count:", df.count())

# COMMAND ----------

# DBTITLE 1,international flag
from pyspark.sql.functions import when, col

df = df.withColumn(
    "is_international",
    when(col("country") != "IN", 1).otherwise(0)
)

# COMMAND ----------

# DBTITLE 1,High amount vs avg (10x rule)
df = df.withColumn(
    "high_vs_avg",
    col("amount") > (10 * col("avg_amt_30d"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC APPLY FRAUD RULES

# COMMAND ----------

# DBTITLE 1,rele 1 : Velocity / Amount breach
df = df.withColumn(
    "rule_velocity_breach",
    (col("txn_count_1h") > 10) | (col("amount_sum_1h") > 500000)
)

# COMMAND ----------

# DBTITLE 1,Rule 2: Impossible travel
df = df.withColumn(
    "rule_geo_anomaly",
    (col("distinct_countries_24h") > 3) |
    ((col("is_international") == 1) & (col("amount") > 100000))
)

# COMMAND ----------

# DBTITLE 1,Rule 3: Merchant / behavior anomaly
df = df.withColumn(
    "rule_behavior_anomaly",
    col("high_vs_avg")
)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE FRAUD FLAG

# COMMAND ----------

df = df.withColumn(
    "fraud_flag",
    col("rule_velocity_breach") |
    col("rule_geo_anomaly") |
    col("rule_behavior_anomaly")
)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE RULE LIST

# COMMAND ----------

from pyspark.sql.functions import array, when

df = df.withColumn(
    "rules_triggered",
    array(
        when(col("rule_velocity_breach"), "VELOCITY_BREACH"),
        when(col("rule_geo_anomaly"), "GEO_ANOMALY"),
        when(col("rule_behavior_anomaly"), "BEHAVIOR_ANOMALY")
    )
)

# COMMAND ----------

# DBTITLE 1,clean nulls
from pyspark.sql.functions import expr

df = df.withColumn(
    "rules_triggered",
    expr("filter(rules_triggered, x -> x is not null)")
)

# COMMAND ----------

# MAGIC %md
# MAGIC add metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df = df.withColumn("rule_fired_ts", current_timestamp()) \
       .withColumn("rule_version", lit("v1.0"))

# COMMAND ----------

# MAGIC %md
# MAGIC validate rules

# COMMAND ----------

# DBTITLE 1,Fraud distribution
df.groupBy("fraud_flag").count().show()

# COMMAND ----------

# DBTITLE 1,rule counts
df.selectExpr("explode(rules_triggered) as rule") \
  .groupBy("rule") \
  .count() \
  .show()

# COMMAND ----------

# DBTITLE 1,Sample fraud rows
display(df.filter("fraud_flag = true").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC save fraud alerts

# COMMAND ----------

df.filter("fraud_flag = true") \
  .write.format("delta").mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.silver_fraud_alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC final check

# COMMAND ----------

alerts = spark.table("`bfsi-project-team-3`.`silver-tables`.silver_fraud_alerts")

print("Fraud Alerts Count:", alerts.count())
display(alerts.limit(10))

# COMMAND ----------

