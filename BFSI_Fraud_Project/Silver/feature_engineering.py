# Databricks notebook source
df_orig = spark.table("`bfsi-project-team-3`.`silver-tables`.silver_unified_transactions")

# COMMAND ----------

df_orig.printSchema()
display(df_orig.limit(5))
print("Count:", df_orig.count())

# COMMAND ----------

# DBTITLE 1,create copy
df_orig.write.format("delta").mode("overwrite") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.silver_features_working")

# COMMAND ----------

# DBTITLE 1,verify copy
df = spark.table("`bfsi-project-team-3`.`silver-tables`.silver_features_working")

print("Original:", df_orig.count())
print("Working:", df.count())

assert df_orig.count() == df.count()

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn("txn_ts_long", col("txn_timestamp").cast("long"))

# COMMAND ----------

df.select("txn_timestamp", "txn_ts_long").show(5)

# COMMAND ----------

# DBTITLE 1,define all windows
from pyspark.sql.window import Window

window_1h = Window.partitionBy("customer_id") \
    .orderBy("txn_ts_long") \
    .rangeBetween(-3600, 0)

window_24h = Window.partitionBy("customer_id") \
    .orderBy("txn_ts_long") \
    .rangeBetween(-86400, 0)

window_30d = Window.partitionBy("customer_id") \
    .orderBy("txn_ts_long") \
    .rangeBetween(-2592000, 0)

window_seq = Window.partitionBy("customer_id") \
    .orderBy("txn_timestamp")

# COMMAND ----------

# DBTITLE 1,verify all
print(window_1h)
print(window_24h)

# COMMAND ----------

# MAGIC %md
# MAGIC FEATURE ENGINEERING
# MAGIC

# COMMAND ----------

# DBTITLE 1,txn_count_1h
from pyspark.sql.functions import count

df = df.withColumn("txn_count_1h", count("*").over(window_1h))

# COMMAND ----------

display(df.select("customer_id", "txn_timestamp", "txn_count_1h"))

# COMMAND ----------

# DBTITLE 1,amount_sum_1h
from pyspark.sql.functions import sum

df = df.withColumn("amount_sum_1h", sum("amount").over(window_1h))

# COMMAND ----------

# DBTITLE 1,distinct_merchants_1h
from pyspark.sql.functions import approx_count_distinct

df = df.withColumn(
    "distinct_merchants_1h",
    approx_count_distinct("merchant_id").over(window_1h)
)

# COMMAND ----------

# DBTITLE 1,distinct_countries_24h
df = df.withColumn(
    "distinct_countries_24h",
    approx_count_distinct("country").over(window_24h)
)

# COMMAND ----------

# DBTITLE 1,time_since_last_txn
from pyspark.sql.functions import lag, unix_timestamp

df = df.withColumn("prev_txn", lag("txn_timestamp").over(window_seq))

df = df.withColumn(
    "time_since_last_txn_sec",
    unix_timestamp("txn_timestamp") - unix_timestamp("prev_txn")
)

# COMMAND ----------

# DBTITLE 1,amount_zscore
from pyspark.sql.functions import avg, stddev

df = df.withColumn("avg_amt_30d", avg("amount").over(window_30d))
df = df.withColumn("std_amt_30d", stddev("amount").over(window_30d))

df = df.withColumn(
    "amount_zscore",
    (col("amount") - col("avg_amt_30d")) / col("std_amt_30d")
)

# COMMAND ----------

# DBTITLE 1,final validation
df.select(
    "txn_count_1h",
    "amount_sum_1h",
    "distinct_merchants_1h",
    "distinct_countries_24h",
    "time_since_last_txn_sec",
    "amount_zscore"
).show(10)

# COMMAND ----------

# DBTITLE 1,saving final table
df.write.format("delta").mode("overwrite") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.silver_features")

# COMMAND ----------

