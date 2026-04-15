# Databricks notebook source
# MAGIC %md
# MAGIC CREATE BASE TABLE (SCD TABLE)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `bfsi-project-team-3`.`silver-tables`.dim_customer_risk (
# MAGIC     customer_id STRING,
# MAGIC     risk_rating STRING,
# MAGIC     kyc_status STRING,
# MAGIC     account_type STRING,
# MAGIC     effective_start_date TIMESTAMP,
# MAGIC     effective_end_date TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC create 2 snapshots for scd

# COMMAND ----------

# MAGIC %md
# MAGIC snapshot 1

# COMMAND ----------

df_base = spark.table("`bfsi-project-team-3`.`silver-tables`.silver_unified_transactions")

# COMMAND ----------

# DBTITLE 1,picking some customers
df_snapshot1 = df_base.select("customer_id") \
    .distinct() \
    .limit(5)

# COMMAND ----------

# DBTITLE 1,add req columns
from pyspark.sql.functions import lit

df_snapshot1 = df_snapshot1 \
    .withColumn("risk_rating", lit("LOW")) \
    .withColumn("kyc_status", lit("VERIFIED")) \
    .withColumn("account_type", lit("SAVINGS"))

# COMMAND ----------

# DBTITLE 1,verify
display(df_snapshot1)

# COMMAND ----------

# DBTITLE 1,save as csv
df_snapshot1.write.format("delta").mode("overwrite") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_1")

# COMMAND ----------

# MAGIC %md
# MAGIC snapshot 2
# MAGIC

# COMMAND ----------

df_snapshot2 = df_snapshot1

# COMMAND ----------

# DBTITLE 1,modify some customers
# from pyspark.sql.functions import when

# df_snapshot2 = df_snapshot2.withColumn(
#     "risk_rating",
#     when(df_snapshot2.customer_id == df_snapshot2.first()["customer_id"], "HIGH")
#     .otherwise("LOW")
# )

# COMMAND ----------

df_snapshot2.write.format("delta").mode("overwrite") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2")

# COMMAND ----------

# DBTITLE 1,see the files
display(spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_1"))
display(spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2"))

# COMMAND ----------

# DBTITLE 1,load ss1
df_snapshot1 = spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_1")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE SNAPSHOT 2 WITH REAL CHANGES
# MAGIC  - Change risk_rating for 1 customer
# MAGIC  - Change kyc_status for another
# MAGIC  rest same

# COMMAND ----------

# DBTITLE 1,apply changes
from pyspark.sql.functions import when

df_snapshot2 = df_snapshot1.withColumn(
    "risk_rating",
    when(df_snapshot1.customer_id == df_snapshot1.collect()[0]["customer_id"], "HIGH")
    .otherwise(df_snapshot1.risk_rating)
).withColumn(
    "kyc_status",
    when(df_snapshot1.customer_id == df_snapshot1.collect()[1]["customer_id"], "PENDING")
    .otherwise(df_snapshot1.kyc_status)
)

# COMMAND ----------

# DBTITLE 1,verify
# display(df_snapshot2)
display(df_snapshot2)

# COMMAND ----------

# DBTITLE 1,OVERWRITE SNAPSHOT 2
df_snapshot2.write.format("delta").mode("overwrite") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2")

# COMMAND ----------

# DBTITLE 1,verify
display(spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN `bfsi-project-team-3`.`silver-tables`;

# COMMAND ----------

# DBTITLE 1,creating destination table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `bfsi-project-team-3`.`silver-tables`.dim_customer_risk (
# MAGIC     customer_id STRING,
# MAGIC     risk_rating STRING,
# MAGIC     kyc_status STRING,
# MAGIC     account_type STRING,
# MAGIC     effective_start_date TIMESTAMP,
# MAGIC     effective_end_date TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN `bfsi-project-team-3`.`silver-tables`;

# COMMAND ----------

# MAGIC %md
# MAGIC initial load (ss1)

# COMMAND ----------

# DBTITLE 1,Load snapshot 1
df_src = spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_1")

# COMMAND ----------

display(df_src)

# COMMAND ----------

# DBTITLE 1,Add SCD columns
from pyspark.sql.functions import current_timestamp, lit

df_src = df_src.withColumn("effective_start_date", current_timestamp()) \
               .withColumn("effective_end_date", lit(None).cast("timestamp")) \
               .withColumn("is_current", lit(True))

# COMMAND ----------

# DBTITLE 1,Insert into dim table
# df_src.write.format("delta").mode("append") \
#     .saveAsTable("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

# COMMAND ----------

display(spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk"))

# COMMAND ----------

# %sql
# TRUNCATE TABLE `bfsi-project-team-3`.`silver-tables`.dim_customer_risk;

# COMMAND ----------

# DBTITLE 1,detect changes
df_updates = spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2")
df_dim = spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

from pyspark.sql.functions import col

df_changed = df_updates.alias("src") \
    .join(df_dim.alias("tgt"),
          (col("src.customer_id") == col("tgt.customer_id")) &
          (col("tgt.is_current") == True)) \
    .filter(
        (col("src.risk_rating") != col("tgt.risk_rating")) |
        (col("src.kyc_status") != col("tgt.kyc_status")) |
        (col("src.account_type") != col("tgt.account_type"))
    ) \
    .select("src.*")

# COMMAND ----------

# DBTITLE 1,verify
display(df_changed)

# COMMAND ----------

# MAGIC %md
# MAGIC apply scd merge (ss2)

# COMMAND ----------

# DBTITLE 1,Run MERGE
# MAGIC %sql
# MAGIC MERGE INTO `bfsi-project-team-3`.`silver-tables`.dim_customer_risk AS target
# MAGIC USING `bfsi-project-team-3`.`silver-tables`.customer_snapshot_2 AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.risk_rating != source.risk_rating OR
# MAGIC     target.kyc_status != source.kyc_status OR
# MAGIC     target.account_type != source.account_type
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.effective_end_date = current_timestamp(),
# MAGIC     target.is_current = false

# COMMAND ----------

df = spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT NEW RECORDS

# COMMAND ----------

# DBTITLE 1,Create new rows
from pyspark.sql.functions import current_timestamp, lit

df_new = df_changed \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# COMMAND ----------

# DBTITLE 1,insert
df_new.write.format("delta").mode("append") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

# COMMAND ----------

# DBTITLE 1,final verify
df = spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")
display(df.orderBy("customer_id", "effective_start_date"))

# COMMAND ----------

# DBTITLE 1,RECREATE df_changed
df_updates = spark.table("`bfsi-project-team-3`.`silver-tables`.customer_snapshot_2")
df_dim = spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

from pyspark.sql.functions import col

df_changed = df_updates.alias("src") \
    .join(df_dim.alias("tgt"),
          col("src.customer_id") == col("tgt.customer_id")) \
    .filter(
        (col("src.risk_rating") != col("tgt.risk_rating")) |
        (col("src.kyc_status") != col("tgt.kyc_status")) |
        (col("src.account_type") != col("tgt.account_type"))
    ) \
    .select("src.*") \
    .distinct()

# COMMAND ----------

# DBTITLE 1,verify
display(df_changed)

# COMMAND ----------

# DBTITLE 1,create new rows
from pyspark.sql.functions import current_timestamp, lit

df_new = df_changed \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# COMMAND ----------

display(df_new)

# COMMAND ----------

# DBTITLE 1,FINAL VERIFY
df = spark.table("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

display(df.orderBy("customer_id", "effective_start_date"))

# COMMAND ----------

# DBTITLE 1,insert new records
df_new.write.format("delta").mode("append") \
    .saveAsTable("`bfsi-project-team-3`.`silver-tables`.dim_customer_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `bfsi-project-team-3`.`silver-tables`.dim_customer_risk
# MAGIC WHERE customer_id = 'CUST0001858'
# MAGIC AND is_current = true;

# COMMAND ----------

# MAGIC %md
# MAGIC time-based query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `bfsi-project-team-3`.`silver-tables`.dim_customer_risk
# MAGIC WHERE customer_id = 'CUST0001858'
# MAGIC AND effective_start_date <= current_timestamp()
# MAGIC AND (
# MAGIC     effective_end_date IS NULL 
# MAGIC     OR effective_end_date > current_timestamp()
# MAGIC );

# COMMAND ----------

# %sql
# TRUNCATE TABLE `bfsi-project-team-3`.`silver-tables`.df_snapshot1;

# COMMAND ----------

display(df_snapshot2)

# COMMAND ----------

# %sql
# TRUNCATE TABLE `bfsi-project-team-3`.`silver-tables`.dim_customer_risk;ā