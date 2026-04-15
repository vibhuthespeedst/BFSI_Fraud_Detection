# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")

# COMMAND ----------

schema = spark.table("card_transactions").schema

# COMMAND ----------

input_path = "/Volumes/bfsi-project-team-3/bronze-tables/card-stream/"

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/bfsi-project-team-3/bronze-tables/card-stream/"))

# COMMAND ----------

schema = spark.table("card_transactions").schema

card_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.includeExistingFiles", "true")
    .schema(schema)
    .load("/Volumes/bfsi-project-team-3/bronze-tables/card-stream/")
)

# COMMAND ----------

from pyspark.sql.functions import *

card_stream_df = (
    card_stream_df
    .withColumn("_source_system", lit("CARD"))
    .withColumn("_load_ts", current_timestamp())
    .withColumn("_file_name", col("_metadata.file_path"))  
    .withColumn("_batch_id", lit("stream_batch_001"))
)

# COMMAND ----------

query = (
    card_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/bfsi-project-team-3/bronze-tables/card-stream/checkpoints/")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("card_txn_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from card_txn_bronze;

# COMMAND ----------

