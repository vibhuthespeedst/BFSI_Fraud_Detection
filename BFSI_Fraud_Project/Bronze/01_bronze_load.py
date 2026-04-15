# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`") 

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Project_01").getOrCreate()


# COMMAND ----------

xml_path = "/Volumes/bfsi-project-team-3/bronze-tables/neft-xml/neft_rtgs_transactions.xml"


# COMMAND ----------

df = (
    spark.read
    .format("xml")
    .option("rowTag", "CdtTrfTxInf")
    .load("/Volumes/bfsi-project-team-3/bronze-tables/neft-xml/neft_rtgs_transactions.xml")
)

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

neft_txn_df = (
    df.select(
        col("InstrId").alias("instr_id"),
        col("Amt._VALUE").cast("decimal(18,2)").alias("amt"),
        col("Amt._Ccy").alias("currency"),
        col("Dbtr.Nm").alias("dbtr_nm"),
        col("Cdtr.Nm").alias("cdtr_nm"),
        col("Purp.Cd").alias("purp_cd"),
        col("RmtInf.Ustrd").alias("rmtinf_ustrd")
    )
)

display(neft_txn_df)

# COMMAND ----------

neft_txn_df.write.format("delta").mode("overwrite").saveAsTable("`bfsi-project-team-3`.`bronze-tables`.`neft_rtgs_txn_raw`")

# COMMAND ----------

df.show()

# COMMAND ----------

#Setup
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")

batch_id = "batch_001"

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/bfsi-project-team-3/bronze-tables/card-stream/")

# COMMAND ----------

