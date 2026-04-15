# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")

spark.sql("USE SCHEMA `bronze-tables`")

# COMMAND ----------

from pyspark.sql.functions import (
    col, concat, substring, lit
)
df = spark.table("upi_txn_bronze")
df.show()


# COMMAND ----------

print("Columns:", df.columns)

# COMMAND ----------

if "mobile_number" in df.columns:
    df = df.withColumn(
        "mobile_masked",
        concat(
            substring(col("mobile_number"), 1, 2),
            lit("XXXXXX"),
            substring(col("mobile_number"), -2, 2)
        )
    ).drop("mobile_number")

# COMMAND ----------

from pyspark.sql.functions import col, concat, substring, lit, split
if "ip_address" in df.columns:
    df = df.withColumn(
        "ip_masked",
        concat(
            split(col("ip_address"), "\\.")[0], lit("."),
            split(col("ip_address"), "\\.")[1], lit("."),
            split(col("ip_address"), "\\.")[2], lit(".0")
        )
    )

# COMMAND ----------

cols_to_drop = ["mobile_number", "ip_address"]

for c in cols_to_drop:
    if c in df.columns:
        df = df.drop(c)

# COMMAND ----------

df.write.format("delta").option("mergeSchema", "true") \
  .mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.`upi_txn_silver`")



# COMMAND ----------

display(df)

# COMMAND ----------

