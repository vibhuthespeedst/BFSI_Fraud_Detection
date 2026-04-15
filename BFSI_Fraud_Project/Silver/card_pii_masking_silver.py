# Databricks notebook source

spark.sql("""
CREATE SCHEMA IF NOT EXISTS `bfsi-project-team-3`.`silver-tables`
""")

# Use correct catalog + schema

spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `silver-tables`")

# COMMAND ----------

from pyspark.sql.functions import concat, substring, lit, col

df = spark.table("card_txn_bronze")

# Mask card number
df = df.withColumn(
    "card_number_masked",
    concat(
        substring(col("card_number"), 1, 6),
        lit("XXXXXX"),
        substring(col("card_number"), -4, 4)
    )
)


# COMMAND ----------

df = df.drop("card_number")

# Write to Silver schema
df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.`card_txn_silver`")


display(df)