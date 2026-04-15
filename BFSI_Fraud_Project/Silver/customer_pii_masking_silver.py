# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")

spark.sql("USE SCHEMA `bronze-tables`")

# COMMAND ----------

df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`customers_corebanking_bronze`")

# COMMAND ----------

from pyspark.sql.functions import (
    col, concat, substring, lit, sha2, concat_ws,
    split, coalesce
)

# COMMAND ----------

# PAN TOKENIZATION (DUMMY SECRET)
# ================================
if "pan_number" in df.columns:
    secret_key = "bfsi_dummy_key_123"   # Replace secrets with dummy

    df = df.withColumn(
        "pan_tokenized",
        sha2(concat_ws("", col("pan_number"), lit(secret_key)), 256)
    )

# COMMAND ----------

# 📱 MOBILE MASKING
# ================================
if "mobile_number" in df.columns:
    df = df.withColumn(
        "mobile_masked",
        concat(
            substring(col("mobile_number"), 1, 2),
            lit("XXXXXX"),
            substring(col("mobile_number"), -2, 2)
        )
    )

# COMMAND ----------

# EMAIL MASKING
# ================================
if "email" in df.columns:
    df = df.withColumn(
        "email_masked",
        concat(
            substring(split(col("email"), "@")[0], 1, 2),
            lit("****@"),
            split(col("email"), "@")[1]
        )
    )

# COMMAND ----------

# NAME PSEUDONYMIZATION
# ================================
if "full_name" in df.columns:
    fake_names = [
        ("Karan Verma", "User_001"),
        ("Vikram Patel", "User_002"),
        ("Priya Joshi", "User_003"),
        ("Neha Sharma", "User_004")
    ]

    lookup_df = spark.createDataFrame(fake_names, ["full_name", "fake_name"])

    df = df.join(lookup_df, on="full_name", how="left")

    df = df.withColumn(
        "full_name_masked",
        coalesce(col("fake_name"), col("full_name"))
    )

# COMMAND ----------



# COMMAND ----------

cols_to_drop = ["pan_number", "mobile_number", "email", "full_name"]

for c in cols_to_drop:
    if c in df.columns:
        df = df.drop(c)

# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("`bfsi-project-team-3`.`silver-tables`.`customers_silver`")


# COMMAND ----------

display(df)