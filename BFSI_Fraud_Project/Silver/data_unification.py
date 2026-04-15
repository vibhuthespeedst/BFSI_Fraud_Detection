# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `bfsi-project-team-3`.`silver-tables`.`card_txn_silver`;

# COMMAND ----------

spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `silver-tables`")

# COMMAND ----------

card_df = spark.table("card_txn_silver")
upi_df = spark.table("upi_txn_silver")
neft_df = spark.table("neft_rtgs_txn_silver")


# COMMAND ----------

card_df.printSchema()
upi_df.printSchema()
neft_df.printSchema()


# COMMAND ----------

final_cols = [
    "txn_id",
    "customer_id",
    "txn_timestamp",
    "txn_type",
    "channel",
    "amount",
    "currency",
    "status",
    "merchant_id",
    "merchant_name",
    "country",
    "payment_mode",
    "ingestion_source"
]

# COMMAND ----------

from pyspark.sql.functions import lit, when

card_final = card_df.select(
    "txn_id",
    "customer_id",
    card_df.txn_ts.alias("txn_timestamp"),
    lit("CARD").alias("txn_type"),
    lit("POS").alias("channel"),

    # amount in INR
    when(card_df.txn_currency == "INR", card_df.txn_amount)
    .otherwise(card_df.txn_amount * 80)
    .alias("txn_amount_inr"),

    # normalized status
    when(card_df.response_code == "00", "SUCCESS")
    .otherwise("FAILED")
    .alias("status"),

    "merchant_id",

    # masked_counterparty
    card_df.merchant_name.alias("masked_counterparty"),

    card_df.country_code.alias("country"),

    # is_international
    card_df.is_international.alias("is_international"),

    lit("CARD").alias("payment_mode"),
    "_source_system"
)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

upi_final = upi_df.select(
    upi_df.upi_txn_id.alias("txn_id"),
    upi_df.payer_vpa.alias("customer_id"),
    to_timestamp("txn_ts").alias("txn_timestamp"),
    lit("UPI").alias("txn_type"),
    lit("UPI").alias("channel"),

    # already INR
    upi_df.amount.alias("txn_amount_inr"),

    # normalized status
    when(upi_df.status == "SUCCESS", "SUCCESS")
    .when(upi_df.status == "FAILED", "FAILED")
    .otherwise("PENDING")
    .alias("status"),

    lit(None).alias("merchant_id"),

    # masked_counterparty
    upi_df.payee_vpa.alias("masked_counterparty"),

    lit(None).alias("country"),

    lit(False).alias("is_international"),

    lit("UPI").alias("payment_mode"),
    "_source_system"
)

# COMMAND ----------

neft_final = neft_df.select(
    neft_df.instr_id.alias("txn_id"),
    neft_df.dbtr_nm_masked.alias("customer_id"),
    lit(None).cast("timestamp").alias("txn_timestamp"),
    lit("NEFT").alias("txn_type"),
    lit("BANK_TRANSFER").alias("channel"),

    neft_df.amt.cast("double").alias("txn_amount_inr"),

    lit("SUCCESS").alias("status"),

    lit(None).alias("merchant_id"),

    # masked_counterparty
    neft_df.cdtr_nm_masked.alias("masked_counterparty"),

    lit(None).alias("country"),

    lit(False).alias("is_international"),

    lit("BANK").alias("payment_mode"),
    lit("NEFT").alias("_source_system")
)

# COMMAND ----------

unified_df = card_final.unionByName(upi_final).unionByName(neft_final)

# COMMAND ----------

unified_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_final_unified_transactions")

# COMMAND ----------

display(unified_df)

# COMMAND ----------

unified_df.count()
display(unified_df)

# COMMAND ----------

unified_df.groupBy("txn_type").count().show()
unified_df.groupBy("payment_mode").count().show()

# COMMAND ----------

unified_df.printSchema()

# COMMAND ----------

