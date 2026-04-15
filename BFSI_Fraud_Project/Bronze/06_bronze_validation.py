# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")  

# COMMAND ----------

card_df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`card_txn_bronze`")
upi_df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`upi_txn_bronze`")
neft_df = spark.table("`bfsi-project-team-3`.`bronze-tables`.`neft_rtgs_txn_bronze`")

# COMMAND ----------

audit_results = []

# COMMAND ----------

# CARD CHECK
card_count = card_df.count()
print("Card count:", card_count)

if not (400000 <= card_count <= 600000):
    audit_results.append(("CARD", "AUDIT_FAIL", f"Count={card_count} outside expected range"))

# UPI CHECK
upi_count = upi_df.count()
print("UPI count:", upi_count)

if upi_count == 0:
    audit_results.append(("UPI", "AUDIT_FAIL", "No data found"))

# NEFT CHECK
neft_count = neft_df.count()
print("NEFT count:", neft_count)

if neft_count == 0:
    audit_results.append(("NEFT_RTGS", "AUDIT_FAIL", "No data found"))

# COMMAND ----------

print(audit_results)

# COMMAND ----------

txn_total = float(
    neft_df.selectExpr("SUM(amt) as total").collect()[0]["total"]
)

header_total = float(5186855560.49)

print("Transaction total:", txn_total)
print("Header total:", header_total)

if txn_total is None or abs(txn_total - header_total) > 0.01:
    audit_results.append((
        "NEFT_RTGS",
        "BALANCE_FAIL",
        f"TxnSum={txn_total}, Header={header_total}"
    ))

# COMMAND ----------

print(audit_results)

# COMMAND ----------

from pyspark.sql.functions import count

# ---------------- NEFT ----------------
neft_dup = (
    neft_df.groupBy("instr_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

neft_dup_count = neft_dup.count()
print("NEFT duplicate count:", neft_dup_count)

if neft_dup_count > 0:
    print(" NEFT duplicates found")
    neft_dup.show(5)
    audit_results.append(("NEFT_RTGS", "CONTROL_FAIL", "Duplicate instr_id found"))
else:
    print(" No NEFT duplicates")


# ---------------- CARD ----------------
card_dup = (
    card_df.groupBy("txn_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

card_dup_count = card_dup.count()
print("CARD duplicate count:", card_dup_count)

if card_dup_count > 0:
    print(" CARD duplicates found")
    card_dup.show(5)
    audit_results.append(("CARD", "CONTROL_FAIL", "Duplicate txn_id found"))
else:
    print(" No CARD duplicates")


# ---------------- UPI ----------------
upi_dup = (
    upi_df.groupBy("upi_txn_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

upi_dup_count = upi_dup.count()
print("UPI duplicate count:", upi_dup_count)

if upi_dup_count > 0:
    print(" UPI duplicates found")
    upi_dup.show(5)
    audit_results.append(("UPI", "CONTROL_FAIL", "Duplicate txn_id found"))
else:
    print("No UPI duplicates")

# COMMAND ----------

card_ids = card_df.selectExpr("txn_id as id")
upi_ids = upi_df.selectExpr("upi_txn_id as id")
neft_ids = neft_df.selectExpr("instr_id as id")

combined = card_ids.union(upi_ids).union(neft_ids)

cross_dup = (
    combined.groupBy("id")
    .count()
    .filter("count > 1")
)

if cross_dup.count() > 0:
    audit_results.append((
        "ALL",
        "CONTROL_FAIL",
        "Cross-source duplicate txn_id"
    ))

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS abc_audit_failures (
    source_system STRING,
    check_type STRING,
    message STRING,
    load_ts TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

if audit_results:
    failure_df = spark.createDataFrame(
    audit_results,
    ["source_system", "check_type", "message"]
).withColumn("load_ts", current_timestamp())

failure_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("abc_audit_failures")

# Check if any FAIL exists
fail_count = failure_df.filter("check_type != 'PASS'").count()

if fail_count > 0:
    dbutils.notebook.exit("FAIL")
else:
    dbutils.notebook.exit("PASS")

# COMMAND ----------

if not audit_results:
    audit_results.append(("ALL", "PASS", "All checks passed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE abc_audit_failures
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.logRetentionDuration' = '3650 days',
# MAGIC   'delta.deletedFileRetentionDuration' = '3650 days'
# MAGIC )

# COMMAND ----------

spark.sql("SHOW TBLPROPERTIES abc_audit_failures").display()

# COMMAND ----------

