# Databricks notebook source
pii_columns = [
    "pan_number",
    "card_number",
    "mobile_number",
    "email",
    "full_name",
    "ip_address"
]



# COMMAND ----------


tables = [
    "customers_silver",
    "card_txn_silver",
    "upi_txn_silver",
    "neft_rtgs_txn_silver"
]


# COMMAND ----------

for table in tables:
    df = spark.table(f"`bfsi-project-team-3`.`silver-tables`.`{table}`")
    
    found = [c for c in pii_columns if c in df.columns]
    
    if len(found) == 0:
        print(f"{table} → CLEAN (No PII)")
    else:
        print(f"{table} → PII FOUND:", found)