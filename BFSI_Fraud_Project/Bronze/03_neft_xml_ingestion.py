# Databricks notebook source
spark.sql("USE CATALOG `bfsi-project-team-3`")
spark.sql("USE SCHEMA `bronze-tables`")  

# COMMAND ----------

input_path = "/Volumes/bfsi-project-team-3/bronze-tables/neft-xml/neft_rtgs_transactions.xml"

raw_df = spark.read.text(input_path)

# COMMAND ----------

import pandas as pd
import xml.etree.ElementTree as ET

def parse_xml_pandas(iterator):
    for pdf in iterator:
        rows = []

        for xml_string in pdf['value']:
            try:
                root = ET.fromstring(xml_string)

                for txn in root.findall(".//CdtTrfTxInf"):
                    instr_id = txn.findtext(".//InstrId")

                    amt = txn.find(".//Amt")
                    amount = float(amt.text) if amt is not None else None
                    currency = amt.attrib.get("Ccy") if amt is not None else None

                    dbtr_nm = txn.findtext(".//Dbtr/Nm")
                    cdtr_nm = txn.findtext(".//Cdtr/Nm")
                    purp_cd = txn.findtext(".//Purp/Cd")
                    rmtinf = txn.findtext(".//RmtInf/Ustrd")

                    rows.append({
                        "instr_id": instr_id,
                        "amt": amount,
                        "currency": currency,
                        "dbtr_nm": dbtr_nm,
                        "cdtr_nm": cdtr_nm,
                        "purp_cd": purp_cd,
                        "rmtinf_ustrd": rmtinf
                    })

            except:
                pass

        yield pd.DataFrame(rows)

# COMMAND ----------

schema = """
instr_id string,
amt double,
currency string,
dbtr_nm string,
cdtr_nm string,
purp_cd string,
rmtinf_ustrd string
"""

neft_txn_df = raw_df.mapInPandas(parse_xml_pandas, schema)

# COMMAND ----------

from pyspark.sql.functions import *

neft_txn_df = (
    neft_txn_df
    .withColumn("_source_system", lit("NEFT_RTGS"))
    .withColumn("_load_ts", current_timestamp())
    .withColumn("_batch_id", lit("batch_001"))
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS `bfsi-project-team-3`.`bronze-tables`.`neft_rtgs_txn_bronze`")
neft_txn_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("neft_rtgs_txn_bronze")


# COMMAND ----------

display(raw_df)

# COMMAND ----------

raw_df.count()

# COMMAND ----------

xml_string = "\n".join([row.value for row in raw_df.collect()])

# COMMAND ----------

raw_df_fixed = spark.createDataFrame([(xml_string,)], ["value"])

# COMMAND ----------

neft_txn_df = raw_df_fixed.mapInPandas(parse_xml_pandas, schema)

# COMMAND ----------

display(neft_txn_df)

# COMMAND ----------

