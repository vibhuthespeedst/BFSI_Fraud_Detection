# BFSI Fraud Detection

Fraud analytics pipeline on Azure Databricks, built as a team project. It ingests card, UPI and NEFT transactions, cleans and masks them, applies fraud rules and produces daily risk metrics and Suspicious Activity Reports (SAR).

Everything is stored as Delta tables in Unity Catalog, organised with the medallion architecture.

## Pipeline

**Bronze** (`BFSI_Fraud_Project/Bronze`)
- Batch load of raw transactions, plus a streaming card feed with Auto Loader (`cloudFiles`, JSON)
- UPI ingestion and NEFT XML parsing
- Metadata columns on every row (source system, load time, file name, batch id) and validation checks

**Silver** (`BFSI_Fraud_Project/Silver`)
- PII masking for customer, card, UPI and NEFT data
- Merchant clean-up and one unified transactions table
- Feature engineering (rolling 1-hour and 30-day amounts and counts)
- Fraud rules: velocity breach, impossible travel, amount far above the customer's average, international flag
- Customer profile kept as SCD Type 2

**Gold** (`BFSI_Fraud_Project/Gold`)
- Daily risk metrics: transaction counts and values, fraud rate and a channel breakdown
- SAR report: customers over the daily high-value limit or with flagged transactions, using window functions, with SHA-256 masked ids
- Capital adequacy summary

## Tech

PySpark · Spark SQL · Delta Lake · Databricks Auto Loader · Unity Catalog · Python

## Running it

The notebooks are Databricks source files. Import the `BFSI_Fraud_Project` folder into a Databricks workspace, create the `bronze-tables`, `silver-tables` and `gold-tables` schemas, and run the Bronze, Silver and Gold notebooks in order.
