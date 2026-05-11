# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1: Create Consolidated Identity Information
# MAGIC
# MAGIC This notebook consolidates digital identity information from raw impression logs to create the foundation for our identity graph. This is the first step in our medallion architecture workflow.
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC Transform raw impression logs into aggregated identity combinations that will serve as the basis for building email-to-identifier relationships.
# MAGIC
# MAGIC ## 📊 Input Data
# MAGIC - **Source**: `{catalog_name}.bronze.impression_logs_prod` (Raw impression logs)
# MAGIC - **Key Fields**: 
# MAGIC   - `request_kv._server_email` - The hashed email address as recorded by the ad server (our core identifier proxy)
# MAGIC   - `request_kv._server_ifa` - The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device, used across applications)
# MAGIC   - `ip_address` - IP addresses from ad requests
# MAGIC   - `date` - Impression timestamps
# MAGIC   - `request_kv._is_coppa` - COPPA compliance flag
# MAGIC
# MAGIC ## 🔄 Processing Logic
# MAGIC 1. **Filter COPPA-protected data** - Remove records flagged for children's privacy protection
# MAGIC 2. **Remove invalid records** - Filter out rows with missing email addresses
# MAGIC 3. **Aggregate identity combinations** - Group by `(email, ip, ifa)` triplets
# MAGIC 4. **Calculate metrics** - Compute frequency and recency for each combination
# MAGIC
# MAGIC ## 📈 Output
# MAGIC - **Destination**: `{catalog_name}.silver.identity_info_consolidated`
# MAGIC - **Schema**: Aggregated identity combinations with statistical metrics

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# Two config sources are supported (widget wins so DAB job parameters take precedence over the file):
#   1. Job parameters / notebook widgets `catalog_name` and `schema_prefix` (DAB flow)
#   2. ./data/catalog_name.json written by 01_Workflow_Orchestration/setup.py (Solution Launcher flow)
import json
import os

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_prefix", "")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_prefix = dbutils.widgets.get("schema_prefix").strip()

if not catalog_name and os.path.exists("./data/catalog_name.json"):
    with open("./data/catalog_name.json", "r") as f:
        config = json.load(f)
    catalog_name = config["catalog_name"]
    if not schema_prefix:
        schema_prefix = config.get("schema_prefix", "")

if not catalog_name:
    raise ValueError(
        "catalog_name is empty. Pass --params catalog_name=<name> to `bundle run`, "
        "or run the Solution Launcher first."
    )

print(f"✅ Loaded catalog name: {catalog_name}")
if schema_prefix:
    print(f"✅ Schema prefix: {schema_prefix}")
    schema_prefix += "_"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Raw Impression Logs
# MAGIC
# MAGIC We start by loading the raw impression logs from our Bronze layer. These logs contain the digital advertising events that include identity signals we'll use to build our graph.

# COMMAND ----------

impression_logs = spark.table(f"{catalog_name}.{schema_prefix}bronze.impression_logs_prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Explore the Data Structure
# MAGIC
# MAGIC The impression logs contain several key fields for identity resolution:
# MAGIC
# MAGIC ### 🔍 Key Data Fields:
# MAGIC 1. **`request_kv`** - JSON object containing identity signals:
# MAGIC    - `_server_email` - The hashed email address as recorded by the ad server (our core identifier proxy)
# MAGIC    - `_server_ifa` - The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device, used across applications)
# MAGIC    - `_is_coppa` - COPPA compliance flag (children's privacy protection)
# MAGIC 2. **`ip_address`** - IP address captured during the ad request
# MAGIC 3. **`date`** - Timestamp of the impression event
# MAGIC
# MAGIC ### 🎯 Why These Fields Matter:
# MAGIC - **Email**: Serves as our core identity anchor across devices
# MAGIC - **IFA**: Consented Advertising ID that works across applications on a single device (e.g., idfa, gaid, rida, tifa, lguid)
# MAGIC - **IP Address**: Indicates household-level connections and geographic patterns
# MAGIC - **Date**: Helps us understand recency and frequency of identity signals

# COMMAND ----------

display(impression_logs.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Identity Aggregations
# MAGIC
# MAGIC Now we'll create our consolidated identity table by aggregating all observed `(email, ip, ifa)` combinations over time.
# MAGIC
# MAGIC ### 📊 Aggregation Strategy:
# MAGIC For each unique combination of identifiers, we calculate:
# MAGIC - **`min_date`** - First time this combination was observed
# MAGIC - **`max_date`** - Most recent observation (indicates freshness)
# MAGIC - **`n_occurrences`** - Total frequency of this combination
# MAGIC
# MAGIC ### 🔒 Privacy & Data Quality Filters:
# MAGIC - **COPPA Compliance**: Remove records flagged for children's privacy protection
# MAGIC - **Data Validity**: Filter out records missing core email identifiers
# MAGIC
# MAGIC ### 💡 Why This Matters:
# MAGIC These metrics will drive our "waterfall logic" in subsequent steps - helping us identify the **strongest** and **most recent** relationships between emails and their associated identifiers.

# COMMAND ----------

# Define our core identifier (email) for filtering
core_identifier = F.col("request_kv._server_email")

# 🔒 COPPA Compliance Filter: Exclude children's data
# Only keep records where COPPA flag is False or null (not flagged)
coppa_filter = (F.col("request_kv._is_coppa") == False) | F.col(
    "request_kv._is_coppa"
).isNull()

print("🔄 Creating consolidated identity information...")
print("📊 Grouping by: (email, ip_address, ifa)")
print("🔒 Applying COPPA compliance filters")
print("📈 Calculating frequency and recency metrics")

# Create our consolidated identity aggregation
identity_info_consolidated = (
    impression_logs
    .filter(coppa_filter)  # Remove COPPA-protected data
    .filter(core_identifier.isNotNull())  # Remove records without email
    .groupBy("request_kv._server_email", "ip_address", "request_kv._server_ifa")
    .agg(
        F.min("date").alias("min_date"),        # First observation
        F.max("date").alias("max_date"),        # Most recent observation
        F.count("*").alias("n_occurances"),     # Total frequency
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Save to Silver Layer
# MAGIC
# MAGIC We'll persist our consolidated identity information to the Silver layer in Unity Catalog. This becomes our clean, aggregated foundation for the next steps in our identity graph workflow.

# COMMAND ----------

# 💾 Save to Unity Catalog Silver layer
print(f"💾 Saving consolidated identity data to: {catalog_name}.{schema_prefix}silver.identity_info_consolidated")

identity_info_consolidated.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}silver.identity_info_consolidated"
)

print("✅ Successfully saved identity_info_consolidated table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Explore the Results
# MAGIC
# MAGIC Let's examine our consolidated identity table to understand the data patterns and validate our aggregations.

# COMMAND ----------

print("📊 Sample of consolidated identity information:")
display(identity_info_consolidated.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Part 1 Complete!
# MAGIC
# MAGIC We've successfully created our consolidated identity foundation table with:
# MAGIC
# MAGIC ### ✅ What We Accomplished:
# MAGIC - **Privacy Compliance**: Filtered out COPPA-protected records
# MAGIC - **Data Quality**: Removed invalid records without email identifiers  
# MAGIC - **Identity Aggregation**: Created unique `(email, ip, ifa)` combinations
# MAGIC - **Relationship Metrics**: Calculated frequency and recency for each combination
# MAGIC
# MAGIC ### 📋 Table Schema (`identity_info_consolidated`):
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC | `_server_email` | The hashed email address as recorded by the ad server (core identifier proxy) |
# MAGIC | `ip_address` | The associated IP address |
# MAGIC | `_server_ifa` | The associated IFA (consented advertising ID) as reported by the ad server |
# MAGIC | `min_date` | First observation date of the `(email, ip, ifa)` combination|
# MAGIC | `max_date` | Most recent observation date  of the `(email, ip, ifa)` combination |
# MAGIC | `n_occurances` | Total frequency count of the `(email, ip, ifa)` combination |
# MAGIC
# MAGIC ### 🔄 Next Steps:
# MAGIC This consolidated table will now feed into our pairing logic:
# MAGIC 1. **Part 2a: `02a_Create Email IFA Paired Table`** - Links emails with their strongest IFA
# MAGIC 2. **Part 2b: `02b_Create Email IP Paired Table`** - Links emails with their strongest IP
# MAGIC
# MAGIC The frequency and recency metrics we calculated here will drive the "waterfall logic" to determine the **primary** identifiers for each email address.
