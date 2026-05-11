# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2a: Create Email-IFA Paired Table
# MAGIC
# MAGIC This notebook creates the first pairing table that links email addresses to their **primary Identifier for Advertising (IFA)**. IFAs are consented advertising identifiers that work across applications on a single device (such as mobile phones, tablets, and CTVs).
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC For each email address, determine the **single best** IFA (Identifier for Advertising) to use as the primary cross-application advertising connection.
# MAGIC
# MAGIC ## 📊 Input Data
# MAGIC - **Source**: `{catalog_name}.silver.identity_info_consolidated` (from Part 1)
# MAGIC - **Focus**: Email-IFA relationships with frequency and recency metrics
# MAGIC
# MAGIC ## 🧮 Primary ID Selection Logic
# MAGIC For each email, we select the "primary" IFA using waterfall logic. This will be the same logic applied to select IPs, and can be applied to any additional digital identifiers you own:
# MAGIC 1. **Highest frequency** (`n_occurances`) - The IFA seen most often with this email
# MAGIC 2. **Most recent** (`max_date`) - In case of ties, choose the IFA observed most recently
# MAGIC
# MAGIC ## 💡 Why This Matters
# MAGIC - **Cross-application tracking**: Links email behavior to app engagement across devices (mobile, tablet, CTV)
# MAGIC - **Audience targeting**: Enables app-based ad targeting based on email segments
# MAGIC - **Attribution**: Connects in-app conversions back to email-driven awareness
# MAGIC - **Device-specific reach**: IFAs are tied to a single device, enabling precise targeting
# MAGIC
# MAGIC ## 📈 Output
# MAGIC - **Destination**: `{catalog_name}.silver.email_ifa_pairs`
# MAGIC - **Schema**: Email addresses with their primary and ranked IFA associations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup Primary ID Resolution Logic
# MAGIC
# MAGIC We'll create a window function that implements our **waterfall logic** for determining primary IFAs:
# MAGIC
# MAGIC ### 🎯 Window Function Strategy:
# MAGIC - **Partition by**: `_server_email` (group all IFAs for each email)
# MAGIC - **Order by**: 
# MAGIC   1. `n_occurances DESC` (highest frequency first)
# MAGIC   2. `max_date DESC` (most recent as tiebreaker)
# MAGIC
# MAGIC This ranking helps us identify the **most relevant advertising identifier** (across mobile, tablet, CTV, etc.) for each email address.

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

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Window Function
# MAGIC
# MAGIC Create the window specification for our primary IFA selection logic.

# COMMAND ----------

# 🎯 Primary ID Resolution Logic
# This window function will rank IFAs for each email based on:
# 1. Frequency (how often they appear together)
# 2. Recency (when they were last seen together)
primary_id_resolution_logic = Window.partitionBy(F.col("_server_email")).orderBy(
    F.col("n_occurances").desc(), F.col("max_date").desc()
)

print("✅ Defined primary ID resolution window function")
print("   📊 Partition by: _server_email")
print("   📈 Order by: n_occurances DESC, max_date DESC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Consolidated Identity Data
# MAGIC
# MAGIC Load our consolidated identity information from Part 1 to begin the pairing process.

# COMMAND ----------

print(f"📂 Loading consolidated identity data from: {catalog_name}.{schema_prefix}silver.identity_info_consolidated")

identity_info_consolidated = spark.table(
    f"{catalog_name}.{schema_prefix}silver.identity_info_consolidated"
)

print("✅ Successfully loaded identity_info_consolidated table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Email-IFA Paired Table
# MAGIC
# MAGIC Now we'll apply our window function to create the email-IFA pairing table with ranking logic.
# MAGIC
# MAGIC ### 🔄 Processing Steps:
# MAGIC 1. **Filter**: Keep only records with valid IFA values (remove nulls)
# MAGIC 2. **Group**: Aggregate by `(email, IFA)` pairs to sum up all occurrences
# MAGIC 3. **Rank**: Apply window function to rank IFAs for each email
# MAGIC 4. **Primary Selection**: IFA with `primary_rank=1` becomes the primary advertising identifier for that email

# COMMAND ----------

print("🔄 Creating email-IFA paired table...")
print("🗂️ Filtering for valid IFA values")
print("📊 Grouping by (email, IFA) pairs")
print("🏆 Ranking IFAs using waterfall logic")

# Create the email-IFA paired table with ranking
email_ifa = (
    identity_info_consolidated
    .filter(F.col("_server_ifa").isNotNull())  # Only keep records with valid IFAs
    .groupBy("_server_email", "_server_ifa")   # Group by email-IFA pairs
    .agg(
        F.min(F.col("min_date")).alias("min_date"),      # Earliest observation
        F.max(F.col("max_date")).alias("max_date"),      # Latest observation  
        F.sum(F.col("n_occurances")).alias("n_occurances"), # Total frequency
    )
    .withColumn("primary_rank", F.row_number().over(primary_id_resolution_logic))  # Rank IFAs
)

print("✅ Email-IFA paired table created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Explore Email-IFA Relationships
# MAGIC
# MAGIC Let's examine the results to understand the email-to-IFA mapping patterns.

# COMMAND ----------

print("📊 Sample of email-IFA paired table (showing ranking):")
display(email_ifa.orderBy("_server_email", "primary_rank").limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save to Silver Layer
# MAGIC
# MAGIC Save our email-IFA paired table to Unity Catalog for use in the final identity graph creation.

# COMMAND ----------

print(f"💾 Saving email-IFA paired table to: {catalog_name}.{schema_prefix}silver.email_ifa_pairs")

email_ifa.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}silver.email_ifa_pairs"
)

print("✅ Successfully saved email_ifa table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Part 2a Complete!
# MAGIC
# MAGIC We've successfully created our email-IFA pairing table with primary advertising identifier selection logic.
# MAGIC
# MAGIC ### ✅ What We Accomplished:
# MAGIC - **Cross-Application Linking**: Connected email addresses to their primary advertising identifiers
# MAGIC - **Waterfall Logic**: Implemented frequency + recency ranking for primary IFA selection
# MAGIC - **Device-Specific Targeting**: Created the foundation for app-based advertising across mobile, tablet, and CTV devices
# MAGIC
# MAGIC ### 📋 Table Schema (`email_ifa`):
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC | `_server_email` | The hashed email address as recorded by the ad server (core identifier proxy) |
# MAGIC | `_server_ifa` | The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device) |
# MAGIC | `min_date` | First time this email-IFA pair was observed |
# MAGIC | `max_date` | Most recent observation of this pair |
# MAGIC | `n_occurances` | Total frequency of this email-IFA combination |
# MAGIC | `primary_rank` | Ranking (1 = primary IFA for this email) |
# MAGIC
# MAGIC ### 🔄 Next Steps:
# MAGIC - **Part 2b: `02b_Create Email IP Paired Table`** - Create similar pairing for IP addresses
# MAGIC - **Part 3: `03_Create Identity Graph`** - Join email-IFA and email-IP tables to create final graph
# MAGIC
# MAGIC ### 💡 Key Insights:
# MAGIC - Emails with `primary_rank=1` represent the **most frequently and recently used advertising identifier** for that email
# MAGIC - Secondary ranks (2, 3, etc.) capture additional devices tied to the same email (e.g., idfa from iPhone, gaid from Android tablet, rida from Roku)
# MAGIC - Each IFA is tied to a single device, enabling precise cross-application targeting on that specific device
