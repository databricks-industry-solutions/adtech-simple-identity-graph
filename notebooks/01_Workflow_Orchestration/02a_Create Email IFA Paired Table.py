# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2a: Create Email-IFA Paired Table
# MAGIC
# MAGIC This notebook creates the first pairing table that links email addresses to their **primary mobile device identifiers (IFAs)**. This is a critical step in building cross-device identity connections.
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC For each email address, determine the **single best** IFA (Identifier for Advertising) to use as the primary mobile device connection.
# MAGIC
# MAGIC ## 📊 Input Data
# MAGIC - **Source**: `{catalog_name}.silver.identity_info_consolidated` (from Step 1)
# MAGIC - **Focus**: Email-IFA relationships with frequency and recency metrics
# MAGIC
# MAGIC ## 🧮 Primary ID Selection Logic
# MAGIC For each email, we select the "primary" IFA using **waterfall logic**:
# MAGIC 1. **Highest frequency** (`n_occurances`) - The IFA seen most often with this email
# MAGIC 2. **Most recent** (`max_date`) - In case of ties, choose the IFA observed most recently
# MAGIC
# MAGIC ## 💡 Why This Matters
# MAGIC - **Cross-device tracking**: Links email behavior to mobile app engagement
# MAGIC - **Audience targeting**: Enables mobile ad targeting based on email segments
# MAGIC - **Attribution**: Connects mobile conversions back to email-driven awareness
# MAGIC
# MAGIC ## 📈 Output
# MAGIC - **Destination**: `{catalog_name}.silver.email_ifa`
# MAGIC - **Schema**: Email addresses with their primary and ranked IFA associations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a.1: Setup Primary ID Resolution Logic
# MAGIC
# MAGIC We'll create a window function that implements our **waterfall logic** for determining primary IFAs:
# MAGIC
# MAGIC ### 🎯 Window Function Strategy:
# MAGIC - **Partition by**: `_server_email` (group all IFAs for each email)
# MAGIC - **Order by**: 
# MAGIC   1. `n_occurances DESC` (highest frequency first)
# MAGIC   2. `max_date DESC` (most recent as tiebreaker)
# MAGIC
# MAGIC This ranking helps us identify the **most relevant mobile device** for each email address.

# COMMAND ----------

# Load catalog name from configuration
import json

with open("./data/catalog_name.json", "r") as f:
    config = json.load(f)
    catalog_name = config["catalog_name"]

print(f"✅ Loaded catalog name: {catalog_name}")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a.2: Define Window Function
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
# MAGIC ## Step 2a.3: Load Consolidated Identity Data
# MAGIC
# MAGIC Load our consolidated identity information from Step 1 to begin the pairing process.

# COMMAND ----------

print(f"📂 Loading consolidated identity data from: {catalog_name}.silver.identity_info_consolidated")

identity_info_consolidated = spark.table(
    f"{catalog_name}.silver.identity_info_consolidated"
)

print("✅ Successfully loaded identity_info_consolidated table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a.4: Create Email-IFA Paired Table
# MAGIC
# MAGIC Now we'll apply our window function to create the email-IFA pairing table with ranking logic.
# MAGIC
# MAGIC ### 🔄 Processing Steps:
# MAGIC 1. **Filter**: Keep only records with valid IFA values (remove nulls)
# MAGIC 2. **Group**: Aggregate by `(email, IFA)` pairs to sum up all occurrences
# MAGIC 3. **Rank**: Apply window function to rank IFAs for each email
# MAGIC 4. **Primary Selection**: IFA with `primary_rank=1` becomes the primary device for that email

# COMMAND ----------

print("🔄 Creating email-IFA paired table...")
print("   🗂️  Filtering for valid IFA values")
print("   📊 Grouping by (email, IFA) pairs")
print("   🏆 Ranking IFAs using waterfall logic")

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
# MAGIC ## Step 2a.5: Explore Email-IFA Relationships
# MAGIC
# MAGIC Let's examine the results to understand the email-to-IFA mapping patterns.

# COMMAND ----------

print("📊 Sample of email-IFA paired table (showing ranking):")
display(email_ifa.orderBy("_server_email", "primary_rank").limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a.6: Save to Silver Layer
# MAGIC
# MAGIC Save our email-IFA paired table to Unity Catalog for use in the final identity graph creation.

# COMMAND ----------

print(f"💾 Saving email-IFA paired table to: {catalog_name}.silver.email_ifa")

email_ifa.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.silver.email_ifa"
)

print("✅ Successfully saved email_ifa table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Step 2a Complete!
# MAGIC
# MAGIC We've successfully created our email-IFA pairing table with primary device selection logic.
# MAGIC
# MAGIC ### ✅ What We Accomplished:
# MAGIC - **Mobile Device Linking**: Connected email addresses to their primary mobile devices
# MAGIC - **Waterfall Logic**: Implemented frequency + recency ranking for primary IFA selection
# MAGIC - **Cross-device Foundation**: Created the mobile component of our identity graph
# MAGIC
# MAGIC ### 📋 Table Schema (`email_ifa`):
# MAGIC 
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC | `_server_email` | Hashed email address |
# MAGIC | `_server_ifa` | Mobile device identifier (IFA) |
# MAGIC | `min_date` | First time this email-IFA pair was observed |
# MAGIC | `max_date` | Most recent observation of this pair |
# MAGIC | `n_occurances` | Total frequency of this email-IFA combination |
# MAGIC | `primary_rank` | Ranking (1 = primary IFA for this email) |
# MAGIC
# MAGIC ### 🔄 Next Steps:
# MAGIC - **`02b_Create Email IP Paired Table`** - Create similar pairing for IP addresses
# MAGIC - **`03_Create Identity Graph`** - Join email-IFA and email-IP tables to create final graph
# MAGIC
# MAGIC ### 💡 Key Insights:
# MAGIC - Emails with `primary_rank=1` represent the **strongest mobile device connection**
# MAGIC - Secondary ranks (2, 3, etc.) capture additional mobile devices for cross-device scenarios
# MAGIC - This pairing enables **mobile ad targeting** based on email-driven audience segments

# COMMAND ----------


