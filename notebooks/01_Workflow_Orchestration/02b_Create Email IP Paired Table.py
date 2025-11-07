# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2b: Create Email-IP Paired Table
# MAGIC
# MAGIC This notebook creates the second pairing table that links email addresses to their **primary IP addresses**. IP addresses often represent household-level connections and geographic patterns.
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC For each email address, determine the **single best** IP address to use as the primary household/location connection.
# MAGIC
# MAGIC ## 📊 Input Data
# MAGIC - **Source**: `{catalog_name}.silver.identity_info_consolidated` (from Part 1)
# MAGIC - **Focus**: Email-IP relationships with frequency and recency metrics
# MAGIC
# MAGIC ## 🧮 Primary ID Selection Logic
# MAGIC For each email, we select the "primary" IP using **waterfall logic** (same as IFA selection):
# MAGIC 1. **Highest frequency** (`n_occurances`) - The IP seen most often with this email
# MAGIC 2. **Most recent** (`max_date`) - In case of ties, choose the IP observed most recently
# MAGIC
# MAGIC ## 💡 Why This Matters
# MAGIC - **Household targeting**: Links email behavior to household IP addresses
# MAGIC - **Geographic insights**: Enables location-based audience targeting
# MAGIC - **Cross-device attribution**: Connects household devices back to email identity
# MAGIC - **Fraud detection**: Helps identify suspicious IP/email combinations
# MAGIC
# MAGIC ## 📈 Output
# MAGIC - **Destination**: `{catalog_name}.silver.email_ip`
# MAGIC - **Schema**: Email addresses with their primary and ranked IP associations

# COMMAND ----------

# Load catalog name and schema prefix from configuration
import json

with open("./data/catalog_name.json", "r") as f:
    config = json.load(f)
    catalog_name = config["catalog_name"]
    schema_prefix = config.get("schema_prefix", "")

print(f"✅ Loaded catalog name: {catalog_name}")
if schema_prefix:
    print(f"✅ Schema prefix: {schema_prefix}")
    schema_prefix += "_"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup Primary ID Resolution Logic
# MAGIC
# MAGIC We'll use the same window function approach as in Part 2a, but now for IP addresses instead of IFAs.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Window Function
# MAGIC
# MAGIC Create the same waterfall logic window specification for IP address selection.

# COMMAND ----------

# 🎯 Primary ID Resolution Logic (same as IFA logic)
# This window function will rank IP addresses for each email based on:
# 1. Frequency (how often they appear together)
# 2. Recency (when they were last seen together)
primary_id_resolution_logic = Window.partitionBy("_server_email").orderBy(
    F.col("n_occurances").desc(), F.col("max_date").desc()
)

print("✅ Defined primary ID resolution window function for IP addresses")
print("📊 Partition by: _server_email")
print("📈 Order by: n_occurances DESC, max_date DESC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Household-Level Identity Logic
# MAGIC
# MAGIC **Important Note**: In this simplified example, we're using IP addresses as a proxy for household assignment. 
# MAGIC
# MAGIC ### 🏠 IP Address = Household Assumption:
# MAGIC - Primary IP often represents the home network
# MAGIC - Multiple family members may share the same household IP
# MAGIC - Enables household-level targeting and measurement
# MAGIC
# MAGIC ### 🚀 Production Enhancements:
# MAGIC In real-world implementations, you can enhance household logic by incorporating:
# MAGIC - **Hashed physical addresses** from customer data
# MAGIC - **Last names or family identifiers** (properly hashed)
# MAGIC - **Geographic clustering** of IP addresses
# MAGIC - **Device fingerprinting** data
# MAGIC - **Third-party household mapping** services

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Consolidated Identity Data

# COMMAND ----------

print(f"📂 Loading consolidated identity data from: {catalog_name}.{schema_prefix}silver.identity_info_consolidated")

identity_info_consolidated = spark.table(
    f"{catalog_name}.{schema_prefix}silver.identity_info_consolidated"
)

print("✅ Successfully loaded identity_info_consolidated table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Email-IP Paired Table
# MAGIC
# MAGIC Now we'll create the email-IP pairing table using the same logic as the IFA pairing, but grouping by IP addresses.
# MAGIC
# MAGIC ### 🔄 Processing Steps:
# MAGIC 1. **Filter**: Keep only records with valid IP addresses (remove nulls)
# MAGIC 2. **Group**: Aggregate by `(email, IP)` pairs to sum up all occurrences
# MAGIC 3. **Rank**: Apply window function to rank IPs for each email
# MAGIC 4. **Primary Selection**: IP with `primary_rank=1` becomes the primary household for that email

# COMMAND ----------

print("🔄 Creating email-IP paired table...")
print("🗂️ Filtering for valid IP address values")
print("📊 Grouping by (email, IP) pairs")
print("🏆 Ranking IP addresses using waterfall logic")

# Create the email-IP paired table with ranking
email_ip = (
    identity_info_consolidated
    .filter(F.col("ip_address").isNotNull())  # Only keep records with valid IPs
    .groupBy("_server_email", "ip_address")   # Group by email-IP pairs
    .agg(
        F.min(F.col("min_date")).alias("min_date"),      # Earliest observation
        F.max(F.col("max_date")).alias("max_date"),      # Latest observation
        F.sum(F.col("n_occurances")).alias("n_occurances"), # Total frequency
    )
    .withColumn("primary_rank", F.row_number().over(primary_id_resolution_logic))  # Rank IPs
)

print("✅ Email-IP paired table created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Explore Email-IP Relationships
# MAGIC
# MAGIC Let's examine the results to understand the email-to-IP mapping patterns.

# COMMAND ----------

print("📊 Sample of email-IP paired table (showing ranking):")
display(email_ip.orderBy("_server_email", "primary_rank").limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save to Silver Layer
# MAGIC
# MAGIC Save our email-IP paired table to Unity Catalog for use in the final identity graph creation.

# COMMAND ----------

print(f"💾 Saving email-IP paired table to: {catalog_name}.{schema_prefix}silver.email_ip")

email_ip.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}silver.email_ip"
)

print("✅ Successfully saved email_ip table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Part 2b Complete!
# MAGIC
# MAGIC We've successfully created our email-IP pairing table with primary household selection logic.
# MAGIC
# MAGIC ### ✅ What We Accomplished:
# MAGIC - **Household Linking**: Connected email addresses to their primary IP addresses
# MAGIC - **Waterfall Logic**: Implemented frequency + recency ranking for primary IP selection
# MAGIC - **Geographic Foundation**: Created the household/location component of our identity graph
# MAGIC
# MAGIC ### 📋 Table Schema (`email_ip`):
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC | `_server_email` | The hashed email address as recorded by the ad server (core identifier proxy) |
# MAGIC | `ip_address` | IP address (household/location identifier) |
# MAGIC | `min_date` | First time this email-IP pair was observed |
# MAGIC | `max_date` | Most recent observation of this pair |
# MAGIC | `n_occurances` | Total frequency of this email-IP combination |
# MAGIC | `primary_rank` | Ranking (1 = primary IP for this email) |
# MAGIC
# MAGIC ### 🔄 Next Steps:
# MAGIC - **Part 3: `03_Create Identity Graph`** - Join email-IFA and email-IP tables to create final comprehensive identity graph
# MAGIC
# MAGIC ### 💡 Key Insights:
# MAGIC - Emails with `primary_rank=1` represent the **strongest household/location connection**
# MAGIC - Secondary ranks capture additional IP addresses used by the same email (e.g., work, travel, vacation homes)
# MAGIC - IP addresses help identify household-level patterns and geographic targeting opportunities
