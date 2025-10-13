# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Create Final Digital Identity Graph
# MAGIC
# MAGIC This is the final step where we combine our email-IFA and email-IP paired tables to create a comprehensive digital identity graph ready for activation and analytics.
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC Join the two pairing tables (email-IFA and email-IP) to create a unified identity graph that connects each email to its primary mobile device and household IP address.
# MAGIC
# MAGIC ## 📊 Input Data
# MAGIC - **Email-IFA Table**: `{catalog_name}.silver.email_ifa` (from Step 2a)
# MAGIC - **Email-IP Table**: `{catalog_name}.silver.email_ip` (from Step 2b)
# MAGIC
# MAGIC ## 🔗 Join Strategy
# MAGIC We'll perform a **full outer join** on email addresses because:
# MAGIC - Some emails may only have mobile activity (IFA but no consistent IP)
# MAGIC - Some emails may only have web/household activity (IP but no mobile IFA)
# MAGIC - Most emails will have both, creating complete cross-device profiles
# MAGIC
# MAGIC ## 🏗️ Graph Structure
# MAGIC Each row in our final identity graph represents a unique individual/household with:
# MAGIC - **Unique ID**: Generated UUID for this identity
# MAGIC - **Email**: Core identity anchor (hashed)
# MAGIC - **Primary IFA**: Best mobile device identifier
# MAGIC - **Primary IP**: Best household/location identifier  
# MAGIC - **Secondary identifiers**: Additional IFAs/IPs for expanded reach
# MAGIC - **Temporal data**: When this identity was first/last observed
# MAGIC
# MAGIC ## 📈 Output
# MAGIC - **Destination**: `{catalog_name}.gold.identity_graph` (Gold layer - production ready!)

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
# MAGIC ## Step 3.1: Define Secondary Identifier Mapping
# MAGIC
# MAGIC We'll set up how to handle secondary (non-primary) identifiers in our final graph. For this simplified version, we're keeping them as simple lists.

# COMMAND ----------

# 💡 Alternative: Create structured maps for secondary identifiers (commented out for simplicity)
# secondary_ip_map = F.create_map(
#     F.lit('ip_address'), F.col('ip_address'),
#     F.lit('min_date'), F.col('min_date'),
#     F.lit('max_date'), F.col('max_date'),
#     F.lit('n_occurances'), F.col('n_occurances')
# )
# secondary_ifa_map = F.create_map(
#     F.lit('_server_ifa'), F.col('_server_ifa'),
#     F.lit('min_date'), F.col('min_date'),
#     F.lit('max_date'), F.col('max_date'),
#     F.lit('n_occurances'), F.col('n_occurances')
# )

# 📝 For this demo, we'll use simple lists of secondary identifiers
secondary_ip_map = F.col("ip_address")
secondary_ifa_map = F.col("_server_ifa")

print("✅ Configured secondary identifier mapping strategy")
print("   📋 Using simple lists for secondary IPs and IFAs")
print("   💡 In production, you might use structured maps with metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.2: Prepare Email-IFA Aggregations
# MAGIC
# MAGIC Load and aggregate the email-IFA data to separate primary and secondary mobile identifiers.

# COMMAND ----------

print(f"📂 Loading email-IFA data from: {catalog_name}.silver.email_ifa")

email_ifa_df = spark.table(f"{catalog_name}.silver.email_ifa")

print("🔄 Creating email-IFA aggregations...")
print("   🏆 Separating primary (rank=1) and secondary (rank>1) IFAs")
print("   📊 Computing temporal bounds for each email")

# Aggregate IFA data by email, separating primary and secondary identifiers
email_ifa_pairing = email_ifa_df.groupBy(F.col("_server_email")).agg(
    # Primary IFA: Collect the IFA with rank=1 (should be only one per email)
    F.collect_list(F.when((F.col("primary_rank") == 1), F.col("_server_ifa"))).alias(
        "primary_ifa_list"
    ),
    # Secondary IFAs: Collect all IFAs with rank>1 (additional mobile devices)
    F.collect_list(F.when((F.col("primary_rank") != 1), secondary_ifa_map)).alias(
        "secondary_ifa_list"
    ),
    # Temporal bounds: When was this email first/last seen with any IFA
    F.min(F.col("min_date")).alias("min_date"),
    F.max(F.col("max_date")).alias("max_date"),
)

print("✅ Email-IFA aggregations complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.3: Prepare Email-IP Aggregations
# MAGIC
# MAGIC Load and aggregate the email-IP data to separate primary and secondary household identifiers.

# COMMAND ----------

print(f"📂 Loading email-IP data from: {catalog_name}.silver.email_ip")

email_ip_df = spark.table(f"{catalog_name}.silver.email_ip")

print("🔄 Creating email-IP aggregations...")
print("   🏆 Separating primary (rank=1) and secondary (rank>1) IP addresses")
print("   📊 Computing temporal bounds for each email")

# Aggregate IP data by email, separating primary and secondary identifiers
email_ip_pairing = email_ip_df.groupBy(F.col("_server_email")).agg(
    # Primary IP: Collect the IP with rank=1 (should be only one per email)
    F.collect_list(F.when((F.col("primary_rank") == 1), F.col("ip_address"))).alias(
        "primary_ip_list"
    ),
    # Secondary IPs: Collect all IPs with rank>1 (work, travel, etc.)
    F.collect_list(F.when((F.col("primary_rank") != 1), secondary_ip_map)).alias(
        "secondary_ip_list"
    ),
    # Temporal bounds: When was this email first/last seen with any IP
    F.min(F.col("min_date")).alias("min_date"),
    F.max(F.col("max_date")).alias("max_date"),
)

print("✅ Email-IP aggregations complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.4: Create the Final Identity Graph
# MAGIC
# MAGIC Now we'll join our email-IP and email-IFA aggregations to create the comprehensive identity graph.
# MAGIC
# MAGIC ### 🔗 Full Outer Join Strategy:
# MAGIC Since `email` is the common link between IFAs and IPs, we'll join on this column using a **full outer join**:
# MAGIC
# MAGIC - **Matched rows**: Emails found in both tables will be merged (most common case)
# MAGIC - **IP-only rows**: Emails only in email-IP table (web/desktop users without mobile)
# MAGIC - **IFA-only rows**: Emails only in email-IFA table (mobile-only users)
# MAGIC
# MAGIC This ensures we capture **all identity signals** without losing any data.

# COMMAND ----------

print("🔗 Creating final identity graph...")
print("   📊 Performing full outer join on email addresses")
print("   🆔 Generating unique identity IDs")
print("   📅 Computing overall temporal bounds")

# Define join condition
join_on = [F.col("a._server_email") == F.col("b._server_email")]

# Create the final identity graph
identity_graph = (
    email_ip_pairing.alias("a")  # Email-IP data (household/location)
    .join(email_ifa_pairing.alias("b"), join_on, "full")  # Email-IFA data (mobile)
    .select(
        # Generate unique identity ID for each person/household
        F.expr("uuid()").alias("megacorp_id"),
        
        # Core identity: Use email from either table (coalesce handles nulls from outer join)
        F.coalesce("a._server_email", "b._server_email").alias("email_sha256"),
        
        # Primary identifiers: Extract first element from lists (should only be one)
        F.get(F.col("primary_ifa_list"), 0).alias("primary_ifa"),
        F.get(F.col("primary_ip_list"), 0).alias("primary_ip"),
        
        # Secondary identifiers: Keep full lists for expanded targeting
        F.col("secondary_ip_list"),
        F.col("secondary_ifa_list"),
        
        # Temporal bounds: Overall first/last observation across all data
        F.greatest(F.col("a.max_date"), F.col("b.max_date")).alias("max_date"),
        F.least(F.col("a.min_date"), F.col("b.min_date")).alias("min_date"),
    )
)

print("✅ Final identity graph created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.5: Explore the Final Identity Graph
# MAGIC
# MAGIC Let's examine our completed identity graph to understand the cross-device connections we've created.

# COMMAND ----------

print("📊 Sample of the final digital identity graph:")
print("   🔍 Each row represents a unified identity with cross-device connections")
display(identity_graph.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.6: Save to Gold Layer
# MAGIC
# MAGIC Save our final identity graph to the Gold layer - this is our production-ready, business-ready dataset.

# COMMAND ----------

print(f"💾 Saving final identity graph to: {catalog_name}.gold.identity_graph")
print("   🏆 This is your production-ready identity graph!")

identity_graph.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.gold.identity_graph"
)

print("✅ Successfully saved identity_graph to Gold layer!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Identity Graph Complete!
# MAGIC
# MAGIC Congratulations! You've successfully built a comprehensive digital identity graph using Databricks.
# MAGIC
# MAGIC ### ✅ What We've Accomplished:
# MAGIC - **Cross-device Identity**: Connected emails to their primary mobile devices and household IPs
# MAGIC - **Waterfall Logic**: Implemented frequency + recency ranking for optimal identifier selection
# MAGIC - **Comprehensive Coverage**: Used full outer joins to capture all identity signals
# MAGIC - **Production Ready**: Created a Gold layer table ready for activation and analytics
# MAGIC
# MAGIC ### 📋 Final Schema (`identity_graph`):
# MAGIC 
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC | `megacorp_id` | Unique UUID for this identity |
# MAGIC | `email_sha256` | Hashed email address (core identity anchor) |
# MAGIC | `primary_ifa` | Primary mobile device identifier |
# MAGIC | `primary_ip` | Primary household IP address |
# MAGIC | `secondary_ifa_list` | Additional mobile device identifiers |
# MAGIC | `secondary_ip_list` | Additional IP addresses (work, travel, etc.) |
# MAGIC | `min_date` | First observation of this identity |
# MAGIC | `max_date` | Most recent observation of this identity |
# MAGIC
# MAGIC ### 🚀 Next Steps - Activate Your Identity Graph:
# MAGIC
# MAGIC #### 📊 **Analytics & Insights**:
# MAGIC - Run cross-device attribution analysis
# MAGIC - Measure true reach and frequency across platforms
# MAGIC - Analyze customer journey patterns
# MAGIC
# MAGIC #### 🎯 **Audience Activation**:
# MAGIC - Export primary IFAs for mobile app targeting
# MAGIC - Use IP addresses for household/location-based campaigns
# MAGIC - Create lookalike audiences based on cross-device behavior
# MAGIC
# MAGIC #### 📈 **Measurement & Optimization**:
# MAGIC - Implement frequency capping across devices
# MAGIC - Measure incremental reach of cross-device campaigns
# MAGIC - Optimize ad spend allocation between mobile and web
# MAGIC
# MAGIC ### 💡 Production Enhancements:
# MAGIC - **Data Quality**: Add identity confidence scoring
# MAGIC - **Privacy**: Implement consent management and right-to-forget
# MAGIC - **Enrichment**: Integrate third-party household data
# MAGIC - **Real-time**: Set up streaming updates for fresh identity signals
# MAGIC - **Monitoring**: Add data quality and freshness dashboards
# MAGIC
# MAGIC ### 🏗️ Technical Benefits:
# MAGIC - **Delta Lake**: Automatic compaction and optimization for fast queries
# MAGIC - **Unity Catalog**: Built-in governance, lineage, and access controls  
# MAGIC - **Medallion Architecture**: Clean separation of raw, processed, and business-ready data
# MAGIC - **Scalable**: Designed to handle billions of impressions and millions of identities
