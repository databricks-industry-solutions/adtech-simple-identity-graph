# Part 3: Create Final Digital Identity Graph

This is the final step where we combine our email-IFA and email-IP paired tables to create a comprehensive digital identity graph ready for activation and analytics.

## 🎯 Objective

Join the two pairing tables (email-IFA and email-IP) to create a unified identity graph that connects each core identifier (email) to its primary Advertising ID and household IP address.

## 📊 Input Data

- **Email-IFA Table**: `{catalog_name}.silver.email_ifa_pairs` (from Part 2a)  
- **Email-IP Table**: `{catalog_name}.silver.email_ip_pairs` (from Part 2b)

## 🔗 Join Strategy

We'll perform a **full outer join** on email addresses because:

- Some emails may only have recorded/associated IFAs
- Some emails may only have recorded/associated IP addresses
- Most emails will have both, creating complete cross-device profiles

## 🏗️ Graph Structure

Each row in our final identity graph represents a unique individual/household with:

- **Unique ID**: Generated UUID for this identity  
- **Email**: Core identity anchor (hashed)  
- **Primary IFA**: Best advertising identifier (tied to a single device, used across applications)
- **Primary IP**: Best household/location identifier  
- **Secondary identifiers**: Additional IFAs/IPs for expanded reach  
- **Temporal data**: When this identity was first/last observed

## 📈 Output

- **Destination**: `{catalog_name}.gold.identity_graph` (Gold layer - production ready!)

---

```python
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
```

---

```python
from pyspark.sql import Window
from pyspark.sql import functions as F
```

---

## Step 1: Define Secondary Identifier Mapping

We'll set up how to handle secondary (non-primary) identifiers in our final graph. For this simplified version, we're keeping them as simple lists.

---

```python
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
print("📋 Using simple lists for secondary IPs and IFAs")
print("💡 In production, you might use structured maps with metadata")
```

---

## Step 2: Prepare Email-IFA Aggregations

Load and aggregate the email-IFA data to separate primary and secondary mobile identifiers.

---

```python
print(f"📂 Loading email-IFA data from: {catalog_name}.{schema_prefix}silver.email_ifa_pairs")

email_ifa_df = spark.table(f"{catalog_name}.{schema_prefix}silver.email_ifa_pairs")

print("🔄 Creating email-IFA aggregations...")
print("🏆 Separating primary (rank=1) and secondary (rank>1) IFAs")
print("📊 Computing temporal bounds for each email")

# Aggregate IFA data by email, separating primary and secondary identifiers
email_ifa_pairing = email_ifa_df.groupBy(F.col("_server_email")).agg(
    # Primary IFA: Collect the IFA with rank=1 (should be only one per email)
    F.collect_list(F.when((F.col("primary_rank") == 1), F.col("_server_ifa"))).alias(
        "primary_ifa_list"
    ),
    # Secondary IFAs: Collect all IFAs with rank>1 (additional devices like tablets, CTVs)
    F.collect_list(F.when((F.col("primary_rank") != 1), secondary_ifa_map)).alias(
        "secondary_ifa_list"
    ),
    # Temporal bounds: When was this email first/last seen with any IFA
    F.min(F.col("min_date")).alias("min_date"),
    F.max(F.col("max_date")).alias("max_date"),
)

print("✅ Email-IFA aggregations complete!")
```

---

## Step 3: Prepare Email-IP Aggregations

Load and aggregate the email-IP data to separate primary and secondary household identifiers.

---

```python
print(f"📂 Loading email-IP data from: {catalog_name}.{schema_prefix}silver.email_ip_pairs")

email_ip_df = spark.table(f"{catalog_name}.{schema_prefix}silver.email_ip_pairs")

print("🔄 Creating email-IP aggregations...")
print("🏆 Separating primary (rank=1) and secondary (rank>1) IP addresses")
print("📊 Computing temporal bounds for each email")

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
```

---

## Step 4: Create the Final Identity Graph

Now we'll join our email-IP and email-IFA aggregations to create the comprehensive identity graph.

### 🔗 Full Outer Join Strategy:

Since `email` is the common link between IFAs and IPs, we'll join on this column using a **full outer join**:

- **Matched rows**: Emails found in both tables will be merged (most common case)
- **IP-only rows**: Emails only in email-IP table (web/desktop users without app-based activity)
- **IFA-only rows**: Emails only in email-IFA table (app-only users across mobile, tablet, CTV, etc.)

This ensures we capture **all identity signals** without losing any data.

---

```python
print("🔗 Creating final identity graph...")
print("📊 Performing full outer join on email addresses")
print("🆔 Generating unique identity IDs")
print("📅 Computing overall temporal bounds")

# Define join condition
join_on = [F.col("a._server_email") == F.col("b._server_email")]

# Create the final identity graph
identity_graph = (
    email_ip_pairing.alias("a")  # Email-IP data (household/location)
    .join(email_ifa_pairing.alias("b"), join_on, "full")  # Email-IFA data (cross-app advertising IDs)
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
```

---

## Step 5: Explore the Final Identity Graph

Let's examine our completed identity graph to understand the cross-device connections we've created.

---

```python
print("📊 Sample of the final digital identity graph:")
print("🔍 Each row represents a unified identity with cross-device connections")
display(identity_graph.limit(1000))
```

---

## Step 6: Save to Gold Layer

Save our final identity graph to the Gold layer - this is our production-ready, business-ready dataset.

---

```python
print(f"💾 Saving final identity graph to: {catalog_name}.{schema_prefix}gold.identity_graph")
print("🏆 This is your production-ready identity graph!")

identity_graph.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}gold.identity_graph"
)

print("✅ Successfully saved identity_graph to Gold layer!")
```

---

## 🎉 Identity Graph Complete!

Congratulations! You've successfully built a comprehensive digital identity graph using Databricks.

### ✅ What We've Accomplished:

- **Cross-device Identity**: Connected emails to their primary advertising identifiers (mobile, tablet, CTV) and household IPs
- **Waterfall Logic**: Implemented frequency + recency ranking for optimal identifier selection
- **Comprehensive Coverage**: Used full outer joins to capture all identity signals
- **Production Ready**: Created a Gold layer table ready for activation and analytics

### 📋 Final Schema (`identity_graph`):

| Column Name | Description |
|-------------|-------------|
| `megacorp_id` | Unique UUID for this identity |
| `email_sha256` | The hashed email address as recorded by the ad server (core identifier proxy) |
| `primary_ifa` | The identifier for advertising as reported by the ad server (primary consented Advertising ID tied to a single device) |
| `primary_ip` | Primary household IP address |
| `secondary_ifa_list` | Additional advertising identifiers for other devices (e.g., gaid, idfa, rida, tifa, lguid) |
| `secondary_ip_list` | Additional IP addresses (work, travel, etc.) |
| `min_date` | First observation of this identity |
| `max_date` | Most recent observation of this identity |

### 🚀 Next Steps - Activate Your Identity Graph:

#### 📊 **Analytics & Insights**:
- Run cross-device attribution analysis
- Measure true reach and frequency across platforms
- Analyze customer journey patterns

#### 🎯 **Audience Activation**:
- Export primary IFAs for app-based targeting (mobile, tablet, CTV)
- Use IP addresses for household/location-based campaigns
- Create lookalike audiences based on cross-device behavior

#### 📈 **Measurement & Optimization**:
- Implement frequency capping across devices
- Measure incremental reach of cross-device campaigns
- Optimize ad spend allocation between app-based (mobile, tablet, CTV) and web channels

### 💡 Production Enhancements:
- **Data Quality**: Add identity confidence scoring
- **Privacy**: Implement consent management and right-to-forget
- **Enrichment**: Integrate third-party household data
- **Real-time**: Set up streaming updates for fresh identity signals
- **Monitoring**: Add data quality and freshness dashboards

### 🏗️ Technical Benefits:
- **Delta Lake**: Automatic compaction and optimization for fast queries
- **Unity Catalog**: Built-in governance, lineage, and access controls  
- **Medallion Architecture**: Clean separation of raw, processed, and business-ready data
- **Scalable**: Designed to handle billions of impressions and millions of identities

