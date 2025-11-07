# Part 1: Create Consolidated Identity Information

This notebook consolidates identity information from raw impression logs to create the foundation for our digital identity graph. This is the first step in our medallion architecture workflow.

## 🎯 Objective
Transform raw impression logs into aggregated identity combinations that will serve as the basis for building email-to-identifier relationships.

## 📊 Input Data
- **Source**: `{catalog_name}.bronze.impression_logs_prod` (Raw impression logs)
- **Key Fields**: 
  - `request_kv._server_email` - The hashed email address as recorded by the ad server (our core identifier proxy)
  - `request_kv._server_ifa` - The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device, used across applications)
  - `ip_address` - IP addresses from ad requests
  - `date` - Impression timestamps
  - `request_kv._is_coppa` - COPPA compliance flag

## 🔄 Processing Logic
1. **Filter COPPA-protected data** - Remove records flagged for children's privacy protection
2. **Remove invalid records** - Filter out rows with missing email addresses
3. **Aggregate identity combinations** - Group by `(email, ip, ifa)` triplets
4. **Calculate metrics** - Compute frequency and recency for each combination

## 📈 Output
- **Destination**: `{catalog_name}.silver.identity_info_consolidated`
- **Schema**: Aggregated identity combinations with statistical metrics

---

```python
from pyspark.sql import Window
from pyspark.sql import functions as F
```

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

## Step 1: Load Raw Impression Logs

We start by loading the raw impression logs from our Bronze layer. These logs contain the digital advertising events that include identity signals we'll use to build our graph.

---

```python
impression_logs = spark.table(f"{catalog_name}.{schema_prefix}bronze.impression_logs_prod")
```

---

## Step 2: Explore the Data Structure

The impression logs contain several key fields for identity resolution:

### 🔍 Key Data Fields:
1. **`request_kv`** - JSON object containing identity signals:
   - `_server_email` - The hashed email address as recorded by the ad server (our core identifier proxy)
   - `_server_ifa` - The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device, used across applications)
   - `_is_coppa` - COPPA compliance flag (children's privacy protection)
2. **`ip_address`** - IP address captured during the ad request
3. **`date`** - Timestamp of the impression event

### 🎯 Why These Fields Matter:
- **Email**: Serves as our core identity anchor across devices
- **IFA**: Consented Advertising ID that works across applications on a single device (e.g., idfa, gaid, rida, tifa, lguid)
- **IP Address**: Indicates household-level connections and geographic patterns
- **Date**: Helps us understand recency and frequency of identity signals

---

```python
display(impression_logs.limit(100))
```

---

## Step 3: Create Identity Aggregations

Now we'll create our consolidated identity table by aggregating all observed `(email, ip, ifa)` combinations over time.

### 📊 Aggregation Strategy:
For each unique combination of identifiers, we calculate:
- **`min_date`** - First time this combination was observed
- **`max_date`** - Most recent observation (indicates freshness)
- **`n_occurrences`** - Total frequency of this combination

### 🔒 Privacy & Data Quality Filters:
- **COPPA Compliance**: Remove records flagged for children's privacy protection
- **Data Validity**: Filter out records missing core email identifiers

### 💡 Why This Matters:
These metrics will drive our "waterfall logic" in subsequent steps - helping us identify the **strongest** and **most recent** relationships between emails and their associated identifiers.

---

```python
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
```

---

## Step 4: Save to Silver Layer

We'll persist our consolidated identity information to the Silver layer in Unity Catalog. This becomes our clean, aggregated foundation for the next steps in our identity graph workflow.

---

```python
# 💾 Save to Unity Catalog Silver layer
print(f"💾 Saving consolidated identity data to: {catalog_name}.{schema_prefix}silver.identity_info_consolidated")

identity_info_consolidated.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}silver.identity_info_consolidated"
)

print("✅ Successfully saved identity_info_consolidated table!")
```

---

## Step 5: Explore the Results

Let's examine our consolidated identity table to understand the data patterns and validate our aggregations.

---

```python
print("📊 Sample of consolidated identity information:")
display(identity_info_consolidated.limit(1000))
```

---

## 🏁 Part 1 Complete!

We've successfully created our consolidated identity foundation table with:

### ✅ What We Accomplished:
- **Privacy Compliance**: Filtered out COPPA-protected records
- **Data Quality**: Removed invalid records without email identifiers  
- **Identity Aggregation**: Created unique `(email, ip, ifa)` combinations
- **Relationship Metrics**: Calculated frequency and recency for each combination

### 📋 Table Schema:

| Column Name | Description |
|-------------|-------------|
| `_server_email` | The hashed email address as recorded by the ad server (core identifier proxy) |
| `ip_address` | Associated IP address |
| `_server_ifa` | The identifier for advertising as reported by the ad server (consented Advertising ID) |
| `min_date` | First observation date |
| `max_date` | Most recent observation date |
| `n_occurances` | Total frequency count |

### 🔄 Next Steps:
This consolidated table will now feed into our pairing logic:
1. **Part 2a: `02a_Create Email IFA Paired Table`** - Links emails with their strongest IFA
2. **Part 2b: `02b_Create Email IP Paired Table`** - Links emails with their strongest IP

The frequency and recency metrics we calculated here will drive the "waterfall logic" to determine the **primary** identifier for each email address.

