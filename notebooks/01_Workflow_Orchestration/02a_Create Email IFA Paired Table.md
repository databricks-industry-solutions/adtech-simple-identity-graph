# Part 2a: Create Email-IFA Paired Table

This notebook creates the first pairing table that links email addresses to their **primary Identifier for Advertising (IFA)**. IFAs are consented advertising identifiers that work across applications on a single device (such as mobile phones, tablets, and CTVs).

## 🎯 Objective
For each email address, determine the **single best** IFA (Identifier for Advertising) to use as the primary cross-application advertising connection.

## 📊 Input Data
- **Source**: `{catalog_name}.silver.identity_info_consolidated` (from Part 1)
- **Focus**: Email-IFA relationships with frequency and recency metrics

## 🧮 Primary ID Selection Logic
For each email, we select the "primary" IFA using waterfall logic. This will be the same logic applied to select IPs, and can be applied to any additional digital identifiers your own:
1. **Highest frequency** (`n_occurances`) - The IFA seen most often with this email
2. **Most recent** (`max_date`) - In case of ties, choose the IFA observed most recently

## 💡 Why This Matters
- **Cross-application tracking**: Links email behavior to app engagement across devices (mobile, tablet, CTV)
- **Audience targeting**: Enables app-based ad targeting based on email segments
- **Attribution**: Connects in-app conversions back to email-driven awareness
- **Device-specific reach**: IFAs are tied to a single device, enabling precise targeting

## 📈 Output
- **Destination**: `{catalog_name}.silver.email_ifa_pairs`
- **Schema**: Email addresses with their primary and ranked IFA associations

---

## Step 1: Setup Primary ID Resolution Logic

We'll create a window function that implements our **waterfall logic** for determining primary IFAs:

### 🎯 Window Function Strategy:
- **Partition by**: `_server_email` (group all IFAs for each email)
- **Order by**: 
  1. `n_occurances DESC` (highest frequency first)
  2. `max_date DESC` (most recent as tiebreaker)

This ranking helps us identify the **most relevant advertising identifier** (across mobile, tablet, CTV, etc.) for each email address.

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

## Step 2: Define Window Function

Create the window specification for our primary IFA selection logic.

---

```python
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
```

---

## Step 3: Load Consolidated Identity Data

Load our consolidated identity information from Part 1 to begin the pairing process.

---

```python
print(f"📂 Loading consolidated identity data from: {catalog_name}.{schema_prefix}silver.identity_info_consolidated")

identity_info_consolidated = spark.table(
    f"{catalog_name}.{schema_prefix}silver.identity_info_consolidated"
)

print("✅ Successfully loaded identity_info_consolidated table")
```

---

## Step 4: Create Email-IFA Paired Table

Now we'll apply our window function to create the email-IFA pairing table with ranking logic.

### 🔄 Processing Steps:
1. **Filter**: Keep only records with valid IFA values (remove nulls)
2. **Group**: Aggregate by `(email, IFA)` pairs to sum up all occurrences
3. **Rank**: Apply window function to rank IFAs for each email
4. **Primary Selection**: IFA with `primary_rank=1` becomes the primary advertising identifier for that email

---

```python
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
```

---

## Step 5: Explore Email-IFA Relationships

Let's examine the results to understand the email-to-IFA mapping patterns.

---

```python
print("📊 Sample of email-IFA paired table (showing ranking):")
display(email_ifa.orderBy("_server_email", "primary_rank").limit(1000))
```

---

## Step 6: Save to Silver Layer

Save our email-IFA paired table to Unity Catalog for use in the final identity graph creation.

---

```python
print(f"💾 Saving email-IFA paired table to: {catalog_name}.{schema_prefix}silver.email_ifa_pairs")

email_ifa.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_prefix}silver.email_ifa_pairs"
)

print("✅ Successfully saved email_ifa table!")
```

---

## 🏁 Part 2a Complete!

We've successfully created our email-IFA pairing table with primary advertising identifier selection logic.

### ✅ What We Accomplished:
- **Cross-Application Linking**: Connected email addresses to their primary advertising identifiers
- **Waterfall Logic**: Implemented frequency + recency ranking for primary IFA selection
- **Device-Specific Targeting**: Created the foundation for app-based advertising across mobile, tablet, and CTV devices

### 📋 Table Schema (`email_ifa`):

| Column Name | Description |
|-------------|-------------|
| `_server_email` | The hashed email address as recorded by the ad server (core identifier proxy) |
| `_server_ifa` | The identifier for advertising as reported by the ad server (consented Advertising ID tied to a single device) |
| `min_date` | First time this email-IFA pair was observed |
| `max_date` | Most recent observation of this pair |
| `n_occurances` | Total frequency of this email-IFA combination |
| `primary_rank` | Ranking (1 = primary IFA for this email) |

### 🔄 Next Steps:
- **Part 2b: `02b_Create Email IP Paired Table`** - Create similar pairing for IP addresses
- **Part 3: `03_Create Identity Graph`** - Join email-IFA and email-IP tables to create final graph

### 💡 Key Insights:
- Emails with `primary_rank=1` represent the **most frequently and recently used advertising identifier** for that email
- Secondary ranks (2, 3, etc.) capture additional devices tied to the same email (e.g., idfa from iPhone, gaid from Android tablet, rida from Roku)
- Each IFA is tied to a single device, enabling precise cross-application targeting on that specific device

